use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tokio::runtime::Runtime;
use tycho_core::global_config::GlobalConfig;
use tycho_util::cli::logger::{init_logger, set_abort_with_tracing};
use tycho_util::cli::{resolve_public_ip, signal};

pub use self::control::CmdControl;
use crate::node::{MetricsConfig, Node, NodeConfig, NodeKeys};
use crate::util::alloc::spawn_allocator_metrics_loop;
use crate::BaseArgs;

mod control;

/// Manage the node.
#[derive(Parser)]
pub struct Cmd {
    #[clap(subcommand)]
    cmd: SubCmd,
}

impl Cmd {
    pub fn run(self, args: BaseArgs) -> Result<()> {
        match self.cmd {
            SubCmd::Run(cmd) => cmd.run(args),
            SubCmd::Control(cmd) => cmd.run(args),
        }
    }
}

#[derive(Subcommand)]
enum SubCmd {
    Run(CmdRun),
    #[clap(flatten)]
    Control(CmdControl),
}

/// Run a Tycho node.
#[derive(Parser)]
struct CmdRun {
    /// Path to the node config. Default: `$TYCHO_HOME/config.json`
    #[clap(long)]
    config: Option<PathBuf>,

    /// Path to the global config. Default: `$TYCHO_HOME/global-config.json`
    #[clap(long)]
    global_config: Option<PathBuf>,

    /// Path to the node keys. Default: `$TYCHO_HOME/node_keys.json`
    #[clap(long)]
    keys: Option<PathBuf>,

    /// Path to the control socket. Default: `$TYCHO_HOME/control.sock`
    #[clap(long)]
    control_socket: Option<PathBuf>,

    /// Path to the logger config.
    #[clap(long)]
    logger_config: Option<PathBuf>,

    /// List of zerostate files to import.
    #[clap(long)]
    import_zerostate: Option<Vec<PathBuf>>,
}

impl CmdRun {
    fn run(self, args: BaseArgs) -> Result<()> {
        let node_config = NodeConfig::from_file(args.node_config_path(self.config.as_ref()))
            .context("failed to load node config")?
            .with_relative_paths(&args.home);

        rayon::ThreadPoolBuilder::new()
            .stack_size(8 * 1024 * 1024)
            .thread_name(|_| "rayon_worker".to_string())
            .num_threads(node_config.threads.rayon_threads)
            .build_global()?;

        let rt = build_tokio_runtime(&node_config)?;

        rt.block_on(async move {
            let run_fut = tokio::spawn(self.run_impl(args, node_config));
            let stop_fut = signal::any_signal(signal::TERMINATION_SIGNALS);
            tokio::select! {
                res = run_fut => res.unwrap(),
                signal = stop_fut => match signal {
                    Ok(signal) => {
                        tracing::info!(?signal, "received termination signal");
                        Ok(())
                    }
                    Err(e) => Err(e.into()),
                }
            }
        })
    }

    async fn run_impl(self, args: BaseArgs, node_config: NodeConfig) -> Result<()> {
        init_logger(&node_config.logger, self.logger_config)?;
        set_abort_with_tracing();

        if let Some(metrics_config) = &node_config.metrics {
            init_metrics(metrics_config)?;
        }

        let node = {
            let global_config =
                GlobalConfig::from_file(args.global_config_path(self.global_config.as_ref()))
                    .context("failed to load global config")?;

            let node_keys_path = args.node_keys_path(self.keys.as_ref());
            let node_keys = if node_keys_path.exists() {
                NodeKeys::from_file(node_keys_path).context("failed to load node keys")?
            } else {
                let keys = rand::random::<NodeKeys>();
                tracing::warn!(
                    node_keys_path = %node_keys_path.display(),
                    public_key = %keys.public_key(),
                    "generated new node keys",
                );

                keys.save_to_file(node_keys_path)
                    .context("failed to save new node keys")?;
                keys
            };

            let public_ip = resolve_public_ip(node_config.public_ip).await?;
            let socket_addr = SocketAddr::new(public_ip, node_config.port);

            let control_socket = args.control_socket_path(self.control_socket.as_ref());

            Node::new(
                socket_addr,
                node_keys,
                node_config,
                global_config,
                control_socket,
            )
            .await?
        };

        node.wait_for_neighbours().await;

        let init_block_id = node
            .boot(self.import_zerostate)
            .await
            .context("failed to init node")?;

        tracing::info!(%init_block_id, "node initialized");

        node.run(&init_block_id).await?;

        Ok(())
    }
}

fn build_tokio_runtime(node_config: &NodeConfig) -> Result<Runtime> {
    #[cfg(all(feature = "tokio-metrics", tokio_unstable))]
    use std::time::Duration;

    #[cfg(all(feature = "tokio-metrics", tokio_unstable))]
    use tokio::runtime::{HistogramConfiguration, LogHistogram};

    let mut rt = tokio::runtime::Builder::new_multi_thread();

    let num_workers = node_config.threads.tokio_workers;
    rt.enable_all().worker_threads(num_workers);

    #[cfg(all(feature = "tokio-metrics", tokio_unstable))]
    let hist_params = LogHistogram::builder()
        .precision_exact(2)
        .min_value(Duration::from_micros(500))
        .max_value(Duration::from_secs(1))
        .max_buckets(NUM_BUCKETS)?;

    #[cfg(all(feature = "tokio-metrics", tokio_unstable))]
    const NUM_BUCKETS: usize = 46;
    #[cfg(all(feature = "tokio-metrics", tokio_unstable))]
    {
        rt.enable_metrics_poll_time_histogram()
            .metrics_poll_time_histogram_configuration(HistogramConfiguration::log(hist_params));
    }

    let rt = rt.build()?;

    #[cfg(all(feature = "tokio-metrics", tokio_unstable))]
    rt.spawn(async move {
      // extracted from tokio sources
      // issue https://github.com/tokio-rs/tokio/issues/7033
        fn bucket_range(bucket: usize, p: u32, bucket_offset: usize, num_buckets: usize) -> std::ops::Range<u64> {
            let input_bucket = bucket;
            let bucket = bucket + bucket_offset;

            let range_start_0th_bucket = match input_bucket {
                0 => Some(0_u64),
                _ => None,
            };
            let range_end_last_bucket = match input_bucket {
                n if n == num_buckets - 1 => Some(u64::MAX),
                _ => None,
            };

            if bucket < 1 << p {
                // The first set of buckets are all size 1
                let bucket = bucket as u64;
                range_start_0th_bucket.unwrap_or(bucket)..range_end_last_bucket.unwrap_or(bucket + 1)
            } else {
                // Determine which range of buckets we're in, then determine which bucket in the range it is
                let bucket = bucket as u64;
                let p = p as u64;
                let w = (bucket >> p) - 1;
                let base_bucket = (w + 1) * (1_u64 << p);
                let offset = bucket - base_bucket;
                let s = 1_u64 << (w + p);
                let start = s + (offset << w);
                let end = s + ((offset + 1) << w);

                range_start_0th_bucket.unwrap_or(start)..range_end_last_bucket.unwrap_or(end)
            }
        }

        fn fill_log_buckets() -> [f64; NUM_BUCKETS] {
            let mut boundaries = [0.0; NUM_BUCKETS];

            for i in 0..NUM_BUCKETS {
                // parameters are taken from dbg!(&hist_params); above
                let range = bucket_range(i, 2, 70, 46);
                boundaries[i] = (range.start as f64) / 1_000_000_000.0;
            }

            boundaries
        }
        let log_buckets: [f64; NUM_BUCKETS] = fill_log_buckets();


        // we can use histogram when https://github.com/metrics-rs/metrics/issues/509 is resolved
        // otherwise it will burn CPU and memory
        let handle = tokio::runtime::Handle::current();
        let runtime_monitor = tokio_metrics::RuntimeMonitor::new(&handle);

        const METRIC_NAME: &str = "tycho_tokio_poll_count_time_bucket";
        const METRIC_SUM: &str = "tycho_tokio_poll_count_time_sum";
        const METRIC_COUNT: &str = "tycho_tokio_poll_count_time_count";

        for interval in runtime_monitor.intervals() {
            let histogram = interval.poll_count_histogram;

            let mut cumulative_count = 0;
            let mut sum = 0.0;

            // poll time histogram via gauages
            for (idx, value) in histogram.iter().enumerate() {
                let bucket = log_buckets[idx];
                cumulative_count += *value;
                let le = format!("{:.6}", bucket);
                metrics::gauge!(METRIC_NAME, "le" => le).set(cumulative_count as f64);
                sum += bucket * (*value as f64);
            }
            // Add sum and count
            metrics::gauge!(METRIC_SUM).set(sum);
            metrics::gauge!(METRIC_COUNT).set(cumulative_count as f64);
            // Add +Inf bucket
            metrics::gauge!(METRIC_NAME, "le" => "+Inf").set(cumulative_count as f64);

            let mean_poll_time = interval.mean_poll_duration.as_secs_f64();
            metrics::gauge!("tycho_tokio_mean_poll_time").set(mean_poll_time);

            let max_poll_time = interval.mean_poll_duration_worker_max.as_secs_f64();
            metrics::gauge!("tycho_tokio_max_poll_time").set(max_poll_time);

            let metrics = handle.metrics();
            metrics::gauge!("tycho_tokio_num_alive_tasks").set(metrics.num_alive_tasks() as f64);

            let global_queue_depth = metrics.global_queue_depth();
            metrics::gauge!("tycho_tokio_global_queue_depth").set(global_queue_depth as f64);

            let num_blocking_threads = metrics.num_blocking_threads();
            metrics::gauge!("tycho_tokio_num_blocking_threads").set(num_blocking_threads as f64);

            let spawned_tasks = metrics.spawned_tasks_count();
            metrics::gauge!("tycho_tokio_spawned_tasks_count").set(spawned_tasks as f64);


            metrics::gauge!("tycho_tokio_num_idle_blocking_threads")
                .set(metrics.num_idle_blocking_threads() as f64);

            metrics::gauge!("tycho_tokio_injection_queue_depth")
                .set(metrics.global_queue_depth() as f64);

            let blocking_queue_length = metrics.blocking_queue_depth();
            metrics::gauge!("tycho_tokio_blocking_queue_depth").set(blocking_queue_length as f64);

            for worker_id in 0..num_workers {
                let park_count = metrics.worker_park_count(worker_id);
                metrics::gauge!("tycho_tokio_worker_park_count", "worker_id" => format!("{worker_id}")).set(park_count as f64);

                let worker_noop_count = metrics.worker_noop_count(worker_id);
                metrics::gauge!("tycho_tokio_worker_noop_count", "worker_id" => format!("{worker_id}")).set(worker_noop_count as f64);

                let worker_steal_count = metrics.worker_steal_count(worker_id);
                metrics::gauge!("tycho_tokio_worker_steal_count", "worker_id" => format!("{worker_id}")).set(worker_steal_count as f64);

                let worker_steal_operations = metrics.worker_steal_operations(worker_id);
                metrics::gauge!("tycho_tokio_worker_steal_operations", "worker_id" => format!("{worker_id}")).set(worker_steal_operations as f64);

                let worker_local_queue_depth = metrics.worker_local_queue_depth(worker_id);
                metrics::gauge!("tycho_tokio_worker_local_queue_depth", "worker_id" => format!("{worker_id}")).set(worker_local_queue_depth as f64);

                let worker_mean_poll_time = metrics.worker_mean_poll_time(worker_id).as_secs_f64();
                metrics::gauge!("tycho_tokio_worker_mean_poll_time", "worker_id" => format!("{worker_id}")).set(worker_mean_poll_time);

                let worker_busy_time = metrics.worker_total_busy_duration(worker_id).as_secs_f64();
                metrics::gauge!("tycho_tokio_worker_busy_time", "worker_id" => format!("{worker_id}")).set(worker_busy_time);
            }
            metrics::gauge!("tycho_tokio_io_driver_fd_registered_count").set(metrics.io_driver_fd_registered_count() as f64);
            metrics::gauge!("tycho_tokio_io_driver_fd_deregistered_count").set(metrics.io_driver_fd_deregistered_count() as f64);

            metrics::gauge!("tycho_tokio_remote_schedule_count").set(metrics.remote_schedule_count() as f64);

            metrics::gauge!("tycho_tokio_budget_forced_yield_count").set(metrics.budget_forced_yield_count() as f64);


            tokio::time::sleep(Duration::from_millis(5000)).await;
        }
    });

    Ok(rt)
}

fn init_metrics(config: &MetricsConfig) -> Result<()> {
    use metrics_exporter_prometheus::Matcher;
    const EXPONENTIAL_SECONDS: &[f64] = &[
        0.000001, 0.00001, 0.0001, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.75, 1.0, 2.5, 5.0,
        7.5, 10.0, 30.0, 60.0, 120.0, 180.0, 240.0, 300.0,
    ];

    const EXPONENTIAL_LONG_SECONDS: &[f64] = &[
        0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 240.0, 300.0, 600.0, 1800.0, 3600.0, 7200.0,
        14400.0, 28800.0, 43200.0, 86400.0,
    ];

    const EXPONENTIAL_THREADS: &[f64] = &[1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0];

    metrics_exporter_prometheus::PrometheusBuilder::new()
        .set_buckets_for_metric(Matcher::Suffix("_time".to_string()), EXPONENTIAL_SECONDS)?
        .set_buckets_for_metric(Matcher::Suffix("_threads".to_string()), EXPONENTIAL_THREADS)?
        .set_buckets_for_metric(
            Matcher::Suffix("_time_long".to_string()),
            EXPONENTIAL_LONG_SECONDS,
        )?
        .with_http_listener(config.listen_addr)
        .install()
        .context("failed to initialize a metrics exporter")?;

    #[cfg(feature = "jemalloc")]
    spawn_allocator_metrics_loop();

    Ok(())
}
