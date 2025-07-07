use std::net::Ipv4Addr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::JoinHandle as StdJoinHandle;
use std::time::Duration;

use clap::Parser;
use futures_util::FutureExt;
use parking_lot::deadlock;
use tokio::sync::{Notify, mpsc, oneshot};
use tycho_consensus::prelude::{
    EngineBinding, EngineNetworkArgs, EngineSession, InitPeers, InputBuffer, MempoolDb,
};
use tycho_consensus::test_utils::*;
use tycho_crypto::ed25519::{KeyPair, SecretKey};
use tycho_network::{Address, DhtConfig, NetworkConfig, OverlayConfig, PeerId, PeerResolverConfig};
use tycho_storage::StorageContext;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> anyhow::Result<()> {
    Cli::parse().run()
}

/// Tycho network node.
#[derive(Parser)]
struct Cli {
    /// total nodes populated as separate tokio runtimes
    #[arg(short, long, default_value_t = NonZeroUsize::new(4).unwrap())]
    nodes: NonZeroUsize,
    /// tokio worker threads per node
    #[arg(short, long, default_value_t = NonZeroUsize::new(2).unwrap())]
    workers_per_node: NonZeroUsize,
    /// step is an amount of points produced by node for payload to grow in size;
    /// every node counts its points in step independently
    #[arg(short, long, default_value_t = 33)]
    payload_step: usize,
    /// number of steps in which payload will increase from 0 to max configured value
    /// by [`PAYLOAD_BATCH_BYTES`](tycho_consensus::MempoolConfig::PAYLOAD_BATCH_BYTES)
    #[arg(short, long, default_value_t = NonZeroUsize::new(3).unwrap())]
    steps_until_full: NonZeroUsize,
    /// generate data for span-aware flame graph (changes log format);
    /// follows `<https://github.com/tokio-rs/tracing/tree/master/tracing-flame#generating-the-image>`,
    /// but results are in git-ignored `./.temp` dir, so don't forget to `$ cd ./.temp` after run
    #[arg(short, long, default_value_t = false)]
    flame: bool,
    /// shutdown after duration elapsed;
    /// format `<https://docs.rs/humantime/latest/humantime/fn.parse_duration.html>`
    #[arg(short, long)]
    #[clap(value_parser = humantime::parse_duration)]
    duration: Option<Duration>,
}

impl Cli {
    fn run(self) -> anyhow::Result<()> {
        if self.flame {
            test_logger::flame("engine with --flame");
        } else {
            test_logger::spans("engine", "info,tycho_consensus=info,tycho_network=info");
        }
        check_parking_lot();

        let anchor_consumer = AnchorConsumer::default();
        let common_anchor_count = anchor_consumer.common_anchor_count.clone();
        let run_guard = RunGuard::default();

        heart_beat(self.duration, common_anchor_count, run_guard.clone());

        for handle in make_network(self, anchor_consumer, run_guard)? {
            match handle.join() {
                Ok(()) => {}
                Err(e) => std::panic::resume_unwind(e),
            }
        }

        Ok(())
    }
}

fn make_network(
    cli: Cli,
    mut anchor_consumer: AnchorConsumer,
    run_guard: RunGuard,
) -> anyhow::Result<Vec<StdJoinHandle<()>>> {
    let keys = (0..cli.nodes.get())
        .map(|_| rand::random::<SecretKey>())
        .map(|secret| (secret, Arc::new(KeyPair::from(&secret))))
        .collect::<Vec<_>>();

    let all_peers = keys
        .iter()
        .map(|(_, kp)| PeerId::from(kp.public_key))
        .collect::<Vec<_>>();

    let bind_addresses = keys
        .iter()
        .map(|_| {
            std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
                .expect("bind udp socket")
                .local_addr()
                .expect("local address")
                .into()
        })
        .collect::<Vec<Address>>();

    let peer_info = keys
        .iter()
        .zip(bind_addresses.iter())
        .map(|((_, key_pair), addr)| Arc::new(make_peer_info(key_pair, vec![addr.clone()], None)))
        .collect::<Vec<_>>();

    let merged_conf = default_test_config();
    let dht_config = DhtConfig {
        local_info_announce_period: Duration::from_secs(1),
        local_info_announce_period_max_jitter: Duration::from_secs(1),
        routing_table_refresh_period: Duration::from_secs(1),
        routing_table_refresh_period_max_jitter: Duration::from_secs(1),
        ..Default::default()
    };

    let mut handles = vec![];

    let started = Arc::new(tokio::sync::Semaphore::new(0));

    for (((secret_key, key_pair), bind_address), peer_id) in keys
        .into_iter()
        .zip(bind_addresses.into_iter())
        .zip(peer_info.iter().map(|p| p.id))
    {
        let init_peers = InitPeers::new(all_peers.clone());
        let peer_info = peer_info.clone();

        let started = started.clone();
        let run_guard = run_guard.clone();

        let merged_conf = merged_conf.clone();
        let dht_config = dht_config.clone();

        let top_known_anchor = anchor_consumer.top_known_anchor.clone();
        let commit_round = anchor_consumer.commit_round.clone();

        let (committed_tx, committed_rx) = mpsc::unbounded_channel();
        anchor_consumer.add(peer_id, committed_rx);

        let handle = std::thread::Builder::new()
            .name(format!("engine-{peer_id:.4}"))
            .spawn(move || {
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(cli.workers_per_node.get())
                    .thread_name_fn(move || {
                        static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                        let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                        format!("{id}-tokio-{peer_id:.4}")
                    })
                    .build()
                    .expect("new tokio runtime")
                    .block_on(async move {
                        let net_args = {
                            let (dht_client, peer_resolver, overlay_service) = from_validator(
                                bind_address,
                                &secret_key,
                                None::<Address>,
                                dht_config,
                                None::<PeerResolverConfig>,
                                None::<OverlayConfig>,
                                NetworkConfig::default(),
                            );
                            for info in peer_info {
                                if info.id != peer_id {
                                    dht_client.add_peer(info).expect("add peer to dht client");
                                }
                            }
                            EngineNetworkArgs {
                                key_pair,
                                network: dht_client.network().clone(),
                                peer_resolver: peer_resolver.clone(),
                                overlay_service: overlay_service.clone(),
                            }
                        };

                        let (ctx, _tmp_dir) =
                            StorageContext::new_temp().await.expect("new storage");

                        let bind = EngineBinding {
                            mempool_db: MempoolDb::open(ctx, commit_round).unwrap(),
                            input_buffer: InputBuffer::new_stub(
                                cli.payload_step,
                                cli.steps_until_full,
                                merged_conf.consensus(),
                            ),
                            output: committed_tx,
                            top_known_anchor,
                        };

                        let (engine_stop_tx, engine_stop_rx) = oneshot::channel();
                        let _session = EngineSession::new(
                            bind,
                            &net_args,
                            &merged_conf,
                            init_peers,
                            engine_stop_tx,
                        );

                        started.add_permits(1);
                        tracing::info!("created engine {peer_id}");
                        tokio::try_join!(
                            engine_stop_rx.map(|_| Err::<(), ()>(())),
                            run_guard.until_any_dropped()
                        )
                        .ok();
                    });
            })?;
        handles.push(handle);
    }

    let node_count = cli.nodes.get() as u32;
    let handle = std::thread::Builder::new()
        .name("anchor-consumer".to_string())
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .worker_threads(1)
                .build()
                .expect("new tokio runtime")
                .block_on(async {
                    _ = started
                        .acquire_many(node_count)
                        .await
                        .expect("wait nodes start");
                    tokio::try_join!(
                        (anchor_consumer.check(&merged_conf)).map(|_| Err::<(), ()>(())),
                        run_guard.until_any_dropped()
                    )
                    .ok();
                });
        })?;
    handles.push(handle);

    Ok(handles)
}

fn check_parking_lot() {
    // Create a background thread which checks for deadlocks every 3s
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(Duration::from_secs(3));
            let deadlocks = deadlock::check_deadlock();
            if deadlocks.is_empty() {
                continue;
            }

            tracing::error!("{} deadlocks detected", deadlocks.len());
            for (i, threads) in deadlocks.iter().enumerate() {
                tracing::error!("Deadlock #{}", i);
                for t in threads {
                    tracing::error!("Thread Id {:#?}: {:#?}", t.thread_id(), t.backtrace());
                }
            }
        }
    });
}

fn heart_beat(
    duration: Option<Duration>,
    common_anchor_count: Arc<AtomicUsize>,
    run_guard: RunGuard,
) {
    std::thread::spawn(move || {
        let _run_guard = run_guard;
        let start = std::time::Instant::now();
        loop {
            std::thread::sleep(Duration::from_secs(1));
            tracing::info!("heart beat");
            if let Some(duration) = duration {
                let elapsed = start.elapsed();
                if elapsed >= duration {
                    let common_anchor_count = common_anchor_count.load(Ordering::Relaxed);
                    let expected_anchor_count = elapsed.as_secs() as usize * 2 / 3;
                    assert!(
                        common_anchor_count >= expected_anchor_count,
                        "mempool network produced just {common_anchor_count} common anchors, \
                        expected at least {expected_anchor_count} during {} ",
                        humantime::format_duration(elapsed)
                    );
                    tracing::info!(
                        "test stopped after {}, \
                        produced {common_anchor_count} common anchors, \
                        expected at least {expected_anchor_count}",
                        humantime::format_duration(elapsed)
                    );
                    break;
                }
            }
        }
    });
}
#[derive(Clone, Default)]
struct RunGuard {
    notify: Arc<Notify>,
}

impl Drop for RunGuard {
    fn drop(&mut self) {
        self.notify.notify_waiters();
    }
}

impl RunGuard {
    pub async fn until_any_dropped(self) -> Result<(), ()> {
        self.notify.notified().await;
        Err(())
    }
}
