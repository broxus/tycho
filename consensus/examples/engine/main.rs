use std::net::Ipv4Addr;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use everscale_crypto::ed25519::{KeyPair, SecretKey};
use futures_util::future::FutureExt;
use parking_lot::deadlock;
use tokio::sync::{mpsc, Notify};
use tycho_consensus::test_utils::*;
use tycho_consensus::{Engine, InputBufferStub};
use tycho_network::{Address, DhtConfig, NetworkConfig, PeerId};
use tycho_storage::Storage;

mod logger;

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
    /// step is an amount of points produced by every node independently
    #[arg(short, long, default_value_t = NonZeroUsize::new(33).unwrap())]
    points_in_step: NonZeroUsize,
    /// number of steps in which payload will increase from 0
    /// to [PAYLOAD_BATCH_BYTES](tycho_consensus::MempoolConfig::PAYLOAD_BATCH_BYTES)
    #[arg(short, long, default_value_t = NonZeroUsize::new(3).unwrap())]
    steps_until_full: NonZeroUsize,
    /// generate data for span-aware flame graph (changes log format);
    /// follows https://github.com/tokio-rs/tracing/tree/master/tracing-flame#generating-the-image,
    /// but results are in git-ignored `./.temp` dir, so don't forget to `$ cd ./.temp` after run
    #[arg(short, long, default_value_t = false)]
    flame: bool,
    /// shutdown after duration elapsed;
    /// format https://docs.rs/humantime/latest/humantime/fn.parse_duration.html
    #[arg(short, long)]
    #[clap(value_parser = humantime::parse_duration)]
    duration: Option<Duration>,
}

impl Cli {
    fn run(self) -> anyhow::Result<()> {
        if self.flame {
            logger::flame("engine with --flame");
        } else {
            logger::spans("engine", "info,tycho_consensus=info,tycho_network=info")
        };
        check_parking_lot();
        heart_beat(self.duration);
        make_network(self);
        Ok(())
    }
}

fn make_network(cli: Cli) {
    let keys = (0..cli.nodes.get())
        .map(|_| SecretKey::generate(&mut rand::thread_rng()))
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

    let run_guard = RunGuard::new();
    let mut anchor_consumer = AnchorConsumer::default();
    let mut handles = vec![];

    for (((secret_key, key_pair), bind_address), peer_id) in keys
        .into_iter()
        .zip(bind_addresses.into_iter())
        .zip(peer_info.iter().map(|p| p.id))
    {
        let all_peers = all_peers.clone();
        let peer_info = peer_info.clone();
        let run_guard = run_guard.clone();
        let (committed_tx, committed_rx) = mpsc::unbounded_channel();
        let collator_round = anchor_consumer.collator_round().clone();
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
                        let (dht_client, overlay_service) = from_validator(
                            bind_address,
                            &secret_key,
                            None,
                            DhtConfig {
                                local_info_announce_period: Duration::from_secs(1),
                                local_info_announce_period_max_jitter: Duration::from_secs(1),
                                routing_table_refresh_period: Duration::from_secs(1),
                                routing_table_refresh_period_max_jitter: Duration::from_secs(1),
                                ..Default::default()
                            },
                            NetworkConfig::default(),
                        );
                        for info in &peer_info {
                            if info.id != dht_client.network().peer_id() {
                                dht_client
                                    .add_peer(info.clone())
                                    .expect("add peer to dht client");
                            }
                        }
                        let (mock_storage, _tmp_dir) = Storage::new_temp().expect("new storage");
                        let mut engine = Engine::new(
                            key_pair,
                            &dht_client,
                            &overlay_service,
                            mock_storage.mempool_storage(),
                            committed_tx.clone(),
                            &collator_round,
                            InputBufferStub::new(cli.points_in_step, cli.steps_until_full),
                        );
                        engine.init_with_genesis(&all_peers).await;
                        tracing::info!("created engine {}", dht_client.network().peer_id());
                        tokio::try_join!(
                            engine.run().map(|_| Err::<(), ()>(())),
                            run_guard.until_any_dropped()
                        )
                        .ok();
                    });
            });
        handles.push(handle);
    }

    let handle = std::thread::Builder::new()
        .name("anchor-consumer".to_string())
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .worker_threads(1)
                .build()
                .expect("new tokio runtime")
                .block_on(async {
                    tokio::try_join!(
                        anchor_consumer.check().map(|_| Err::<(), ()>(())),
                        run_guard.until_any_dropped()
                    )
                    .ok();
                });
        });
    handles.push(handle);
    for handle in handles {
        match handle {
            Ok(handle) => _ = handle.join(),
            Err(_ignored) => {} // from std::thread::spawn()
        }
    }
    // for all panic hooks
    use std::io::Write;
    std::io::stderr().flush().ok();
    std::io::stdout().flush().ok();
}

fn check_parking_lot() {
    // Create a background thread which checks for deadlocks every 3s
    std::thread::spawn(move || loop {
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
    });
}

fn heart_beat(duration: Option<Duration>) {
    std::thread::spawn(move || {
        let start = std::time::Instant::now();
        loop {
            std::thread::sleep(Duration::from_secs(1));
            tracing::info!("heart beat");
            if let Some(duration) = duration {
                if start.elapsed() >= duration {
                    tracing::info!(
                        "test stopped after {}",
                        humantime::format_duration(start.elapsed())
                    );
                    use std::io::Write;
                    std::io::stderr().flush().ok();
                    std::io::stdout().flush().ok();
                    std::process::exit(0);
                }
            }
        }
    });
}
#[derive(Clone)]
struct RunGuard {
    notify: Arc<Notify>,
}

impl Drop for RunGuard {
    fn drop(&mut self) {
        self.notify.notify_waiters();
    }
}

impl RunGuard {
    pub fn new() -> Self {
        let notify = Arc::new(Notify::new());
        Self { notify }
    }
    pub async fn until_any_dropped(self) -> Result<(), ()> {
        self.notify.notified().await;
        Err(())
    }
}
