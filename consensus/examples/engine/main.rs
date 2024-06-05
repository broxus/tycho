use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use everscale_crypto::ed25519::{KeyPair, SecretKey};
use parking_lot::deadlock;
use tokio::sync::{mpsc, Mutex};
use tycho_consensus::test_utils::*;
use tycho_consensus::{Engine, InputBufferStub};
use tycho_network::{Address, DhtConfig, NetworkConfig, PeerId};
use tycho_util::FastHashMap;

mod logger;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Cli::parse().run().await
}

/// Tycho network node.
#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// total nodes populated as separate tokio runtimes
    #[arg(short, long, default_value_t = 4)]
    nodes: usize,
    /// tokio worker threads per node
    #[arg(short, long, default_value_t = 2)]
    workers_per_node: usize,
    /// step is an amount of points produced by every node independently
    #[arg(short, long, default_value_t = 33)]
    points_in_step: usize,
    /// number of steps in which payload will increase from 0
    /// to [PAYLOAD_BATCH_BYTES](tycho_consensus::MempoolConfig::PAYLOAD_BATCH_BYTES)
    #[arg(short, long, default_value_t = 3)]
    steps_until_full: usize,
    /// generate data for span-aware flame graph (changes log format);
    /// follows https://github.com/tokio-rs/tracing/tree/master/tracing-flame#generating-the-image,
    /// but results are in git-ignored `./.temp` dir
    #[arg(short, long, default_value_t = false)]
    flame: bool,
}

impl Cli {
    async fn run(self) -> anyhow::Result<()> {
        let fun = if self.flame {
            logger::flame
        } else {
            logger::spans
        };
        fun(
            "engine_works",
            "info,tycho_consensus=info,tycho_network=info",
        );
        check_parking_lot();
        heart_beat();
        let handles = make_network(self);
        for handle in handles {
            handle.join().unwrap();
        }
        Ok(())
    }
}

fn make_network(cli: Cli) -> Vec<std::thread::JoinHandle<()>> {
    let keys = (0..cli.nodes)
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

    let anchors_map = Arc::new(Mutex::new(FastHashMap::default()));
    let refs_map = Arc::new(Mutex::new(FastHashMap::default()));

    let mut handles = vec![];
    for (((secret_key, key_pair), bind_address), peer_id) in keys
        .into_iter()
        .zip(bind_addresses.into_iter())
        .zip(peer_info.iter().map(|p| p.id))
    {
        let all_peers = all_peers.clone();
        let peer_info = peer_info.clone();
        let anchors_map = anchors_map.clone();
        let refs_map = refs_map.clone();

        let handle = std::thread::Builder::new()
            .name(format!("engine-{peer_id:.4}"))
            .spawn(move || {
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(cli.workers_per_node)
                    // .thread_name(format!("tokio-{}", peer_id.alt()))
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

                        let (committed_tx, committed_rx) = mpsc::unbounded_channel();
                        tokio::spawn(check_anchors(
                            committed_rx,
                            peer_id,
                            anchors_map,
                            refs_map,
                            cli.nodes,
                        ));
                        // tokio::spawn(drain_anchors(committed_rx));
                        let mut engine = Engine::new(
                            key_pair,
                            &dht_client,
                            &overlay_service,
                            committed_tx.clone(),
                            InputBufferStub::new(cli.points_in_step, cli.steps_until_full),
                        );
                        engine.init_with_genesis(all_peers.as_slice()).await;
                        tracing::info!("created engine {}", dht_client.network().peer_id());
                        engine.run().await;
                    });
            })
            .unwrap();
        handles.push(handle);
    }
    handles
}

fn check_parking_lot() {
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_secs(1));
        let deadlocks = deadlock::check_deadlock();
        if deadlocks.is_empty() {
            continue;
        }

        tracing::error!("{} deadlocks detected", deadlocks.len());
        for (i, threads) in deadlocks.iter().enumerate() {
            tracing::error!("Deadlock #{}", i);
            for t in threads {
                tracing::error!("Thread Id {:#?}", t.thread_id());
                tracing::error!("{:#?}", t.backtrace());
            }
        }
    });
}

fn heart_beat() {
    // Create a background thread which checks for deadlocks every 10s
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_secs(1));
        tracing::info!("heart beat");
    });
}
