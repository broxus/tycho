use std::collections::HashSet;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use alloc_metrics::{Metrics, global_metrics};
use hdrhistogram::Histogram;
use parking_lot::RwLock;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rand_distr::weighted::WeightedAliasIndex;
use rand_distr::{Distribution, LogNormal};
use tycho_types::models::StdAddr;
use uuid::Uuid;

use crate::api::SubscriberManagerConfig;
use crate::manager::SubscriberManager;
use crate::perf_tests::client::Client;

pub const NUM_THREADS: usize = 8;
pub const NUM_CLIENTS: usize = 1_000_000;
pub const NUM_GLOBAL_HOT_ADDRS: usize = 8;
pub const NUM_SHARED_ADDRS: usize = 200_000;
pub const DEFAULT_TEST_DURATION_SECS: u64 = 60;
pub const ZIPF_EXPONENT: f64 = 1.5;
pub const PROB_SUB: f64 = 0.08;
pub const PROB_UNSUB: f64 = 0.04;
pub const PROB_DISCONNECT: f64 = 0.001;
pub const PROB_START_SWAP: f64 = 0.002;
pub const PROB_FULL_RESUB: f64 = 0.0001;
pub const BLOCK_INTERVAL_MS: u64 = 1_000;
pub const MAX_ADDRS_PER_BLOCK: usize = 256;
pub const WALLET_FRACTION: f64 = 0.1;
pub const NOTIFY_HOT_FRACTION: f64 = 0.7;
pub const BLOCK_FRACTION_HOT: f64 = 0.6;
pub const BLOCK_FRACTION_WALLET: f64 = WALLET_FRACTION;
pub const SWAP_AVG_LIFETIME_BLOCKS: u64 = 10;
pub const MIN_RESUB_BLOCKS: u64 = 300;
pub const NUM_PAIR_ADDRS: usize = 50_000;

pub fn make_seeded_rng() -> StdRng {
    if let Ok(s) = std::env::var("SUBSCRIBER_FUZZ_SEED")
        && let Ok(seed) = s.parse::<u64>()
    {
        return StdRng::seed_from_u64(seed);
    }
    let mut base = rand::rng();
    StdRng::from_rng(&mut base)
}

pub fn make_thread_rng(seed: u64, thread_id: usize) -> StdRng {
    let mixed = seed ^ (thread_id as u64).wrapping_mul(0x9E37_79B9);
    StdRng::seed_from_u64(mixed)
}

#[derive(Clone, Copy, Debug)]
pub struct PerfConfig {
    pub num_threads: usize,
    pub num_clients: usize,
    pub num_global_hot_addrs: usize,
    pub num_shared_addrs: usize,
    pub test_duration_secs: u64,
    pub zipf_exponent: f64,
    pub prob_sub: f64,
    pub prob_unsub: f64,
    pub prob_disconnect: f64,
    pub block_interval_ms: u64,
    pub max_addrs_per_block: usize,
}

impl Default for PerfConfig {
    fn default() -> Self {
        Self {
            num_threads: NUM_THREADS,
            num_clients: NUM_CLIENTS,
            num_global_hot_addrs: NUM_GLOBAL_HOT_ADDRS,
            num_shared_addrs: NUM_SHARED_ADDRS,
            test_duration_secs: DEFAULT_TEST_DURATION_SECS,
            zipf_exponent: ZIPF_EXPONENT,
            prob_sub: PROB_SUB,
            prob_unsub: PROB_UNSUB,
            prob_disconnect: PROB_DISCONNECT,
            block_interval_ms: BLOCK_INTERVAL_MS,
            max_addrs_per_block: MAX_ADDRS_PER_BLOCK,
        }
    }
}

impl PerfConfig {
    pub fn prob_notify(&self) -> f64 {
        1.0 - (self.prob_sub + self.prob_unsub + self.prob_disconnect)
    }

    pub fn tiny_smoke() -> Self {
        Self {
            num_threads: 1,
            num_clients: 200,
            num_global_hot_addrs: 4,
            num_shared_addrs: 1_000,
            test_duration_secs: 3,
            ..Self::default()
        }
    }

    pub fn action_weights(&self) -> [f64; 4] {
        [
            self.prob_notify(),
            self.prob_sub,
            self.prob_unsub,
            self.prob_disconnect,
        ]
    }

    pub fn block_action_weights(&self) -> [f64; 6] {
        let used = PROB_START_SWAP
            + self.prob_sub
            + self.prob_unsub
            + self.prob_disconnect
            + PROB_FULL_RESUB;
        let expire = (1.0 - used).max(0.0);
        [
            PROB_START_SWAP,
            self.prob_sub,
            self.prob_unsub,
            self.prob_disconnect,
            PROB_FULL_RESUB,
            expire,
        ]
    }

    pub fn duration(&self) -> Duration {
        Duration::from_secs(self.test_duration_secs)
    }

    pub fn shared_start(&self) -> usize {
        self.num_global_hot_addrs + self.num_clients
    }

    pub fn pair_range(&self) -> Range<usize> {
        let start = self.shared_start();
        let end = start + NUM_PAIR_ADDRS.min(self.num_shared_addrs);
        start..end
    }

    pub fn shared_token_range(&self) -> Range<usize> {
        let start = self.shared_start();
        let pair_end = start + NUM_PAIR_ADDRS.min(self.num_shared_addrs);
        pair_end..(start + self.num_shared_addrs)
    }

    pub fn shared_addr_range(&self) -> Range<usize> {
        let start = self.shared_start();
        start..(start + self.num_shared_addrs)
    }
}

#[derive(Debug)]
pub struct BranchHists {
    pub notify: Histogram<u64>,
    pub subscribe: Histogram<u64>,
    pub unsubscribe: Histogram<u64>,
    pub disconnect: Histogram<u64>,
}

impl BranchHists {
    pub fn new() -> Self {
        fn hist() -> Histogram<u64> {
            Histogram::<u64>::new(3).unwrap()
        }
        Self {
            notify: hist(),
            subscribe: hist(),
            unsubscribe: hist(),
            disconnect: hist(),
        }
    }

    pub fn merge_from(&mut self, other: &BranchHists) {
        self.notify.add(&other.notify).unwrap();
        self.subscribe.add(&other.subscribe).unwrap();
        self.unsubscribe.add(&other.unsubscribe).unwrap();
        self.disconnect.add(&other.disconnect).unwrap();
    }

    pub fn assert_non_empty(&self) {
        assert!(!self.notify.is_empty(), "notify histogram is empty");
        assert!(!self.subscribe.is_empty(), "subscribe histogram is empty");
        assert!(
            !self.unsubscribe.is_empty(),
            "unsubscribe histogram is empty"
        );
        assert!(!self.disconnect.is_empty(), "disconnect histogram is empty");
    }

    pub fn print(&self) {
        println!("Latency quantiles (per branch, in µs):");
        print_hist("NOTIFY      (clients_to_notify)", &self.notify);
        print_hist("SUBSCRIBE   (subscribe)", &self.subscribe);
        print_hist("UNSUBSCRIBE (unsubscribe)", &self.unsubscribe);
        print_hist("DISCONNECT  (remove_client)", &self.disconnect);
        println!("---------------------------------------------------");
    }
}

pub fn measure<F>(hist: &mut Histogram<u64>, f: F)
where
    F: FnOnce(),
{
    let start = Instant::now();
    f();
    let _ = hist.record(start.elapsed().as_micros() as u64);
}

fn print_hist(label: &str, h: &Histogram<u64>) {
    if h.is_empty() {
        println!("{label}: no samples");
        return;
    }

    let p50 = h.value_at_quantile(0.50);
    let p90 = h.value_at_quantile(0.90);
    let p99 = h.value_at_quantile(0.99);
    let p999 = h.value_at_quantile(0.999);
    let max = h.max();

    println!("{label}:");
    println!("  count = {}", h.len());
    println!("  p50   = {}", format_latency(p50));
    println!("  p90   = {}", format_latency(p90));
    println!("  p99   = {}", format_latency(p99));
    println!("  p99.9 = {}", format_latency(p999));
    println!("  max   = {}", format_latency(max));
}

fn format_latency(micros: u64) -> String {
    const MICROS_PER_MILLI: u64 = 1_000;
    const MICROS_PER_SEC: u64 = 1_000_000;
    const SECS_PER_MIN: u64 = 60;
    const SECS_PER_HOUR: u64 = SECS_PER_MIN * SECS_PER_MIN;

    let total_secs = micros / MICROS_PER_SEC;
    let micros = micros % MICROS_PER_SEC;

    let hours = total_secs / SECS_PER_HOUR;
    let minutes = (total_secs % SECS_PER_HOUR) / SECS_PER_MIN;
    let seconds = total_secs % SECS_PER_MIN;

    let millis = micros / MICROS_PER_MILLI;
    let micros = micros % MICROS_PER_MILLI;

    let mut parts = Vec::new();
    if hours > 0 {
        parts.push(format!("{hours}h"));
    }
    if minutes > 0 {
        parts.push(format!("{minutes}m"));
    }
    if seconds > 0 {
        parts.push(format!("{seconds}s"));
    }

    if millis > 0 || micros > 0 || parts.is_empty() {
        if micros == 0 {
            parts.push(format!("{millis}ms"));
        } else {
            let fraction = format!("{micros:03}");
            let fraction = fraction.trim_end_matches('0');
            parts.push(format!("{millis}.{fraction}ms"));
        }
    }

    parts.join(" ")
}

struct AliasTables {
    hot_alias: WeightedAliasIndex<f64>,
    shared_alias: WeightedAliasIndex<f64>,
}

fn build_clients(cfg: &PerfConfig, rng: &mut StdRng) -> Arc<Vec<Client>> {
    const MEDIAN_SUBS_PER_CLIENT: f64 = 5.0;
    const MAX_SUBS_PER_CLIENT: usize = 255;

    let dist = LogNormal::new(MEDIAN_SUBS_PER_CLIENT.ln(), 1.0).unwrap();

    let mut clients = Vec::with_capacity(cfg.num_clients);
    for _ in 0..cfg.num_clients {
        let uuid = Uuid::from_bytes(rand::random::<[u8; 16]>());
        let target = dist
            .sample(rng)
            .round()
            .clamp(1.0, MAX_SUBS_PER_CLIENT as f64) as usize;
        clients.push(Client::new(uuid, target));
    }
    Arc::new(clients)
}

fn build_addrs(cfg: &PerfConfig) -> Arc<Vec<StdAddr>> {
    let num_addrs = cfg.num_global_hot_addrs + cfg.num_clients + cfg.num_shared_addrs;
    let addrs: Vec<StdAddr> = (0..num_addrs)
        .map(|i| StdAddr {
            anycast: None,
            workchain: 0,
            address: {
                let mut bytes = [0u8; 32];
                bytes[0..4].copy_from_slice(&(i as u32).to_be_bytes());
                bytes.into()
            },
        })
        .collect();
    Arc::new(addrs)
}

fn build_alias_tables(cfg: &PerfConfig) -> AliasTables {
    let hot_weights: Vec<f64> = (0..cfg.num_global_hot_addrs)
        .map(|i| 1.0 / ((i + 1) as f64).powf(cfg.zipf_exponent))
        .collect();
    let shared_weights: Vec<f64> = (0..cfg.num_shared_addrs)
        .map(|i| 1.0 / ((i + 1) as f64).powf(cfg.zipf_exponent))
        .collect();

    AliasTables {
        hot_alias: WeightedAliasIndex::new(hot_weights).expect("hot alias"),
        shared_alias: WeightedAliasIndex::new(shared_weights).expect("shared alias"),
    }
}

#[derive(Clone)]
pub struct PerfWorld {
    pub cfg: PerfConfig,
    pub clients: Arc<Vec<Client>>,
    pub addrs: Arc<Vec<StdAddr>>,
    pub hot_alias: Arc<WeightedAliasIndex<f64>>,
    pub shared_alias: Arc<WeightedAliasIndex<f64>>,
    pub manager: SubscriberManager,
}

impl PerfWorld {
    pub fn new(cfg: PerfConfig, rng: &mut StdRng) -> Self {
        let clients = build_clients(&cfg, rng);
        let addrs = build_addrs(&cfg);
        let alias = build_alias_tables(&cfg);

        let manager = SubscriberManager::new(SubscriberManagerConfig::new(
            (addrs.len() as u32) * 2,
            (cfg.num_clients as u32) * 2,
        ));

        Self {
            cfg,
            clients,
            addrs,
            hot_alias: Arc::new(alias.hot_alias),
            shared_alias: Arc::new(alias.shared_alias),
            manager,
        }
    }

    pub fn shared_start(&self) -> usize {
        self.cfg.shared_start()
    }

    pub fn sample_notify_addr<R: Rng + ?Sized>(&self, rng: &mut R) -> StdAddr {
        let choice: f64 = rng.random();

        if choice < NOTIFY_HOT_FRACTION {
            let rank = self.hot_alias.sample(rng);
            self.addrs[rank].clone()
        } else if choice < NOTIFY_HOT_FRACTION + WALLET_FRACTION {
            let wallet_owner = rng.random_range(0..self.cfg.num_clients);
            let wallet_idx = self.cfg.num_global_hot_addrs + wallet_owner;
            self.addrs[wallet_idx].clone()
        } else {
            let idx = self.shared_start() + self.shared_alias.sample(rng);
            self.addrs[idx].clone()
        }
    }

    pub fn pick_new_subscription<R: Rng + ?Sized>(
        &self,
        rng: &mut R,
        current_subs: &RwLock<HashSet<StdAddr>>,
    ) -> Option<StdAddr> {
        let subs = current_subs.read();
        (0..5)
            .map(|_| self.shared_start() + self.shared_alias.sample(rng))
            .map(|idx| self.addrs[idx].clone())
            .find(|addr| !subs.contains(addr))
    }
}

pub fn warmup_initial_subscriptions(world: &PerfWorld, rng: &mut StdRng) {
    let cfg = &world.cfg;
    let shared_range = cfg.shared_addr_range();
    let shared_len = shared_range.len();
    let clients = &world.clients;
    let addrs = &world.addrs;
    let manager = &world.manager;

    for (client_idx, client) in clients.iter().enumerate() {
        subscribe_wallet(cfg, addrs, manager, client_idx, client);

        let mut subs = client.subscriptions.write();

        for addr_idx in 0..cfg.num_global_hot_addrs {
            let addr = addrs[addr_idx].clone();
            if manager.subscribe_many(client.uuid, [addr.clone()]).is_ok() {
                subs.insert(addr);
            }
        }

        while subs.len() < client.target_subs && shared_len > 0 {
            let shared_idx = shared_range.start + rng.random_range(0..shared_len);
            let shared_addr = addrs[shared_idx].clone();
            if !subs.contains(&shared_addr)
                && manager
                    .subscribe_many(client.uuid, [shared_addr.clone()])
                    .is_ok()
            {
                subs.insert(shared_addr);
            }
        }
    }
}

fn subscribe_wallet(
    cfg: &PerfConfig,
    addrs: &Arc<Vec<StdAddr>>,
    manager: &SubscriberManager,
    client_idx: usize,
    client: &Client,
) {
    let wallet_idx = cfg.num_global_hot_addrs + client_idx;
    let wallet_addr = addrs[wallet_idx].clone();
    if manager
        .subscribe_many(client.uuid, [wallet_addr.clone()])
        .is_ok()
    {
        client.subscriptions.write().insert(wallet_addr);
    }
}

pub fn warmup_block_driven(world: &PerfWorld) {
    let cfg = &world.cfg;
    let clients = &world.clients;
    let addrs = &world.addrs;
    let manager = &world.manager;
    let hot_alias = world.hot_alias.as_ref();
    let shared_alias = world.shared_alias.as_ref();

    let token_start = cfg.shared_start();
    let token_len = cfg.num_shared_addrs;
    let mut warm_rng = make_seeded_rng();

    for (client_idx, client) in clients.iter().enumerate() {
        subscribe_wallet(cfg, addrs, manager, client_idx, client);

        let mut subs = client.subscriptions.write();

        let num_hot = warm_rng.random_range(1..=cfg.num_global_hot_addrs.min(3));
        for _ in 0..num_hot {
            let hot_idx = hot_alias.sample(&mut warm_rng);
            let addr = addrs[hot_idx].clone();
            if manager.subscribe_many(client.uuid, [addr.clone()]).is_ok() {
                subs.insert(addr);
            }
        }

        while subs.len() < client.target_subs && token_len > 0 {
            let shared_idx = token_start + (shared_alias.sample(&mut warm_rng) % token_len);
            let shared_addr = addrs[shared_idx].clone();
            if !subs.contains(&shared_addr)
                && manager
                    .subscribe_many(client.uuid, [shared_addr.clone()])
                    .is_ok()
            {
                subs.insert(shared_addr);
            }
        }

        client.last_full_resub_block.store(0, Ordering::Relaxed);
    }
}

pub fn print_distribution_check(
    cfg: &PerfConfig,
    manager: &SubscriberManager,
    addrs: &Arc<Vec<StdAddr>>,
) {
    println!("Distribution Check:");
    let mut clients = Vec::new();

    if let Some(hot_addr) = addrs.first() {
        manager.clients_to_notify(hot_addr.clone(), &mut clients);
        let hot_count = clients.len();
        println!("  USDT-like hot addr subscribers: {}", hot_count);
    }

    let wallet_idx = cfg.num_global_hot_addrs;
    let wallet_addr = addrs[wallet_idx].clone();
    manager.clients_to_notify(wallet_addr, &mut clients);
    let wallet_count = clients.len();
    println!(
        "  Wallet Addr (client 0) subscribers: {} (should be <= 1)",
        wallet_count
    );

    let shared_idx = cfg.shared_start() + cfg.num_shared_addrs / 2;
    let shared_addr = addrs[shared_idx].clone();
    manager.clients_to_notify(shared_addr, &mut clients);
    let shared_count = clients.len();
    println!("  Shared Addr (mid rank) subscribers: {}", shared_count);
}

static METRICS_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

pub fn metrics_guard() -> std::sync::MutexGuard<'static, ()> {
    METRICS_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap()
}

pub fn setup_perf_test(
    cfg: PerfConfig,
) -> (
    std::sync::MutexGuard<'static, ()>,
    StdRng,
    Arc<PerfWorld>,
    Metrics,
) {
    let guard = metrics_guard();
    let mut rng = make_seeded_rng();
    let alloc_before = global_metrics();
    let world = Arc::new(PerfWorld::new(cfg, &mut rng));
    (guard, rng, world, alloc_before)
}

pub fn assert_no_leak(label: &str, before: Metrics, after: Metrics) {
    let delta = after - before;
    const ALLOC_TOLERANCE_BYTES: isize = 4 * 1024;
    const ALLOC_TOLERANCE_OPS: isize = 32;
    assert!(
        delta.allocated_bytes <= ALLOC_TOLERANCE_BYTES,
        "{label} should not leak bytes (delta={})",
        delta.allocated_bytes
    );
    assert!(
        delta.allocations <= ALLOC_TOLERANCE_OPS,
        "{label} should balance alloc/dealloc (delta={})",
        delta.allocations
    );
}

pub fn log_alloc_delta(label: &str, before: Metrics) -> Metrics {
    let after = global_metrics();
    let delta = after - before;
    println!(
        "{label}: bytes={} ops={} total_bytes={} total_ops={}",
        delta.allocated_bytes, delta.allocations, after.allocated_bytes, after.allocations
    );
    after
}

#[derive(Debug)]
pub struct RunResult {
    pub hists: BranchHists,
    pub ops: usize,
    pub duration: Duration,
}
pub type Worker = Arc<
    dyn Fn(
            usize,
            u64,
            Arc<PerfWorld>,
            Arc<AtomicBool>,
            Arc<AtomicUsize>,
            Arc<Barrier>,
        ) -> BranchHists
        + Send
        + Sync,
>;
pub type ChainWorker =
    Arc<dyn Fn(Arc<PerfWorld>, Arc<AtomicBool>, Arc<Barrier>) -> BranchHists + Send + Sync>;

pub fn run_clients(world: Arc<PerfWorld>, chain: Option<ChainWorker>, worker: Worker) -> RunResult {
    let stop = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(
        world.cfg.num_threads + 1 + chain.is_some() as usize,
    ));

    let mut seed_rng = make_seeded_rng();
    let seeds: Vec<u64> = (0..world.cfg.num_threads)
        .map(|_| seed_rng.random())
        .collect();

    let chain_handle = chain.map(|c| {
        let world = world.clone();
        let stop = stop.clone();
        let barrier = barrier.clone();
        thread::spawn(move || c(world, stop, barrier))
    });

    let mut handles = Vec::with_capacity(world.cfg.num_threads);
    for (id, seed) in seeds.into_iter().enumerate() {
        let world = world.clone();
        let stop = stop.clone();
        let ops = ops.clone();
        let barrier = barrier.clone();
        let worker = worker.clone();
        handles.push(thread::spawn(move || {
            worker(id, seed, world, stop, ops, barrier)
        }));
    }

    barrier.wait();
    let start = Instant::now();
    thread::sleep(world.cfg.duration());
    stop.store(true, Ordering::Relaxed);

    let mut hists = BranchHists::new();
    for h in handles {
        hists.merge_from(&h.join().unwrap());
    }
    if let Some(h) = chain_handle {
        hists.merge_from(&h.join().unwrap());
    }

    let duration = start.elapsed();
    let ops_count = ops.load(Ordering::Relaxed);
    RunResult {
        hists,
        ops: ops_count,
        duration,
    }
}
