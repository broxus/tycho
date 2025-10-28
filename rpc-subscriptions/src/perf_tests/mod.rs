use std::alloc::System;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use alloc_metrics::{MetricAlloc, global_metrics};
use rand::distr::Distribution;
use rand::distr::weighted::WeightedIndex;
use rand::prelude::*;
use tycho_types::models::StdAddr;

mod client;
pub mod common;

use common::{
    BLOCK_FRACTION_HOT, BLOCK_FRACTION_WALLET, BranchHists, ChainWorker, MIN_RESUB_BLOCKS,
    PerfConfig, PerfWorld, RunResult, SWAP_AVG_LIFETIME_BLOCKS, Worker, assert_no_leak,
    log_alloc_delta, make_seeded_rng, make_thread_rng, measure, metrics_guard,
    print_distribution_check, run_clients, setup_perf_test, warmup_block_driven,
    warmup_initial_subscriptions,
};

#[global_allocator]
static GLOBAL: MetricAlloc<System> = MetricAlloc::new(System);

#[derive(Copy, Clone)]
enum Action {
    Notify,
    Subscribe,
    Unsubscribe,
    Disconnect,
    StartSwap,
    FullResub,
    Expire,
}

fn log_run(label: &str, result: &RunResult) {
    let ops_per_sec = if result.duration.as_secs_f64() > 0.0 {
        result.ops as f64 / result.duration.as_secs_f64()
    } else {
        0.0
    };
    println!("---------------------------------------------------");
    println!("{label}");
    println!("Total Operations: {}", result.ops);
    println!("Duration:         {:.2?}", result.duration);
    println!("Throughput:       {:.2} op/s", ops_per_sec);
    println!("---------------------------------------------------");
}

fn make_zipf_worker(actions: [Action; 4], dist: WeightedIndex<f64>) -> Worker {
    Arc::new(move |id, seed, world, stop, ops, barrier| {
        let mut rng = make_thread_rng(seed, id);
        let mut hists = BranchHists::new();
        let mut clients = Vec::new();
        let num_clients = world.clients.len();
        let dist = dist.clone();

        barrier.wait();

        while !stop.load(Ordering::Relaxed) {
            let client_idx = rng.random_range(0..num_clients);
            let client = &world.clients[client_idx];
            let uuid = client.uuid;

            match actions[dist.sample(&mut rng)] {
                Action::Notify => measure(&mut hists.notify, || {
                    let addr = world.sample_notify_addr(&mut rng);
                    world.manager.clients_to_notify(addr, &mut clients);
                }),
                Action::Subscribe => measure(&mut hists.subscribe, || {
                    if client.subscriptions.read().len() >= client.target_subs {
                        let wallet_owner = rng.random_range(0..num_clients);
                        let wallet_idx = world.cfg.num_global_hot_addrs + wallet_owner;
                        let addr = world.addrs[wallet_idx].clone();
                        world.manager.clients_to_notify(addr, &mut clients);
                        return;
                    }

                    if let Some(addr) = world.pick_new_subscription(&mut rng, &client.subscriptions)
                        && world.manager.subscribe_many(uuid, [addr.clone()]).is_ok()
                    {
                        client.subscriptions.write().insert(addr);
                    }
                }),
                Action::Unsubscribe => measure(&mut hists.unsubscribe, || {
                    let next = {
                        let subs = client.subscriptions.read();
                        if subs.len() <= 1 {
                            None
                        } else {
                            subs.iter().next().cloned()
                        }
                    };
                    if let Some(addr) = next
                        && world.manager.unsubscribe(uuid, addr.clone()).is_ok()
                    {
                        client.subscriptions.write().remove(&addr);
                    }
                }),
                Action::Disconnect => measure(&mut hists.disconnect, || {
                    world.manager.remove_client(uuid);
                    client.subscriptions.write().clear();
                }),
                _ => {}
            }

            ops.fetch_add(1, Ordering::Relaxed);
        }

        hists
    })
}

fn make_block_worker(
    actions: &'static [Action],
    dist: WeightedIndex<f64>,
    current_block: Arc<AtomicU64>,
) -> Worker {
    Arc::new(move |_id, _seed, world, stop, ops, barrier| {
        let mut rng = rand::rng();
        let mut hists = BranchHists::new();
        let num_clients = world.clients.len();
        let pair = world.cfg.pair_range();
        let pair_len = pair.len();
        let token_range = world.cfg.shared_token_range();
        let token_len = token_range.len();
        let token_start = token_range.start;
        let dist = dist.clone();

        barrier.wait();

        while !stop.load(Ordering::Relaxed) {
            let client_idx = rng.random_range(0..num_clients);
            let client = &world.clients[client_idx];
            let uuid = client.uuid;
            let now_block = current_block.load(Ordering::Relaxed);

            match actions[dist.sample(&mut rng)] {
                Action::StartSwap => measure(&mut hists.subscribe, || {
                    if pair_len > 0 {
                        let idx = pair.start + (world.shared_alias.sample(&mut rng) % pair_len);
                        let addr = world.addrs[idx].clone();
                        let missing = !client.subscriptions.read().contains(&addr);
                        if missing && world.manager.subscribe_many(uuid, [addr.clone()]).is_ok() {
                            client.subscriptions.write().insert(addr.clone());
                            client
                                .temp_subs
                                .insert(addr, now_block + SWAP_AVG_LIFETIME_BLOCKS);
                        }
                    }
                }),
                Action::Subscribe => measure(&mut hists.subscribe, || {
                    if client.subscriptions.read().len() < client.target_subs && token_len > 0 {
                        let candidate = (0..5)
                            .map(|_| {
                                token_start + (world.shared_alias.sample(&mut rng) % token_len)
                            })
                            .map(|idx| world.addrs[idx].clone())
                            .find(|candidate| !client.subscriptions.read().contains(candidate));
                        if let Some(addr) = candidate
                            && world.manager.subscribe_many(uuid, [addr.clone()]).is_ok()
                        {
                            client.subscriptions.write().insert(addr);
                        }
                    }
                }),
                Action::Unsubscribe => measure(&mut hists.unsubscribe, || {
                    let wallet_idx = world.cfg.num_global_hot_addrs + client_idx;
                    let wallet_addr = world.addrs[wallet_idx].clone();
                    let candidate = client
                        .subscriptions
                        .read()
                        .iter()
                        .find(|addr| {
                            let addr_ref: &StdAddr = addr;
                            let is_hot = world.addrs[..world.cfg.num_global_hot_addrs]
                                .iter()
                                .any(|a| a == addr_ref);
                            let is_wallet = addr_ref == &wallet_addr;
                            !is_hot && !is_wallet
                        })
                        .cloned();
                    if let Some(addr) = candidate {
                        let _ = world.manager.unsubscribe(uuid, addr.clone());
                        client.subscriptions.write().remove(&addr);
                        client.temp_subs.remove(&addr);
                    }
                }),
                Action::Disconnect => measure(&mut hists.disconnect, || {
                    world.manager.remove_client(uuid);
                    client.subscriptions.write().clear();
                    client.temp_subs.clear();
                    client
                        .last_full_resub_block
                        .store(now_block, Ordering::Relaxed);
                }),
                Action::FullResub => {
                    if now_block
                        .saturating_sub(client.last_full_resub_block.load(Ordering::Relaxed))
                        >= MIN_RESUB_BLOCKS
                    {
                        world.manager.remove_client(uuid);
                        client.subscriptions.write().clear();
                        client.temp_subs.clear();

                        let wallet_idx = world.cfg.num_global_hot_addrs + client_idx;
                        let wallet_addr = world.addrs[wallet_idx].clone();
                        if world
                            .manager
                            .subscribe_many(uuid, [wallet_addr.clone()])
                            .is_ok()
                        {
                            client.subscriptions.write().insert(wallet_addr);
                        }

                        let num_hot = rng.random_range(1..=world.cfg.num_global_hot_addrs.min(3));
                        for _ in 0..num_hot {
                            let hot_idx = world.hot_alias.sample(&mut rng);
                            let addr = world.addrs[hot_idx].clone();
                            if world.manager.subscribe_many(uuid, [addr.clone()]).is_ok() {
                                client.subscriptions.write().insert(addr);
                            }
                        }

                        while client.subscriptions.read().len() < client.target_subs
                            && token_len > 0
                        {
                            let shared_idx =
                                token_start + (world.shared_alias.sample(&mut rng) % token_len);
                            let shared_addr = world.addrs[shared_idx].clone();
                            if !client.subscriptions.read().contains(&shared_addr)
                                && world
                                    .manager
                                    .subscribe_many(uuid, [shared_addr.clone()])
                                    .is_ok()
                            {
                                client.subscriptions.write().insert(shared_addr);
                            }
                        }

                        client
                            .last_full_resub_block
                            .store(now_block, Ordering::Relaxed);
                    }
                }
                Action::Expire => {
                    let mut expired = Vec::new();
                    for entry in client.temp_subs.iter() {
                        if *entry.value() <= now_block {
                            expired.push(entry.key().clone());
                        }
                    }
                    for addr in expired {
                        let _ = world.manager.unsubscribe(uuid, addr.clone());
                        client.subscriptions.write().remove(&addr);
                        client.temp_subs.remove(&addr);
                    }
                }
                Action::Notify => {}
            }

            ops.fetch_add(1, Ordering::Relaxed);
        }

        hists
    })
}

fn make_chain_worker(current_block: Arc<AtomicU64>) -> ChainWorker {
    Arc::new(move |world, stop, barrier| {
        let mut rng = make_seeded_rng();
        let mut hists = BranchHists::new();
        let mut clients = Vec::new();
        let shared_start = world.shared_start();

        barrier.wait();

        while !stop.load(Ordering::Relaxed) {
            let block_start = Instant::now();
            let _block_height = current_block.fetch_add(1, Ordering::Relaxed) + 1;

            let num_addrs = rng.random_range(1..=world.cfg.max_addrs_per_block);
            let mut indices = HashSet::with_capacity(num_addrs);

            while indices.len() < num_addrs {
                let roll: f64 = rng.random();
                let idx = if roll < BLOCK_FRACTION_HOT {
                    world.hot_alias.sample(&mut rng)
                } else if roll < BLOCK_FRACTION_HOT + BLOCK_FRACTION_WALLET {
                    let wallet_owner = rng.random_range(0..world.cfg.num_clients);
                    world.cfg.num_global_hot_addrs + wallet_owner
                } else {
                    let shared_idx = world.shared_alias.sample(&mut rng);
                    shared_start + shared_idx
                };

                if idx < world.addrs.len() {
                    indices.insert(idx);
                }
            }

            for idx in indices {
                let addr = world.addrs[idx].clone();
                measure(&mut hists.notify, || {
                    world.manager.clients_to_notify(addr, &mut clients);
                });
            }

            let elapsed = block_start.elapsed();
            let desired = Duration::from_millis(world.cfg.block_interval_ms);
            if elapsed < desired {
                thread::sleep(desired - elapsed);
            }
        }

        hists
    })
}

#[test]
#[ignore = "This is a long-running test, run manually"]
fn block_like() {
    println!("Preparing block-driven test data...");

    let (_metrics_guard, _rng, world, alloc_before) = setup_perf_test(PerfConfig::default());

    println!("Warming up initial subscriptions (block-driven)...");
    warmup_block_driven(world.as_ref());

    let current_block = Arc::new(AtomicU64::new(0));
    let chain = Some(make_chain_worker(current_block.clone()));

    let dist = WeightedIndex::new(world.cfg.block_action_weights()).unwrap();
    let worker = make_block_worker(
        &[
            Action::StartSwap,
            Action::Subscribe,
            Action::Unsubscribe,
            Action::Disconnect,
            Action::FullResub,
            Action::Expire,
        ],
        dist,
        current_block.clone(),
    );

    let result = run_clients(world.clone(), chain, worker);

    log_run("Block-driven test finished.", &result);

    result.hists.print();

    let _ = log_alloc_delta("Alloc snapshot (block-driven)", alloc_before);

    print_distribution_check(&world.cfg, &world.manager, &world.addrs);

    println!("Running invariant check...");
    world.manager.debug_check_invariants();
    println!("Invariant check OK.");

    println!(
        "Memory used by manager: {:?} bytes",
        world.manager.mem_snapshot()
    );
}

#[test]
fn zipfian_smoke_perf() {
    let _metrics_guard = metrics_guard();

    let mut rng = make_seeded_rng();
    let cfg = PerfConfig::tiny_smoke();

    let alloc_before = global_metrics();

    {
        let world = Arc::new(PerfWorld::new(cfg, &mut rng));

        warmup_initial_subscriptions(world.as_ref(), &mut rng);

        let dist = WeightedIndex::new(world.cfg.action_weights()).unwrap();
        let worker = make_zipf_worker(
            [
                Action::Notify,
                Action::Subscribe,
                Action::Unsubscribe,
                Action::Disconnect,
            ],
            dist,
        );

        let result = run_clients(world.clone(), None, worker);

        result.hists.assert_non_empty();

        world.manager.debug_check_invariants();
    }

    let after = log_alloc_delta("Alloc snapshot (smoke)", alloc_before);
    assert_no_leak("smoke test", alloc_before, after);
}
