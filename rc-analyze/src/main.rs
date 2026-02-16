use std::collections::VecDeque;
use std::fmt::Write as _;
use std::io::{self, BufReader, Read};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Parser;
use tycho_storage::fs::Dir;
use tycho_util::FastHashMap;

const COPS_MAGIC: [u8; 4] = *b"COPS";
const COPS_VERSION: u16 = 2;
const COPS_KIND_BLOCK: u16 = 1;
const COPS_KIND_DELTA: u16 = 2;
const MASTER_WORKCHAIN: i32 = -1;
const MASTER_SHARD_PREFIX: u64 = 0x8000_0000_0000_0000;

type Hash = [u8; 32];

#[derive(Parser, Debug)]
#[command(name = "rc-analyze")]
#[command(about = "Analyze COPS v2 refcount logs")]
struct Cli {
    file_path: PathBuf,
    #[arg(long)]
    from: u32,
    #[arg(long)]
    to: u32,
    #[arg(long, default_value_t = 50)]
    window_size: u32,
    #[arg(long, default_value_t = 10)]
    top_k: usize,
    #[arg(long, default_value_t = 100)]
    roots_kept: u32,
}

#[derive(Clone, Copy)]
struct BlockCtx {
    workchain: i32,
    shard_prefix: u64,
    seqno: u32,
}

#[derive(Clone, Copy)]
struct Delta {
    delta: i32,
    abs_val: u64,
    hash: Hash,
}

enum Rec {
    Block(BlockCtx),
    Delta(Delta),
}

#[derive(Clone, Copy)]
struct RefCountTransition {
    hash: Hash,
    seqno: u32,
    prev_val: u64,
    new_val: u64,
    delta: i64,
    abs_delta: u64,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Tier {
    U8,
    U16,
    U32,
    U64,
}

impl RefCountTransition {
    fn from_delta(seqno: u32, op: &Delta) -> Result<Self, TransitionError> {
        let delta = i64::from(op.delta);
        let prev_i128 = i128::from(op.abs_val) - i128::from(delta);
        if prev_i128 < 0 {
            return Err(TransitionError::PrevUnderflow);
        }
        let Ok(prev_val) = u64::try_from(prev_i128) else {
            return Err(TransitionError::PrevUnderflow);
        };

        Ok(Self {
            hash: op.hash,
            seqno,
            prev_val,
            new_val: op.abs_val,
            delta,
            abs_delta: delta.unsigned_abs(),
        })
    }

    fn is_alloc_flip(self) -> bool {
        self.prev_val == 0 && self.new_val > 1
    }

    fn is_free_flip(self) -> bool {
        self.prev_val > 1 && self.new_val == 1
    }

    fn is_resurrection(self) -> bool {
        self.prev_val == 0 && self.new_val > 0
    }

    fn is_death(self) -> bool {
        self.new_val == 0
    }

    fn tier_change(self) -> Option<(Tier, Tier)> {
        let prev = tier_for(self.prev_val);
        let next = tier_for(self.new_val);
        (prev != next).then_some((prev, next))
    }
}

enum TransitionError {
    PrevUnderflow,
}

fn tier_for(value: u64) -> Tier {
    if u8::try_from(value).is_ok() {
        return Tier::U8;
    }
    if u16::try_from(value).is_ok() {
        return Tier::U16;
    }
    if u32::try_from(value).is_ok() {
        return Tier::U32;
    }
    Tier::U64
}

#[derive(Default)]
struct ParseCounters {
    blocks: u64,
    deltas: u64,
}

#[derive(Default)]
struct ConsistencyTracker {
    invalid_prev_underflow: u64,
    prev_map_mismatch: u64,
    orphan_delta: u64,
    malformed_records: u64,
}

#[derive(Default)]
struct TierTotals {
    promote_u8_u16: u64,
    promote_u16_u32: u64,
    promote_u32_u64: u64,
    demote_u16_u8: u64,
    demote_u32_u16: u64,
    demote_u64_u32: u64,
}

#[derive(Default)]
struct ChurnTracker {
    total_ops: u64,
    alloc_flips: u64,
    free_flips: u64,
    deletes: u64,
    resurrections: u64,
    flip_1_to_2: u64,
    flip_2_to_1: u64,
}

#[derive(Default)]
struct LiveSetTracker {
    dense_live: u64,
    dense_peak: u64,
    non_one_live_start: u64,
    non_one_live: u64,
    non_one_peak: u64,
    shared_live: u64,
    shared_peak: u64,
}

#[derive(Default)]
struct TierTracker {
    totals: TierTotals,
}

#[derive(Default)]
struct Global {
    churn: ChurnTracker,
    live: LiveSetTracker,
    max_abs_value: u64,
    consistency: ConsistencyTracker,
    tiers: TierTracker,
}

impl ChurnTracker {
    fn on_transition(&mut self, transition: RefCountTransition, block: &mut BlockChurnTracker) {
        self.total_ops = self.total_ops.saturating_add(1);
        if transition.is_alloc_flip() {
            self.flip_1_to_2 = self.flip_1_to_2.saturating_add(1);
            self.alloc_flips = self.alloc_flips.saturating_add(1);
            block.alloc_flips = block.alloc_flips.saturating_add(1);
        }
        if transition.is_free_flip() {
            self.flip_2_to_1 = self.flip_2_to_1.saturating_add(1);
            self.free_flips = self.free_flips.saturating_add(1);
            block.free_flips = block.free_flips.saturating_add(1);
        }
        if transition.is_death() {
            self.deletes = self.deletes.saturating_add(1);
            block.deaths = block.deaths.saturating_add(1);
        }
        if transition.is_resurrection() {
            self.resurrections = self.resurrections.saturating_add(1);
            block.resurrections = block.resurrections.saturating_add(1);
        }
    }
}

impl LiveSetTracker {
    fn on_transition(&mut self, transition: RefCountTransition) {
        if transition.prev_val == 0 && transition.new_val > 0 {
            self.dense_live = self.dense_live.saturating_add(1);
        } else if transition.prev_val > 0 && transition.new_val == 0 {
            self.dense_live = self.dense_live.saturating_sub(1);
        }
        self.dense_peak = self.dense_peak.max(self.dense_live);

        if transition.prev_val == 1 && transition.new_val != 1 {
            self.non_one_live = self.non_one_live.saturating_add(1);
        } else if transition.prev_val != 1 && transition.new_val == 1 {
            self.non_one_live = self.non_one_live.saturating_sub(1);
        }
        self.non_one_peak = self.non_one_peak.max(self.non_one_live);

        if transition.prev_val <= 1 && transition.new_val > 1 {
            self.shared_live = self.shared_live.saturating_add(1);
        } else if transition.prev_val > 1 && transition.new_val <= 1 {
            self.shared_live = self.shared_live.saturating_sub(1);
        }
        self.shared_peak = self.shared_peak.max(self.shared_live);
    }
}

impl TierTracker {
    fn on_transition(&mut self, transition: RefCountTransition, local: &mut TierTotals) {
        let Some((from, to)) = transition.tier_change() else {
            return;
        };
        match (from, to) {
            (Tier::U8, Tier::U16) => {
                self.totals.promote_u8_u16 = self.totals.promote_u8_u16.saturating_add(1);
                local.promote_u8_u16 = local.promote_u8_u16.saturating_add(1);
            }
            (Tier::U16, Tier::U32) => {
                self.totals.promote_u16_u32 = self.totals.promote_u16_u32.saturating_add(1);
                local.promote_u16_u32 = local.promote_u16_u32.saturating_add(1);
            }
            (Tier::U32, Tier::U64) => {
                self.totals.promote_u32_u64 = self.totals.promote_u32_u64.saturating_add(1);
                local.promote_u32_u64 = local.promote_u32_u64.saturating_add(1);
            }
            (Tier::U16, Tier::U8) => {
                self.totals.demote_u16_u8 = self.totals.demote_u16_u8.saturating_add(1);
                local.demote_u16_u8 = local.demote_u16_u8.saturating_add(1);
            }
            (Tier::U32, Tier::U16) => {
                self.totals.demote_u32_u16 = self.totals.demote_u32_u16.saturating_add(1);
                local.demote_u32_u16 = local.demote_u32_u16.saturating_add(1);
            }
            (Tier::U64, Tier::U32) => {
                self.totals.demote_u64_u32 = self.totals.demote_u64_u32.saturating_add(1);
                local.demote_u64_u32 = local.demote_u64_u32.saturating_add(1);
            }
            _ => {}
        }
    }
}

impl BlockMassTracker {
    fn on_transition(&mut self, transition: RefCountTransition) {
        self.max_abs_value = self.max_abs_value.max(transition.new_val);
        self.max_abs_delta = self.max_abs_delta.max(transition.abs_delta);
        self.sum_abs = self.sum_abs.saturating_add(transition.abs_delta);
        if transition.delta > 0 {
            self.sum_pos = self.sum_pos.saturating_add(transition.abs_delta);
        } else if transition.delta < 0 {
            self.sum_neg = self.sum_neg.saturating_add(transition.abs_delta);
        }
    }
}

#[derive(Clone, Copy, Default)]
struct WinChange {
    net_delta: i64,
    saw_pos: bool,
    saw_neg: bool,
}

#[derive(Default)]
struct BlockMassTracker {
    sum_pos: u64,
    sum_neg: u64,
    sum_abs: u64,
    max_abs_value: u64,
    max_abs_delta: u64,
}

#[derive(Default)]
struct BlockChurnTracker {
    alloc_flips: u64,
    free_flips: u64,
    deaths: u64,
    resurrections: u64,
}

#[derive(Default)]
struct BlockAgg {
    seqno: u32,
    mass: BlockMassTracker,
    churn: BlockChurnTracker,
    tiers: TierTotals,
    changes: FastHashMap<Hash, WinChange>,
}

struct BlockDone {
    seqno: u32,
    u_touched: u64,
    sum_abs: u64,
    sum_pos: u64,
    sum_neg: u64,
    max_abs_value: u64,
    max_abs_delta: u64,
    alloc_flips: u64,
    free_flips: u64,
    deaths: u64,
    resurrections: u64,
    promote_u8_u16: u64,
    promote_u16_u32: u64,
    promote_u32_u64: u64,
    demote_u16_u8: u64,
    demote_u32_u16: u64,
    demote_u64_u32: u64,
    non_one_live_end: u64,
    shared_live_end: u64,
    changes: FastHashMap<Hash, WinChange>,
}

#[derive(Clone, Copy, Default)]
struct WinState {
    net_delta: i64,
    pos_blocks: u32,
    neg_blocks: u32,
}

struct WinBlock {
    sum_abs: u64,
    changes: FastHashMap<Hash, WinChange>,
}

struct WindowRow {
    u_win: u64,
    nz_win: u64,
    z_win: u64,
    osc_win: u64,
    pure_zero_osc_win: u64,
    total_churn: u64,
    net_change: u64,
    wasted_ops: u64,
}

struct WindowAgg {
    size: usize,
    blocks: VecDeque<WinBlock>,
    by_hash: FastHashMap<Hash, WinState>,
    nz_count: u64,
    osc_count: u64,
    pure_zero_count: u64,
    net_change_sum: u64,
    total_churn_sum: u64,
}

impl WindowAgg {
    fn new(size: usize) -> Self {
        Self {
            size,
            blocks: VecDeque::new(),
            by_hash: FastHashMap::default(),
            nz_count: 0,
            osc_count: 0,
            pure_zero_count: 0,
            net_change_sum: 0,
            total_churn_sum: 0,
        }
    }

    fn push_block(&mut self, block: BlockDone) -> Option<WindowRow> {
        let win_block = WinBlock {
            sum_abs: block.sum_abs,
            changes: block.changes,
        };
        self.apply_block(&win_block, true);
        self.total_churn_sum = self.total_churn_sum.saturating_add(win_block.sum_abs);
        self.blocks.push_back(win_block);

        if self.blocks.len() > self.size
            && let Some(old) = self.blocks.pop_front()
        {
            self.apply_block(&old, false);
            self.total_churn_sum = self.total_churn_sum.saturating_sub(old.sum_abs);
        }

        if self.blocks.len() < self.size {
            return None;
        }

        Some(WindowRow {
            u_win: self.by_hash.len() as u64,
            nz_win: self.nz_count,
            z_win: self.by_hash.len() as u64 - self.nz_count,
            osc_win: self.osc_count,
            pure_zero_osc_win: self.pure_zero_count,
            total_churn: self.total_churn_sum,
            net_change: self.net_change_sum,
            wasted_ops: self.total_churn_sum.saturating_sub(self.net_change_sum),
        })
    }

    fn apply_block(&mut self, block: &WinBlock, add: bool) {
        for (hash, change) in &block.changes {
            let old = self.by_hash.get(hash).copied().unwrap_or_default();
            let old_nz = old.net_delta != 0;
            let old_osc = old.pos_blocks > 0 && old.neg_blocks > 0;
            let old_pure = old_osc && old.net_delta == 0;
            let old_abs = old.net_delta.unsigned_abs();

            let mut next = old;
            if add {
                next.net_delta = next.net_delta.saturating_add(change.net_delta);
                if change.saw_pos {
                    next.pos_blocks = next.pos_blocks.saturating_add(1);
                }
                if change.saw_neg {
                    next.neg_blocks = next.neg_blocks.saturating_add(1);
                }
            } else {
                next.net_delta = next.net_delta.saturating_sub(change.net_delta);
                if change.saw_pos {
                    next.pos_blocks = next.pos_blocks.saturating_sub(1);
                }
                if change.saw_neg {
                    next.neg_blocks = next.neg_blocks.saturating_sub(1);
                }
            }

            let new_nz = next.net_delta != 0;
            let new_osc = next.pos_blocks > 0 && next.neg_blocks > 0;
            let new_pure = new_osc && next.net_delta == 0;
            let new_abs = next.net_delta.unsigned_abs();

            if old_nz && !new_nz {
                self.nz_count = self.nz_count.saturating_sub(1);
            } else if !old_nz && new_nz {
                self.nz_count = self.nz_count.saturating_add(1);
            }
            if old_osc && !new_osc {
                self.osc_count = self.osc_count.saturating_sub(1);
            } else if !old_osc && new_osc {
                self.osc_count = self.osc_count.saturating_add(1);
            }
            if old_pure && !new_pure {
                self.pure_zero_count = self.pure_zero_count.saturating_sub(1);
            } else if !old_pure && new_pure {
                self.pure_zero_count = self.pure_zero_count.saturating_add(1);
            }

            self.net_change_sum = self
                .net_change_sum
                .saturating_sub(old_abs)
                .saturating_add(new_abs);

            if next.net_delta == 0 && next.pos_blocks == 0 && next.neg_blocks == 0 {
                self.by_hash.remove(hash);
            } else {
                self.by_hash.insert(*hash, next);
            }
        }
    }
}

#[derive(Default)]
struct Hot {
    max_abs_value: u64,
    touches: u64,
    total_delta: i128,
    total_abs_delta: u128,
    first_seen: u32,
    last_seen: u32,
    last_touched_in: Option<u32>,
    flips: u64,
}

#[derive(Default)]
struct HotKeyTracker {
    states: FastHashMap<Hash, Hot>,
}

#[derive(Default)]
struct LifetimeTracker {
    births: FastHashMap<Hash, u32>,
    ttl: Vec<u64>,
    unknown_birth_deaths: u64,
}

struct Run {
    cli: Cli,
    parsed: ParseCounters,
    blocks: Vec<BlockDone>,
    windows: Vec<WindowRow>,
    global: Global,
    hot: HotKeyTracker,
    lifetimes: LifetimeTracker,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    if cli.from > cli.to {
        bail!("--from must be <= --to");
    }
    if cli.window_size == 0 {
        bail!("--window-size must be > 0");
    }

    let mut run = Run {
        cli,
        parsed: ParseCounters::default(),
        blocks: Vec::new(),
        windows: Vec::new(),
        global: Global::default(),
        hot: HotKeyTracker::default(),
        lifetimes: LifetimeTracker::default(),
    };

    run.global.live.non_one_live_start = run.global.live.non_one_live;

    analyze(&mut run)?;
    print_report(&run);
    Ok(())
}

fn analyze(run: &mut Run) -> Result<()> {
    let parent = run
        .cli
        .file_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    let dir = Dir::new_readonly(parent);
    let file_name = run
        .cli
        .file_path
        .file_name()
        .context("file path has no file name")?;
    let file = dir
        .file(file_name)
        .read(true)
        .open()
        .with_context(|| format!("failed to open {}", run.cli.file_path.display()))?;
    let reader = BufReader::with_capacity(64 * 1024, file);
    let records = CopsLogReader::new(reader);

    let mut refcounts: FastHashMap<Hash, u64> = FastHashMap::default();
    let mut current_block: Option<BlockCtx> = None;
    let mut current_master_seqno: Option<u32> = None;
    let mut current_agg: Option<BlockAgg> = None;
    let mut win = WindowAgg::new(run.cli.window_size as usize);

    for record in records {
        match record {
            Ok(Rec::Block(ctx)) => {
                run.parsed.blocks = run.parsed.blocks.saturating_add(1);
                if let Some(done) = finalize_block(current_agg.take(), &run.global) {
                    if let Some(row) = win.push_block(done.clone_for_window()) {
                        run.windows.push(row);
                    }
                    run.blocks.push(done);
                }
                current_block = Some(ctx);
                if is_master_block(ctx) {
                    current_master_seqno = Some(ctx.seqno);
                }
                current_agg =
                    if is_master_seq_selected(current_master_seqno, run.cli.from, run.cli.to) {
                        Some(BlockAgg {
                            seqno: ctx.seqno,
                            ..BlockAgg::default()
                        })
                    } else {
                        None
                    };
            }
            Ok(Rec::Delta(op)) => {
                run.parsed.deltas = run.parsed.deltas.saturating_add(1);
                let Some(_ctx) = current_block else {
                    run.global.consistency.orphan_delta =
                        run.global.consistency.orphan_delta.saturating_add(1);
                    continue;
                };
                if !is_master_seq_selected(current_master_seqno, run.cli.from, run.cli.to) {
                    continue;
                }
                let Some(agg) = current_agg.as_mut() else {
                    run.global.consistency.orphan_delta =
                        run.global.consistency.orphan_delta.saturating_add(1);
                    continue;
                };
                process_delta(agg, &op, &mut refcounts, run);
            }
            Err(error) => {
                run.global.consistency.malformed_records =
                    run.global.consistency.malformed_records.saturating_add(1);
                let _ = error;
                break;
            }
        }
    }

    if let Some(done) = finalize_block(current_agg.take(), &run.global) {
        if let Some(row) = win.push_block(done.clone_for_window()) {
            run.windows.push(row);
        }
        run.blocks.push(done);
    }

    Ok(())
}

fn process_delta(
    agg: &mut BlockAgg,
    op: &Delta,
    refcounts: &mut FastHashMap<Hash, u64>,
    run: &mut Run,
) {
    let transition = match RefCountTransition::from_delta(agg.seqno, op) {
        Ok(transition) => transition,
        Err(TransitionError::PrevUnderflow) => {
            run.global.consistency.invalid_prev_underflow = run
                .global
                .consistency
                .invalid_prev_underflow
                .saturating_add(1);
            return;
        }
    };

    let old = refcounts.get(&transition.hash).copied().unwrap_or(0);
    if old != transition.prev_val {
        run.global.consistency.prev_map_mismatch =
            run.global.consistency.prev_map_mismatch.saturating_add(1);
    }

    run.global.churn.on_transition(transition, &mut agg.churn);
    run.global.live.on_transition(transition);
    run.global.tiers.on_transition(transition, &mut agg.tiers);
    agg.mass.on_transition(transition);
    run.global.max_abs_value = run.global.max_abs_value.max(transition.new_val);

    if transition.new_val == 0 {
        refcounts.remove(&transition.hash);
    } else {
        refcounts.insert(transition.hash, transition.new_val);
    }

    run.lifetimes.on_transition(transition);
    run.hot.on_transition(transition);

    if transition.delta != 0 {
        let entry = agg.changes.entry(transition.hash).or_default();
        entry.net_delta = entry.net_delta.saturating_add(transition.delta);
        if transition.delta > 0 {
            entry.saw_pos = true;
        } else {
            entry.saw_neg = true;
        }
    }
}

impl HotKeyTracker {
    fn on_transition(&mut self, transition: RefCountTransition) {
        let state = self.states.entry(transition.hash).or_insert_with(|| Hot {
            first_seen: transition.seqno,
            ..Hot::default()
        });
        state.max_abs_value = state.max_abs_value.max(transition.new_val);
        if state.last_touched_in != Some(transition.seqno) {
            state.touches = state.touches.saturating_add(1);
            state.last_touched_in = Some(transition.seqno);
        }
        state.total_delta = state
            .total_delta
            .saturating_add(i128::from(transition.delta));
        state.total_abs_delta = state
            .total_abs_delta
            .saturating_add(u128::from(transition.abs_delta));
        state.last_seen = transition.seqno;
        if transition.is_alloc_flip() || transition.is_free_flip() {
            state.flips = state.flips.saturating_add(1);
        }
    }
}

impl LifetimeTracker {
    fn on_transition(&mut self, transition: RefCountTransition) {
        if transition.prev_val == 0 && transition.new_val > 0 {
            self.births.insert(transition.hash, transition.seqno);
        }
        if transition.new_val == 0 {
            if let Some(start) = self.births.remove(&transition.hash) {
                self.ttl
                    .push(u64::from(transition.seqno.saturating_sub(start)));
            } else {
                self.unknown_birth_deaths = self.unknown_birth_deaths.saturating_add(1);
            }
        }
    }
}

fn finalize_block(block: Option<BlockAgg>, global: &Global) -> Option<BlockDone> {
    block.map(|block| BlockDone {
        seqno: block.seqno,
        u_touched: block.changes.len() as u64,
        sum_abs: block.mass.sum_abs,
        sum_pos: block.mass.sum_pos,
        sum_neg: block.mass.sum_neg,
        max_abs_value: block.mass.max_abs_value,
        max_abs_delta: block.mass.max_abs_delta,
        alloc_flips: block.churn.alloc_flips,
        free_flips: block.churn.free_flips,
        deaths: block.churn.deaths,
        resurrections: block.churn.resurrections,
        promote_u8_u16: block.tiers.promote_u8_u16,
        promote_u16_u32: block.tiers.promote_u16_u32,
        promote_u32_u64: block.tiers.promote_u32_u64,
        demote_u16_u8: block.tiers.demote_u16_u8,
        demote_u32_u16: block.tiers.demote_u32_u16,
        demote_u64_u32: block.tiers.demote_u64_u32,
        non_one_live_end: global.live.non_one_live,
        shared_live_end: global.live.shared_live,
        changes: block.changes,
    })
}

impl BlockDone {
    fn clone_for_window(&self) -> BlockDone {
        BlockDone {
            seqno: self.seqno,
            u_touched: self.u_touched,
            sum_abs: self.sum_abs,
            sum_pos: self.sum_pos,
            sum_neg: self.sum_neg,
            max_abs_value: self.max_abs_value,
            max_abs_delta: self.max_abs_delta,
            alloc_flips: self.alloc_flips,
            free_flips: self.free_flips,
            deaths: self.deaths,
            resurrections: self.resurrections,
            promote_u8_u16: self.promote_u8_u16,
            promote_u16_u32: self.promote_u16_u32,
            promote_u32_u64: self.promote_u32_u64,
            demote_u16_u8: self.demote_u16_u8,
            demote_u32_u16: self.demote_u32_u16,
            demote_u64_u32: self.demote_u64_u32,
            non_one_live_end: self.non_one_live_end,
            shared_live_end: self.shared_live_end,
            changes: self.changes.clone(),
        }
    }
}

fn is_master_seq_selected(current_master_seqno: Option<u32>, from: u32, to: u32) -> bool {
    current_master_seqno.is_some_and(|seqno| seqno >= from && seqno <= to)
}

fn is_master_block(ctx: BlockCtx) -> bool {
    ctx.workchain == MASTER_WORKCHAIN && ctx.shard_prefix == MASTER_SHARD_PREFIX
}

struct CopsLogReader<R> {
    reader: R,
}

impl<R> CopsLogReader<R> {
    fn new(reader: R) -> Self {
        Self { reader }
    }
}

fn read_exact_or_eof<R: Read>(reader: &mut R, target: &mut [u8]) -> io::Result<bool> {
    let mut read = 0;
    while read < target.len() {
        match reader.read(&mut target[read..]) {
            Ok(0) if read == 0 => return Ok(false),
            Ok(0) => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short record")),
            Ok(size) => read += size,
            Err(error) => return Err(error),
        }
    }
    Ok(true)
}

impl<R: Read> Iterator for CopsLogReader<R> {
    type Item = io::Result<Rec>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut header = [0u8; 8];
        match read_exact_or_eof(&mut self.reader, &mut header) {
            Ok(false) => return None,
            Ok(true) => {}
            Err(error) => return Some(Err(error)),
        }

        if header[..4] != COPS_MAGIC {
            return Some(Err(io::Error::new(io::ErrorKind::InvalidData, "bad magic")));
        }

        let version = u16::from_le_bytes([header[4], header[5]]);
        if version != COPS_VERSION {
            return Some(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad version",
            )));
        }

        let kind = u16::from_le_bytes([header[6], header[7]]);
        let record = match kind {
            COPS_KIND_BLOCK => {
                let mut tail = [0u8; 20];
                match read_exact_or_eof(&mut self.reader, &mut tail) {
                    Ok(true) => {}
                    Ok(false) => {
                        return Some(Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "short block record",
                        )));
                    }
                    Err(error) => return Some(Err(error)),
                }
                let workchain = i32::from_le_bytes([tail[0], tail[1], tail[2], tail[3]]);
                let shard_prefix = u64::from_le_bytes([
                    tail[4], tail[5], tail[6], tail[7], tail[8], tail[9], tail[10], tail[11],
                ]);
                let seqno = u32::from_le_bytes([tail[12], tail[13], tail[14], tail[15]]);
                Rec::Block(BlockCtx {
                    workchain,
                    shard_prefix,
                    seqno,
                })
            }
            COPS_KIND_DELTA => {
                let mut tail = [0u8; 48];
                match read_exact_or_eof(&mut self.reader, &mut tail) {
                    Ok(true) => {}
                    Ok(false) => {
                        return Some(Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "short delta record",
                        )));
                    }
                    Err(error) => return Some(Err(error)),
                }
                let delta = i32::from_le_bytes([tail[0], tail[1], tail[2], tail[3]]);
                let abs_val = u64::from_le_bytes([
                    tail[8], tail[9], tail[10], tail[11], tail[12], tail[13], tail[14], tail[15],
                ]);
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&tail[16..48]);
                Rec::Delta(Delta {
                    delta,
                    abs_val,
                    hash,
                })
            }
            _ => {
                return Some(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unknown record kind",
                )));
            }
        };

        Some(Ok(record))
    }
}

fn print_report(run: &Run) {
    let mut out = String::new();
    let from_block = run.cli.from;
    let to_block = run.cli.to;
    let requested_blocks = u64::from(to_block.saturating_sub(from_block)) + 1;
    let analyzed_blocks = run.blocks.len() as u64;
    let windows = run.windows.len() as u64;

    let _ = writeln!(
        out,
        "Processing complete. Analyzed {analyzed_blocks} blocks."
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "--- Global Sparse Storage Metrics ---");
    let _ = writeln!(
        out,
        "1. Allocation Churn (1->2 flips): {}",
        run.global.churn.flip_1_to_2
    );
    let _ = writeln!(
        out,
        "2. Deallocation Churn (2->1 flips): {}",
        run.global.churn.flip_2_to_1
    );
    let flip_ratio = if run.global.churn.total_ops == 0 {
        0.0
    } else {
        (run.global.churn.flip_1_to_2 + run.global.churn.flip_2_to_1) as f64
            / run.global.churn.total_ops as f64
    };
    let _ = writeln!(out, "3. Flip Ratio (Flips / Total Ops): {flip_ratio:.6}");
    let _ = writeln!(
        out,
        "   (> 0.5 indicates sparse storage will thrash metadata)"
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "--- Peak RAM Requirements ---");
    let _ = writeln!(
        out,
        "1. Max Concurrent RC>1 (Sparse Map): {} items",
        run.global.live.shared_peak
    );
    let _ = writeln!(
        out,
        "2. Max Concurrent RC>0 (Dense Map): {} items",
        run.global.live.dense_peak
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "--- Windowed Analysis (Snapshot Efficiency) ---");
    let _ = writeln!(out, "Window Size: {} blocks", run.cli.window_size);
    let _ = writeln!(
        out,
        "1. Avg Unique Hashes per Window: {:.2}",
        avg_u64(run.windows.iter().map(|row| row.u_win))
    );
    let avg_wasted = avg_u64(run.windows.iter().map(|row| row.wasted_ops));
    let avg_total = avg_u64(run.windows.iter().map(|row| row.total_churn));
    let wasted_pct = if avg_total == 0.0 {
        0.0
    } else {
        (avg_wasted / avg_total) * 100.0
    };
    let _ = writeln!(
        out,
        "2. Avg Wasted Ops per Window: {avg_wasted:.2} ({wasted_pct:.2}% of total)"
    );
    let _ = writeln!(
        out,
        "   (High % means snapshots save significant IO vs logs)"
    );
    let _ = writeln!(
        out,
        "3. Pure Oscillators (Net Zero in Window): {:.2}",
        avg_u64(run.windows.iter().map(|row| row.pure_zero_osc_win))
    );
    let _ = writeln!(out);

    let _ = writeln!(out, "--- Run Metadata ---");
    let _ = writeln!(out, "from_block: {from_block}");
    let _ = writeln!(out, "to_block: {to_block}");
    let _ = writeln!(out, "blocks_requested: {requested_blocks}");
    let _ = writeln!(out, "blocks_analyzed: {analyzed_blocks}");
    let _ = writeln!(out, "snapshot_interval: {}", run.cli.window_size);
    let _ = writeln!(out, "roots_kept: {}", run.cli.roots_kept);
    let _ = writeln!(out, "windows_computed: {windows}");
    let _ = writeln!(out);

    let _ = writeln!(out, "--- Parser Counters ---");
    let _ = writeln!(out, "parsed_block_records: {}", run.parsed.blocks);
    let _ = writeln!(out, "parsed_delta_records: {}", run.parsed.deltas);
    let _ = writeln!(
        out,
        "malformed_records: {}",
        run.global.consistency.malformed_records
    );
    let _ = writeln!(
        out,
        "orphan_delta_records: {}",
        run.global.consistency.orphan_delta
    );
    let _ = writeln!(
        out,
        "prev_underflow_errors: {}",
        run.global.consistency.invalid_prev_underflow
    );
    let _ = writeln!(
        out,
        "prev_map_mismatch: {}",
        run.global.consistency.prev_map_mismatch
    );
    let _ = writeln!(out);

    print_block_distributions(&mut out, &run.blocks);
    print_flip_distributions(&mut out, &run.blocks);
    print_tier_stats(&mut out, &run.blocks, &run.global.tiers.totals);
    print_live_stats(&mut out, &run.blocks, &run.global);
    print_window_distributions(&mut out, &run.windows);
    print_hot(&mut out, &run.hot.states, run.cli.top_k);
    print_lifetimes(&mut out, &run.lifetimes);
    print_six(&mut out, run);

    let _ = io::Write::write_all(&mut io::stdout().lock(), out.as_bytes());
}

fn print_block_distributions(out: &mut String, blocks: &[BlockDone]) {
    let _ = writeln!(out, "--- Per-Block Working Set & Delta Mass ---");
    print_dist_u64(out, "U(block)", blocks.iter().map(|b| b.u_touched));
    print_dist_u64(out, "sum_abs(block)", blocks.iter().map(|b| b.sum_abs));
    print_dist_u64(out, "sum_pos(block)", blocks.iter().map(|b| b.sum_pos));
    print_dist_u64(out, "sum_neg(block)", blocks.iter().map(|b| b.sum_neg));
    print_dist_u64(
        out,
        "max_abs_value(block)",
        blocks.iter().map(|b| b.max_abs_value),
    );
    print_dist_u64(
        out,
        "max_abs_delta(block)",
        blocks.iter().map(|b| b.max_abs_delta),
    );
    let _ = writeln!(out);
}

fn print_flip_distributions(out: &mut String, blocks: &[BlockDone]) {
    let _ = writeln!(out, "--- Sentinel Flip Pressure ---");
    print_dist_u64(
        out,
        "alloc_flips(block)",
        blocks.iter().map(|b| b.alloc_flips),
    );
    print_dist_u64(
        out,
        "free_flips(block)",
        blocks.iter().map(|b| b.free_flips),
    );
    print_dist_u64(out, "deaths(block)", blocks.iter().map(|b| b.deaths));
    print_dist_u64(
        out,
        "resurrections(block)",
        blocks.iter().map(|b| b.resurrections),
    );
    let _ = writeln!(out);
}

fn print_tier_stats(out: &mut String, blocks: &[BlockDone], totals: &TierTotals) {
    let _ = writeln!(out, "--- Tier Promotions & Demotions ---");
    let _ = writeln!(out, "totals.promote_u8_to_u16: {}", totals.promote_u8_u16);
    let _ = writeln!(out, "totals.promote_u16_to_u32: {}", totals.promote_u16_u32);
    let _ = writeln!(out, "totals.promote_u32_to_u64: {}", totals.promote_u32_u64);
    let _ = writeln!(out, "totals.demote_u16_to_u8: {}", totals.demote_u16_u8);
    let _ = writeln!(out, "totals.demote_u32_to_u16: {}", totals.demote_u32_u16);
    let _ = writeln!(out, "totals.demote_u64_to_u32: {}", totals.demote_u64_u32);
    print_dist_u64(
        out,
        "promote_u8_to_u16(block)",
        blocks.iter().map(|b| b.promote_u8_u16),
    );
    print_dist_u64(
        out,
        "promote_u16_to_u32(block)",
        blocks.iter().map(|b| b.promote_u16_u32),
    );
    print_dist_u64(
        out,
        "promote_u32_to_u64(block)",
        blocks.iter().map(|b| b.promote_u32_u64),
    );
    let _ = writeln!(out);
}

fn print_live_stats(out: &mut String, blocks: &[BlockDone], global: &Global) {
    let _ = writeln!(out, "--- Non-One Live Set ---");
    let _ = writeln!(
        out,
        "start_non_one_live: {}",
        global.live.non_one_live_start
    );
    let _ = writeln!(out, "end_non_one_live: {}", global.live.non_one_live);
    let _ = writeln!(out, "peak_non_one_live: {}", global.live.non_one_peak);
    let vals: Vec<u64> = blocks.iter().map(|b| b.non_one_live_end).collect();
    if let Some(dist) = summarize(&vals) {
        let _ = writeln!(
            out,
            "non_one_live_over_blocks p50/p90/p99: {}/{}/{}",
            dist.p50, dist.p90, dist.p99
        );
    }
    let _ = writeln!(out, "peak_shared_live: {}", global.live.shared_peak);
    let _ = writeln!(out);
}

fn print_window_distributions(out: &mut String, windows: &[WindowRow]) {
    let _ = writeln!(out, "--- Rolling Window (W=window-size) ---");
    print_dist_window(out, "U_win", windows.iter().map(|w| w.u_win));
    print_dist_window(out, "NZ_win", windows.iter().map(|w| w.nz_win));
    print_dist_window(out, "Z_win", windows.iter().map(|w| w.z_win));
    print_dist_window(out, "net_change_win", windows.iter().map(|w| w.net_change));
    print_dist_window(out, "osc_win", windows.iter().map(|w| w.osc_win));
    print_dist_window(
        out,
        "pure_zero_osc_win",
        windows.iter().map(|w| w.pure_zero_osc_win),
    );
    print_dist_window(out, "wasted_ops_win", windows.iter().map(|w| w.wasted_ops));
    let _ = writeln!(out);
}

fn print_hot(out: &mut String, hot: &FastHashMap<Hash, Hot>, top_k: usize) {
    let _ = writeln!(out, "--- Hot Keys ---");
    print_hot_rank(out, "top_by_max_abs_value", hot, top_k, |item| {
        item.1.max_abs_value as u128
    });
    print_hot_rank(out, "top_by_touch_count", hot, top_k, |item| {
        item.1.touches as u128
    });
    print_hot_rank(out, "top_by_sum_abs_delta", hot, top_k, |item| {
        item.1.total_abs_delta
    });
    let _ = writeln!(out);
}

fn print_lifetimes(out: &mut String, life: &LifetimeTracker) {
    let _ = writeln!(out, "--- Lifetime Stats ---");
    let _ = writeln!(out, "unknown_birth_deaths: {}", life.unknown_birth_deaths);
    if let Some(dist) = summarize(&life.ttl) {
        let _ = writeln!(
            out,
            "ttl_blocks p50/p90/p99: {}/{}/{}",
            dist.p50, dist.p90, dist.p99
        );
        let _ = writeln!(out, "ttl_blocks min/max: {}/{}", dist.min, dist.max);
    } else {
        let _ = writeln!(out, "ttl_blocks: no completed lifetimes in selected range");
    }
    let _ = writeln!(out);
}

fn print_six(out: &mut String, run: &Run) {
    let _ = writeln!(out, "--- Six Number Decision Set ---");
    let max_u_block = run.blocks.iter().map(|b| b.u_touched).max().unwrap_or(0);
    let max_alloc = run.blocks.iter().map(|b| b.alloc_flips).max().unwrap_or(0);
    let max_free = run.blocks.iter().map(|b| b.free_flips).max().unwrap_or(0);
    let max_u_win = run.windows.iter().map(|w| w.u_win).max().unwrap_or(0);
    let max_nz_win = run.windows.iter().map(|w| w.nz_win).max().unwrap_or(0);
    let _ = writeln!(out, "1) max U(block): {max_u_block}");
    let _ = writeln!(
        out,
        "2) max alloc_flips / free_flips: {max_alloc} / {max_free}"
    );
    let _ = writeln!(
        out,
        "3) peak non_one_live: {}",
        run.global.live.non_one_peak
    );
    let _ = writeln!(out, "4) max U_win / NZ_win: {max_u_win} / {max_nz_win}");
    let _ = writeln!(out, "5) max abs_value: {}", run.global.max_abs_value);
    let _ = writeln!(
        out,
        "6) total resurrections: {}",
        run.global.churn.resurrections
    );
}

fn print_hot_rank<F>(
    out: &mut String,
    title: &str,
    hot: &FastHashMap<Hash, Hot>,
    top_k: usize,
    mut score: F,
) where
    F: FnMut((&Hash, &Hot)) -> u128,
{
    let mut rows: Vec<(&Hash, &Hot)> = hot.iter().collect();
    rows.sort_by_key(|item| std::cmp::Reverse(score(*item)));
    let _ = writeln!(out, "{title}:");
    for (index, (hash, state)) in rows.into_iter().take(top_k).enumerate() {
        let _ = writeln!(
            out,
            "{}. {} max_abs={} touches={} total_delta={} total_abs_delta={} first={} last={} flips={}",
            index + 1,
            to_hex(hash),
            state.max_abs_value,
            state.touches,
            state.total_delta,
            state.total_abs_delta,
            state.first_seen,
            state.last_seen,
            state.flips
        );
    }
}

fn print_dist_u64(out: &mut String, label: &str, values: impl Iterator<Item = u64>) {
    let data: Vec<u64> = values.collect();
    if let Some(dist) = summarize(&data) {
        let _ = writeln!(
            out,
            "{label}: min={} mean={:.2} p50={} p90={} p99={} max={}",
            dist.min, dist.mean, dist.p50, dist.p90, dist.p99, dist.max
        );
    } else {
        let _ = writeln!(out, "{label}: no data");
    }
}

fn print_dist_window(out: &mut String, label: &str, values: impl Iterator<Item = u64>) {
    let data: Vec<u64> = values.collect();
    if let Some(dist) = summarize(&data) {
        let _ = writeln!(
            out,
            "{label}: min={} mean={:.2} p50={} p90={} p99={} max={}",
            dist.min, dist.mean, dist.p50, dist.p90, dist.p99, dist.max
        );
    } else {
        let _ = writeln!(out, "{label}: no data");
    }
}

fn avg_u64(values: impl Iterator<Item = u64>) -> f64 {
    let mut count = 0u64;
    let mut total = 0f64;
    for value in values {
        count = count.saturating_add(1);
        total += value as f64;
    }
    if count == 0 {
        0.0
    } else {
        total / count as f64
    }
}

#[derive(Clone, Copy)]
struct Dist {
    min: u64,
    mean: f64,
    p50: u64,
    p90: u64,
    p99: u64,
    max: u64,
}

fn summarize(values: &[u64]) -> Option<Dist> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let min = sorted[0];
    let max = *sorted.last().unwrap_or(&min);
    let sum: u128 = sorted.iter().map(|value| u128::from(*value)).sum();
    let mean = sum as f64 / sorted.len() as f64;
    Some(Dist {
        min,
        mean,
        p50: percentile(&sorted, 50),
        p90: percentile(&sorted, 90),
        p99: percentile(&sorted, 99),
        max,
    })
}

fn percentile(sorted: &[u64], p: u32) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let len = sorted.len();
    let idx = ((len - 1) * p as usize) / 100;
    sorted[idx]
}

fn to_hex(hash: &Hash) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for byte in hash {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
