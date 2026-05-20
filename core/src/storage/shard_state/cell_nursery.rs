use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use parking_lot::{RwLock, RwLockReadGuard};
use rayon::ThreadPool;
use rayon::prelude::*;
use tycho_types::cell::HashBytes;

use super::counters::Idx;
use super::util::{CellHashMap, HashBytesKey};

pub(super) const PROMOTION_DELAY_ROUNDS: u32 = 100;

pub(super) struct CellNursery {
    // We use several hashmaps to feed vector into them in parallel. Single rwlock is fine cause we have only 1 writer and load is round-like.
    // Dashmap shown a few percent worse performance and much worse ergonomics for per shard writes.
    entries: RwLock<Box<[CellHashMap<NurseryEntry>]>>,
    entry_stripes: usize,
    by_round: Mutex<RoundBuckets>,
    worker_pool: Arc<ThreadPool>,
    live_cell_bytes: AtomicU64,
}

impl CellNursery {
    pub fn new(entry_stripes: NonZeroUsize, worker_pool: Arc<ThreadPool>) -> Self {
        Self {
            entries: RwLock::new(
                (0..entry_stripes.get())
                    .map(|_| CellHashMap::default())
                    .collect(),
            ),
            entry_stripes: entry_stripes.get(),
            by_round: Default::default(),
            worker_pool,
            live_cell_bytes: Default::default(),
        }
    }

    pub fn get(&self, hash: &HashBytes) -> Option<Bytes> {
        self.read().get(hash)
    }

    pub fn read(&self) -> NurseryReadGuard<'_> {
        NurseryReadGuard {
            entries: self.entries.read(),
        }
    }

    pub fn apply_commit(&self, commit: NurseryDelta) {
        if commit.is_empty() {
            return;
        }

        let mut plan = PublishPlan::build(self, commit);
        let stats = self.publish_entries(&mut plan);
        self.publish_round_updates(plan, stats);
        stats.record();
    }

    pub fn drain_promotable(&self, round: u32, mut f: impl FnMut(HashBytes, Idx, &[u8])) {
        let Some(max_born_round) = round.checked_sub(PROMOTION_DELAY_ROUNDS) else {
            return;
        };

        let mut plan = self.drain_promotion_candidates(max_born_round);
        for cell in &plan.promoted {
            f(cell.hash, cell.idx, cell.data.as_ref());
        }
        self.recycle_promotion_plan(&mut plan);
    }

    pub fn snapshot_entries(&self) -> Vec<NurseryEntryRecord> {
        self.entries
            .read()
            .iter()
            .flatten()
            .map(|(hash, entry)| NurseryEntryRecord {
                hash: HashBytes(hash.0),
                idx: entry.idx,
                born_round: entry.born_round,
                data: entry.data.clone(),
            })
            .collect::<Vec<_>>()
    }

    pub fn record_metrics(&self) {
        let entries = self.entries.read();
        let entries_len = entries.iter().map(CellHashMap::len).sum::<usize>();
        let entries_capacity = entries.iter().map(CellHashMap::capacity).sum::<usize>();
        drop(entries);

        metrics::gauge!("tycho_storage_cell_nursery_entries").set(entries_len as f64);
        metrics::gauge!("tycho_storage_cell_nursery_entry_slots_bytes")
            .set((entries_capacity * size_of::<(HashBytesKey, NurseryEntry)>()) as f64);

        let by_round = self.by_round.lock().unwrap();
        let round_active_bytes = by_round
            .by_round
            .values()
            .map(|hashes| hashes.capacity() * size_of::<HashBytes>())
            .sum::<usize>();
        let round_cached_bytes = by_round
            .buffers
            .iter()
            .map(|hashes| hashes.capacity() * size_of::<HashBytes>())
            .sum::<usize>();
        metrics::gauge!("tycho_storage_cell_nursery_round_buffer_bytes")
            .set((round_active_bytes + round_cached_bytes) as f64);
        drop(by_round);

        metrics::gauge!("tycho_storage_cell_nursery_live_cell_bytes")
            .set(self.live_cell_bytes.load(Ordering::Relaxed) as f64);
    }
}

pub(super) struct NurseryReadGuard<'a> {
    entries: RwLockReadGuard<'a, Box<[CellHashMap<NurseryEntry>]>>,
}

impl NurseryReadGuard<'_> {
    pub fn get(&self, hash: &HashBytes) -> Option<Bytes> {
        self.entry(hash).map(|entry| entry.data.clone())
    }

    pub fn get_idx(&self, hash: &HashBytes) -> Option<Idx> {
        self.entry(hash).map(|entry| entry.idx)
    }

    pub fn get_data(&self, hash: &HashBytes) -> Option<(Idx, &[u8])> {
        self.entry(hash)
            .map(|entry| (entry.idx, entry.data.as_ref()))
    }

    fn entry(&self, hash: &HashBytes) -> Option<&NurseryEntry> {
        let key = hash_key(hash);
        let stripe = entry_stripe(key, self.entries.len());
        self.entries[stripe].get(key)
    }
}

#[derive(Clone, Debug)]
pub(super) struct NurseryEntryRecord {
    pub hash: HashBytes,
    pub idx: Idx,
    pub born_round: u32,
    pub data: Bytes,
}

#[derive(Debug, Default)]
pub(super) struct NurseryDelta {
    pub inserts: Vec<NurseryEntryRecord>,
    pub removes: Vec<HashBytes>,
}

impl NurseryDelta {
    pub fn is_empty(&self) -> bool {
        self.inserts.is_empty() && self.removes.is_empty()
    }
}

#[derive(Clone, Copy, Default)]
pub(super) struct NurseryCommitStats {
    pub admitted_count: u64,
    pub admitted_bytes: u64,
    pub removed_bytes: u64,
}

impl NurseryCommitStats {
    pub fn record(self) {
        if self.admitted_count > 0 {
            metrics::counter!("tycho_storage_cell_nursery_admitted_count")
                .increment(self.admitted_count);
            metrics::counter!("tycho_storage_cell_nursery_admitted_bytes")
                .increment(self.admitted_bytes);
        }
    }
}

impl CellNursery {
    fn drain_promotion_candidates(&self, max_born_round: u32) -> PromotionPlan {
        let mut by_round = self.by_round.lock().unwrap();
        let mut to_reserve = 0;
        let mut promoted = std::mem::take(&mut by_round.promoted);
        let popped = by_round
            .by_round
            .extract_if(..=max_born_round, |_, hashes| {
                to_reserve += hashes.len();
                true
            })
            .collect::<Vec<_>>();
        drop(by_round);

        promoted.reserve(to_reserve);
        let entries = self.entries.read();
        for (born_round, hashes) in &popped {
            for hash in hashes {
                let key = hash_key(hash);
                let stripe = entry_stripe(key, self.entry_stripes);
                let Some(entry) = entries[stripe].get(key) else {
                    continue;
                };
                if entry.born_round != *born_round {
                    continue;
                }
                promoted.push(PromotedCell {
                    hash: *hash,
                    idx: entry.idx,
                    data: entry.data.clone(),
                });
            }
        }
        drop(entries);

        PromotionPlan { popped, promoted }
    }

    fn recycle_promotion_plan(&self, plan: &mut PromotionPlan) {
        let mut by_round = self.by_round.lock().unwrap();
        by_round.recycle_popped(std::mem::take(&mut plan.popped));
        plan.promoted.clear();
        by_round.promoted = std::mem::take(&mut plan.promoted);
    }

    fn publish_entries(&self, plan: &mut PublishPlan) -> NurseryCommitStats {
        let mut entries = self.entries.write();
        Self::reserve_entries(&mut entries, &plan.insert_buckets);
        let insert_stats =
            self.insert_entries(&mut entries, std::mem::take(&mut plan.insert_buckets));
        let remove_stats =
            self.remove_entries(&mut entries, std::mem::take(&mut plan.remove_buckets));
        drop(entries);

        NurseryCommitStats {
            admitted_count: insert_stats.admitted_count,
            admitted_bytes: insert_stats.admitted_bytes,
            removed_bytes: remove_stats.removed_bytes,
        }
    }

    fn publish_round_updates(&self, mut plan: PublishPlan, stats: NurseryCommitStats) {
        let has_round_updates =
            !plan.pending_by_round.is_empty() || !plan.reused_round_buffers.is_empty();
        let mut by_round = has_round_updates.then(|| self.by_round.lock().unwrap());

        if let Some(by_round) = &mut by_round {
            by_round.append_all(plan.pending_by_round);
            by_round.buffers.append(&mut plan.reused_round_buffers);
        }

        self.live_cell_bytes
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |bytes| {
                bytes
                    .checked_add(stats.admitted_bytes)?
                    .checked_sub(stats.removed_bytes)
            })
            .expect("nursery live cell bytes underflow");
    }

    fn reserve_entries(entries: &mut [CellHashMap<NurseryEntry>], buckets: &[Vec<PendingInsert>]) {
        for (entries, bucket) in entries.iter_mut().zip(buckets) {
            entries.reserve(bucket.len());
        }
    }

    fn insert_entries(
        &self,
        entries: &mut [CellHashMap<NurseryEntry>],
        buckets: Vec<Vec<PendingInsert>>,
    ) -> EntryMutationStats {
        self.worker_pool.install(|| {
            entries
                .par_iter_mut()
                .zip(buckets.into_par_iter())
                .map(|(entries, bucket)| {
                    let mut stats = EntryMutationStats::default();
                    for insert in bucket {
                        let prev = entries.insert(insert.key, insert.entry);
                        assert!(prev.is_none(), "nursery insert rewrote existing cell",);
                        stats.admitted_count += 1;
                        stats.admitted_bytes += insert.data_len;
                    }
                    stats
                })
                .reduce(EntryMutationStats::default, EntryMutationStats::merged)
        })
    }

    fn remove_entries(
        &self,
        entries: &mut [CellHashMap<NurseryEntry>],
        buckets: Vec<Vec<HashBytesKey>>,
    ) -> EntryMutationStats {
        self.worker_pool.install(|| {
            entries
                .par_iter_mut()
                .zip(buckets.into_par_iter())
                .map(|(entries, bucket)| {
                    let mut stats = EntryMutationStats::default();
                    for key in bucket {
                        if let Some(entry) = entries.remove(&key) {
                            stats.removed_bytes += entry.data.len() as u64;
                        }
                    }
                    stats
                })
                .reduce(EntryMutationStats::default, EntryMutationStats::merged)
        })
    }
}

struct PublishPlan {
    insert_buckets: Vec<Vec<PendingInsert>>,
    remove_buckets: Vec<Vec<HashBytesKey>>,
    pending_by_round: BTreeMap<u32, Vec<HashBytes>>,
    reused_round_buffers: Vec<Vec<HashBytes>>,
}

impl PublishPlan {
    fn build(nursery: &CellNursery, commit: NurseryDelta) -> Self {
        let NurseryDelta { inserts, removes } = commit;

        let mut reused_round_buffers = if inserts.is_empty() {
            Vec::new()
        } else {
            let mut by_round = nursery.by_round.lock().unwrap();
            std::mem::take(&mut by_round.buffers)
        };

        let stripe_count = nursery.entry_stripes;
        let inserts_per_stripe = inserts.len().div_ceil(stripe_count);
        let removes_per_stripe = removes.len().div_ceil(stripe_count);
        let mut insert_buckets = (0..stripe_count)
            .map(|_| Vec::with_capacity(inserts_per_stripe))
            .collect::<Vec<_>>();
        let mut remove_buckets = (0..stripe_count)
            .map(|_| Vec::with_capacity(removes_per_stripe))
            .collect::<Vec<_>>();

        let mut round_index = PendingRoundIndex::new(&inserts, &mut reused_round_buffers);
        for insert in inserts {
            let data_len = insert.data.len() as u64;
            round_index.push(insert.born_round, insert.hash);
            let key = *hash_key(&insert.hash);
            let entry = NurseryEntry {
                idx: insert.idx,
                born_round: insert.born_round,
                data: insert.data,
            };
            let stripe = entry_stripe(&key, stripe_count);
            insert_buckets[stripe].push(PendingInsert {
                key,
                entry,
                data_len,
            });
        }
        let pending_by_round = round_index.finish();

        for hash in removes {
            let key = *hash_key(&hash);
            let stripe = entry_stripe(&key, stripe_count);
            remove_buckets[stripe].push(key);
        }

        Self {
            insert_buckets,
            remove_buckets,
            pending_by_round,
            reused_round_buffers,
        }
    }
}

struct PromotionPlan {
    popped: Vec<(u32, Vec<HashBytes>)>,
    promoted: Vec<PromotedCell>,
}

struct PendingRoundIndex<'a> {
    pending_by_round: BTreeMap<u32, Vec<HashBytes>>,
    pending_round: Option<(u32, Vec<HashBytes>)>,
    round_buffers: &'a mut Vec<Vec<HashBytes>>,
    hashes_per_round: usize,
}

impl PendingRoundIndex<'_> {
    fn new<'a>(
        inserts: &[NurseryEntryRecord],
        round_buffers: &'a mut Vec<Vec<HashBytes>>,
    ) -> PendingRoundIndex<'a> {
        let hashes_per_round = inserts
            .len()
            .div_ceil(PROMOTION_DELAY_ROUNDS as usize)
            .max(1);
        PendingRoundIndex {
            pending_by_round: BTreeMap::new(),
            pending_round: None,
            round_buffers,
            hashes_per_round,
        }
    }

    fn push(&mut self, born_round: u32, hash: HashBytes) {
        match &mut self.pending_round {
            Some((round, hashes)) if *round == born_round => hashes.push(hash),
            _ => {
                self.flush_pending();
                let mut hashes = self.take_buffer();
                hashes.push(hash);
                self.pending_round = Some((born_round, hashes));
            }
        }
    }

    fn finish(mut self) -> BTreeMap<u32, Vec<HashBytes>> {
        self.flush_pending();
        self.pending_by_round
    }

    fn flush_pending(&mut self) {
        let Some((born_round, mut hashes)) = self.pending_round.take() else {
            return;
        };
        match self.pending_by_round.entry(born_round) {
            Entry::Vacant(entry) => {
                entry.insert(hashes);
            }
            Entry::Occupied(mut entry) => {
                entry.get_mut().append(&mut hashes);
            }
        }
    }

    fn take_buffer(&mut self) -> Vec<HashBytes> {
        let mut hashes = self.round_buffers.pop().unwrap_or_default();
        hashes.clear();
        if hashes.capacity() < self.hashes_per_round {
            hashes.reserve(self.hashes_per_round - hashes.capacity());
        }
        hashes
    }
}

struct NurseryEntry {
    idx: Idx,
    born_round: u32,
    data: Bytes,
}

struct PendingInsert {
    key: HashBytesKey,
    entry: NurseryEntry,
    data_len: u64,
}

#[derive(Default)]
struct EntryMutationStats {
    admitted_count: u64,
    admitted_bytes: u64,
    removed_bytes: u64,
}

impl EntryMutationStats {
    fn merge(&mut self, other: Self) {
        self.admitted_count += other.admitted_count;
        self.admitted_bytes += other.admitted_bytes;
        self.removed_bytes += other.removed_bytes;
    }

    fn merged(mut self, other: Self) -> Self {
        self.merge(other);
        self
    }
}

struct PromotedCell {
    hash: HashBytes,
    idx: Idx,
    data: Bytes,
}

#[derive(Default)]
struct RoundBuckets {
    by_round: BTreeMap<u32, Vec<HashBytes>>,
    buffers: Vec<Vec<HashBytes>>,
    promoted: Vec<PromotedCell>,
}

impl RoundBuckets {
    fn append_all(&mut self, pending_by_round: BTreeMap<u32, Vec<HashBytes>>) {
        for (born_round, mut hashes) in pending_by_round {
            match self.by_round.entry(born_round) {
                Entry::Vacant(entry) => {
                    entry.insert(hashes);
                }
                Entry::Occupied(mut entry) => {
                    debug_assert!(
                        !hashes.iter().any(|hash| entry.get().contains(hash)),
                        "round bucket MUST NOT HAVE duplicated nursery hash",
                    );
                    entry.get_mut().append(&mut hashes);
                    self.cache_buffer(hashes);
                }
            }
        }
    }

    fn recycle_popped(&mut self, popped: Vec<(u32, Vec<HashBytes>)>) {
        for (_, hashes) in popped {
            self.cache_buffer(hashes);
        }
    }

    fn cache_buffer(&mut self, mut hashes: Vec<HashBytes>) {
        hashes.clear();
        if self.buffers.len() < PROMOTION_DELAY_ROUNDS as usize {
            self.buffers.push(hashes);
        }
    }
}

fn hash_key(hash: &HashBytes) -> &HashBytesKey {
    HashBytesKey::wrap(hash.as_array())
}

fn entry_stripe(key: &HashBytesKey, stripes: usize) -> usize {
    let lane = u64::from_le_bytes(key.0[0..8].try_into().unwrap()) as usize;
    lane % stripes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::shard_state::util::test_hash as hash;

    impl CellNursery {
        fn potatoe() -> Self {
            Self::new(
                NonZeroUsize::new(1).unwrap(),
                Arc::new(
                    rayon::ThreadPoolBuilder::new()
                        .num_threads(1)
                        .build()
                        .unwrap(),
                ),
            )
        }
    }

    fn remove(nursery: &CellNursery, hash: HashBytes) -> Option<usize> {
        let saved_bytes = nursery.get(&hash).map(|data| data.len());
        let mut commit = NurseryDelta::default();
        commit.removes.push(hash);
        nursery.apply_commit(commit);
        saved_bytes
    }

    #[test]
    fn promotes_only_old_live_entries() {
        let nursery = CellNursery::potatoe();
        nursery.apply_commit(commit(vec![
            (hash(1), Idx::new(1), 10, vec![1, 2, 3]),
            (hash(2), Idx::new(2), 10 + PROMOTION_DELAY_ROUNDS - 1, vec![
                4, 5, 6,
            ]),
        ]));

        let mut promoted = Vec::new();
        nursery.drain_promotable(10 + PROMOTION_DELAY_ROUNDS - 1, |hash, _, _| {
            promoted.push(hash);
        });
        assert!(promoted.is_empty());

        nursery.drain_promotable(10 + PROMOTION_DELAY_ROUNDS, |hash, _, _| {
            promoted.push(hash);
        });
        assert_eq!(promoted, vec![hash(1)]);
        assert!(nursery.get(&hash(1)).is_some());
        assert!(nursery.get(&hash(2)).is_some());
    }

    #[test]
    fn stale_bucket_key_is_skipped_after_remove() {
        let nursery = CellNursery::potatoe();
        nursery.apply_commit(commit(vec![(hash(1), Idx::new(1), 10, vec![1, 2, 3])]));
        assert_eq!(remove(&nursery, hash(1)), Some(3));

        let mut promoted = Vec::new();
        nursery.drain_promotable(10 + PROMOTION_DELAY_ROUNDS, |hash, _, _| {
            promoted.push(hash);
        });
        assert!(promoted.is_empty());
        assert!(nursery.get(&hash(1)).is_none());
    }

    #[test]
    fn snapshot_entries_include_all_entries() {
        let nursery = CellNursery::potatoe();
        nursery.apply_commit(commit(vec![
            (hash(1), Idx::new(2), 10, vec![1, 2, 3]),
            (hash(2), Idx::new(1), 10, vec![4, 5, 6]),
        ]));
        let snapshot = nursery
            .snapshot_entries()
            .into_iter()
            .map(|entry| (entry.hash, entry.idx, entry.born_round, entry.data.to_vec()))
            .collect::<Vec<_>>();
        assert_eq!(snapshot, vec![
            (hash(1), Idx::new(2), 10, vec![1, 2, 3]),
            (hash(2), Idx::new(1), 10, vec![4, 5, 6]),
        ],);
    }

    #[test]
    fn striped_commit_parallel_path_updates_all_views() {
        let pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(4)
                .build()
                .unwrap(),
        );
        let nursery = CellNursery::new(NonZeroUsize::new(4).unwrap(), pool);

        let insert = commit(vec![
            (hash(1), Idx::new(1), 10, vec![1]),
            (hash(2), Idx::new(2), 10, vec![2, 2]),
            (hash(3), Idx::new(3), 11, vec![3, 3, 3]),
            (hash(4), Idx::new(4), 11, vec![4, 4, 4, 4]),
        ]);
        nursery.apply_commit(insert);
        assert_eq!(nursery.snapshot_entries().len(), 4);
        assert_eq!(nursery.read().get_idx(&hash(3)), Some(Idx::new(3)));
        assert_eq!(nursery.get(&hash(4)).unwrap().as_ref(), [4, 4, 4, 4]);

        let used_stripes = nursery
            .entries
            .read()
            .iter()
            .filter(|entries| !entries.is_empty())
            .count();
        assert!(used_stripes > 1);

        let mut promoted = Vec::new();
        nursery.drain_promotable(11 + PROMOTION_DELAY_ROUNDS, |hash, idx, data| {
            promoted.push((hash, idx, data.to_vec()));
        });
        assert_eq!(promoted.len(), 4);

        let mut remove = NurseryDelta::default();
        remove.removes.push(hash(2));
        remove.removes.push(hash(4));
        nursery.apply_commit(remove);
        assert!(nursery.get(&hash(2)).is_none());
        assert!(nursery.get(&hash(4)).is_none());
        assert_eq!(nursery.snapshot_entries().len(), 2);
    }

    #[test]
    fn nursery_insert_bytes_survive_after_commit_source_drop() {
        let nursery = CellNursery::potatoe();
        let first = vec![1, 2, 3];
        let second = vec![4, 5, 6];
        nursery.apply_commit(commit(vec![
            (hash(1), Idx::new(1), 10, first),
            (hash(2), Idx::new(2), 10, second),
        ]));
        assert_eq!(remove(&nursery, hash(1)), Some(3));
        assert_eq!(nursery.get(&hash(2)).unwrap().as_ref(), [4, 5, 6]);
        assert_eq!(remove(&nursery, hash(2)), Some(3));
        assert!(nursery.get(&hash(2)).is_none());
    }

    fn commit(entries: Vec<(HashBytes, Idx, u32, Vec<u8>)>) -> NurseryDelta {
        let mut commit = NurseryDelta::default();
        for (hash, idx, born_round, data) in entries {
            commit.inserts.push(NurseryEntryRecord {
                hash,
                idx,
                born_round,
                data: Bytes::from(data),
            });
        }
        commit
    }
}
