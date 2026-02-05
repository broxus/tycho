use std::num::NonZeroU32;
use std::ops::RangeInclusive;
use std::sync::Arc;

use ahash::{HashMapExt, HashSetExt};
use anyhow::{Context, Result};
use bumpalo::Bump;
use itertools::Itertools;
use tycho_util::metrics::HistogramGuard;
use tycho_util::{FastHashMap, FastHashSet};
use weedb::rocksdb::{ReadOptions, WriteBatch};

use super::MempoolStore;
use crate::effects::AltFormat;
use crate::engine::{ConsensusConfigExt, MempoolConfig};
use crate::models::point_status::{
    AnchorFlags, CommitHistoryPart, PointStatusCommittable, PointStatusStore,
};
use crate::models::{AnchorData, MempoolOutput, Point, PointInfo, PointKey, PointRestore, Round};
use crate::storage::MempoolDb;

#[derive(Clone)]
pub struct MempoolAdapterStore(Arc<MempoolDb>);

impl MempoolAdapterStore {
    pub fn new(mempool_db: Arc<MempoolDb>) -> Self {
        Self(mempool_db)
    }

    /// Next must call [`Self::set_committed`] for GC as watch notification is deferred
    pub fn expand_anchor_history<'b>(
        &self,
        adata: &AnchorData,
        bump: &'b Bump,
    ) -> Result<Vec<&'b [u8]>> {
        if adata.history.is_empty() {
            // history is checked at the end of DAG commit, leave traces in case its broken
            tracing::warn!(
                "anchor {:?} has empty history, it's ok only for anchor at DAG bottom round \
                 immediately after an unrecoverable gap",
                adata.anchor.id().alt()
            );
            Ok(Vec::new())
        } else {
            self.load_payload(&adata.history, bump)
                .with_context(|| Self::history_context(adata))
                .context("DB expand anchor history")
        }
    }

    fn history_context(anchor_data: &AnchorData) -> String {
        let range = Option::zip(anchor_data.history.first(), anchor_data.history.last())
            .map(|(first, last)| [first.round().0, last.round().0]);
        format!(
            "anchor {:?} history {} points in rounds range {:?}",
            anchor_data.anchor.id().alt(),
            anchor_data.history.len(),
            range.as_slice()
        )
    }

    pub fn expand_anchor_history_arena_size(&self, history: &[PointInfo]) -> usize {
        let payload_bytes =
            (history.iter()).fold(0, |acc, info| acc + info.payload_bytes() as usize);
        let keys_bytes = history.len() * PointKey::MAX_TL_BYTES;
        payload_bytes + keys_bytes
    }

    fn load_payload<'b>(&self, history: &[PointInfo], bump: &'b Bump) -> Result<Vec<&'b [u8]>> {
        let _call_duration =
            HistogramGuard::begin("tycho_mempool_store_expand_anchor_history_time");
        let mut key_buf = [0; _];
        let mut keys = FastHashSet::<&'b [u8]>::with_capacity(history.len());
        for info in history {
            info.key().fill(&mut key_buf);
            keys.insert(bump.alloc_slice_copy(&key_buf));
        }

        let mut opt = ReadOptions::default();

        let first = (history.first()).context("anchor history must not be empty")?;
        PointKey::fill_prefix(first.round(), &mut key_buf);
        opt.set_iterate_lower_bound(key_buf);

        let last = history.last().context("anchor history must not be empty")?;
        PointKey::fill_prefix(last.round().next(), &mut key_buf);
        opt.set_iterate_upper_bound(key_buf);

        let db = self.0.db.rocksdb();
        let points_cf = self.0.db.points.cf();

        let mut found = FastHashMap::with_capacity(history.len());
        let mut iter = db.raw_iterator_cf_opt(&points_cf, opt);
        iter.seek_to_first();

        let mut total_payload_items = 0;
        while iter.valid() {
            let key = iter.key().context("history iter invalidated on key")?;
            if let Some(key) = keys.take(key) {
                let bytes = iter.value().context("history iter invalidated on value")?;
                let payload =
                    Point::read_payload_from_tl_bytes(bytes, bump).context("deserialize point")?;

                total_payload_items += payload.len();
                if found.insert(key, payload).is_some() {
                    // we'll panic thus we don't care about performance
                    let full_point = Point::from_bytes(bytes.to_vec())
                        .context("deserialize non-unique point")?;
                    anyhow::bail!("iter read non-unique point {:?}", full_point.info().id());
                }
            }
            if keys.is_empty() {
                break;
            }
            iter.next();
        }
        iter.status().context("anchor history iter is not ok")?;
        drop(iter);

        anyhow::ensure!(
            keys.is_empty(),
            "{} history points were not found id db:\n{}",
            keys.len(),
            keys.into_iter().map(PointKey::format_loose).join(",\n")
        );
        anyhow::ensure!(found.len() == history.len(), "stored point key collision");

        let mut result = Vec::with_capacity(total_payload_items);
        for info in history {
            info.key().fill(&mut key_buf);
            let payload = found
                .remove(&key_buf[..])
                .with_context(|| PointKey::format_loose(&key_buf))
                .context("key was searched in db but was not found")?;
            for msg in payload {
                result.push(msg);
            }
        }

        Ok(result)
    }

    pub fn set_committed(&self, adata: &AnchorData) -> Result<()> {
        self.set_committed_db(adata)
            .with_context(|| Self::history_context(adata))
            .context("DB set committed")
    }

    fn set_committed_db(&self, adata: &AnchorData) -> Result<()> {
        let AnchorData {
            proof_key,
            anchor,
            history,
            ..
        } = adata;

        let _call_duration = HistogramGuard::begin("tycho_mempool_store_set_committed_status_time");

        let anchor_round =
            NonZeroU32::try_from(anchor.round().0).context("zero round cannot have points")?;

        let mut key_buf = [0; _];

        let db = self.0.db.rocksdb();
        let status_cf = self.0.db.points_status.cf();
        let mut batch = WriteBatch::with_capacity_bytes(
            (PointKey::MAX_TL_BYTES + PointStatusCommittable::BYTE_SIZE) * (2 + history.len()),
        );

        let mut status = PointStatusCommittable::default();
        let mut status_encoded: [u8; PointStatusCommittable::BYTE_SIZE] = [0; _];

        status.anchor_flags = AnchorFlags::Used | AnchorFlags::Anchor;
        status.fill(&mut status_encoded)?;

        anchor.key().fill(&mut key_buf);
        batch.merge_cf(&status_cf, &key_buf[..], &status_encoded[..]);

        status = PointStatusCommittable::default();
        status.anchor_flags = AnchorFlags::Used | AnchorFlags::Proof;
        status.fill(&mut status_encoded)?;

        proof_key.fill(&mut key_buf);
        batch.merge_cf(&status_cf, &key_buf[..], &status_encoded[..]);

        status = PointStatusCommittable::default();
        for (index, info) in history.iter().enumerate() {
            status.committed = Some(CommitHistoryPart {
                anchor_round,
                seq_no: u32::try_from(index).context("anchor has insanely long history")?,
            });

            status.fill(&mut status_encoded)?;

            info.key().fill(&mut key_buf);
            batch.merge_cf(&status_cf, &key_buf[..], &status_encoded[..]);
        }

        Ok(db.write(batch)?)
    }

    pub async fn restore_committed(
        &self,
        top_processed_to_anchor: u32,
        conf: &MempoolConfig,
    ) -> Result<Vec<MempoolOutput>> {
        // no overlay id check: do not rewrite db state, just try to load data

        let bottom_round = (conf.genesis_round)
            .max(Round(top_processed_to_anchor) - conf.consensus.replay_anchor_rounds());

        let anchors = self
            .load_history_since(bottom_round, conf)
            .context("load history")?;

        anyhow::ensure!(
            anchors.is_sorted_by_key(|adata| adata.anchor.round()),
            "anchors must be restored in order"
        );

        let mut output = Vec::<MempoolOutput>::new();
        let mut last_visited = None;

        for adata in anchors {
            let has_gap = last_visited.is_some() && last_visited != adata.prev_anchor;
            if has_gap {
                anyhow::ensure!(adata.prev_anchor.is_some(), "don't expect genesis");
                let adata_bottom = adata.history.first().unwrap_or(&adata.anchor).round();
                let full_history_bottom = adata_bottom + conf.consensus.commit_history_rounds.get();
                output.push(MempoolOutput::NewStartAfterGap(full_history_bottom));
            };
            last_visited = Some(adata.anchor.round());
            output.push(MempoolOutput::NextAnchor(adata));
        }

        Ok(output)
    }

    fn load_history_since(
        &self,
        bottom_round: Round,
        conf: &MempoolConfig,
    ) -> Result<Vec<AnchorData>> {
        let store = MempoolStore::new(self.0.clone());

        let Some(last_db_round) = store.last_round() else {
            tracing::warn!("Mempool db is empty");
            return Ok(Default::default());
        };

        let mut anchors = FastHashMap::new();
        let mut proofs = FastHashMap::new();

        let mut items = store
            .load_restore(&RangeInclusive::new(bottom_round, last_db_round))
            .into_iter()
            .filter_map(|item| match item {
                PointRestore::Valid(info, status) => {
                    Some((info, status.anchor_flags, status.committed))
                }
                PointRestore::TransInvalid(info, status) => {
                    Some((info, status.anchor_flags, status.committed))
                }
                _ => None,
            })
            .filter_map(|(info, anchor_flags, committed_opt)| {
                if anchor_flags.contains(AnchorFlags::Used | AnchorFlags::Anchor) {
                    anchors.insert(info.round(), info.clone());
                } else if anchor_flags.contains(AnchorFlags::Used | AnchorFlags::Proof) {
                    proofs.insert(info.round(), info.clone());
                }
                committed_opt.map(|committed| (committed, info))
            })
            .collect::<Vec<_>>();

        items.sort_unstable_by_key(|(committed, _)| (committed.anchor_round, committed.seq_no));

        let mut result = Vec::with_capacity(anchors.len());

        // should not allocate as all items are sorted
        let grouped = items
            .into_iter()
            .chunk_by(|(committed, _)| Round(committed.anchor_round.get()));

        for (anchor_round, group) in &grouped {
            let mut keyed_vec = group.collect::<Vec<_>>();
            // should be already ordered
            keyed_vec.sort_unstable_by_key(|(committed, _)| committed.seq_no);
            let anchor = anchors.remove(&anchor_round).with_context(|| {
                format!("no anchor point for commit at round {}", anchor_round.0)
            })?;
            let proof = proofs.remove(&anchor_round.next()).with_context(|| {
                format!("no proof point for commit at round {}", anchor_round.0)
            })?;
            result.push(AnchorData {
                proof_key: proof.key(),
                anchor,
                prev_anchor: proof
                    .chained_anchor_proof()
                    .map(|link| link.to.round)
                    .filter(|r| *r > conf.genesis_round)
                    .map(|r| r.prev()),
                history: keyed_vec.into_iter().map(|(_, info)| info).collect(),
            });
        }
        Ok(result)
    }
}
