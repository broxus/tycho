use std::collections::BTreeMap;
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

use super::{
    AnchorFlags, MempoolStore, POINT_KEY_LEN, fill_point_key, fill_point_prefix, format_point_key,
};
use crate::effects::AltFormat;
use crate::models::{
    CommitHistoryPart, Point, PointInfo, PointRestore, PointRestoreSelect, PointStatus,
    PointStatusValidated, Round,
};
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
        anchor: &PointInfo,
        history: &[PointInfo],
        bump: &'b Bump,
        set_committed_in_db: bool,
    ) -> Vec<&'b [u8]> {
        fn context(anchor: &PointInfo, history: &[PointInfo]) -> String {
            format!(
                "anchor {:?} history {} points rounds [{}..{}]",
                anchor.id().alt(),
                history.len(),
                history.first().map(|i| i.round().0).unwrap_or_default(),
                history.last().map(|i| i.round().0).unwrap_or_default()
            )
        }

        let payloads = if history.is_empty() {
            // history is checked at the end of DAG commit, leave traces in case its broken
            tracing::warn!(
                "anchor {:?} has empty history, it's ok only for anchor at DAG bottom round \
                 immediately after an unrecoverable gap",
                anchor.id().alt()
            );
            Vec::new()
        } else {
            self.load_payload(history, bump)
                .with_context(|| context(anchor, history))
                .expect("DB expand anchor history")
        };
        if set_committed_in_db {
            self.set_committed_db(anchor, history)
                .with_context(|| context(anchor, history))
                .expect("DB set committed");
        }
        payloads
    }

    pub fn expand_anchor_history_arena_size(&self, history: &[PointInfo]) -> usize {
        let payload_bytes =
            (history.iter()).fold(0, |acc, info| acc + info.payload_bytes() as usize);
        let keys_bytes = history.len() * POINT_KEY_LEN;
        payload_bytes + keys_bytes
    }

    fn load_payload<'b>(&self, history: &[PointInfo], bump: &'b Bump) -> Result<Vec<&'b [u8]>> {
        let _call_duration =
            HistogramGuard::begin("tycho_mempool_store_expand_anchor_history_time");
        let mut buf = [0_u8; POINT_KEY_LEN];
        let mut keys = FastHashSet::<&'b [u8]>::with_capacity(history.len());
        for info in history {
            fill_point_key(info.round().0, info.digest().inner(), &mut buf);
            keys.insert(bump.alloc_slice_copy(&buf));
        }
        buf.fill(0);

        let mut opt = ReadOptions::default();

        let first = (history.first()).context("anchor history must not be empty")?;
        fill_point_prefix(first.round().0, &mut buf);
        opt.set_iterate_lower_bound(buf);

        let last = history.last().context("anchor history must not be empty")?;
        fill_point_prefix(last.round().next().0, &mut buf);
        opt.set_iterate_upper_bound(buf);

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
                    // we panic thus we don't care about performance
                    let full_point =
                        Point::from_bytes(bytes.to_vec()).context("deserialize point")?;
                    panic!("iter read non-unique point {:?}", full_point.info().id())
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
            keys.iter().map(|key| format_point_key(key)).join(",\n")
        );
        anyhow::ensure!(found.len() == history.len(), "stored point key collision");

        let mut result = Vec::with_capacity(total_payload_items);
        for info in history {
            fill_point_key(info.round().0, info.digest().inner(), &mut buf);
            let payload = found
                .remove(buf.as_slice())
                .with_context(|| format_point_key(&buf))
                .context("key was searched in db but was not found")?;
            for msg in payload {
                result.push(msg);
            }
        }

        Ok(result)
    }

    fn set_committed_db(&self, anchor: &PointInfo, history: &[PointInfo]) -> Result<()> {
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_set_committed_status_time");

        let anchor_round =
            NonZeroU32::try_from(anchor.round().0).context("zero round cannot have points")?;

        let mut buf = [0_u8; POINT_KEY_LEN];

        let db = self.0.db.rocksdb();
        let status_cf = self.0.db.points_status.cf();
        let mut batch = WriteBatch::with_capacity_bytes(
            PointStatusValidated::size_hint() * (1 + history.len()),
        );

        let mut status = PointStatusValidated::default();
        let mut status_encoded = Vec::with_capacity(PointStatusValidated::size_hint());

        status.anchor_flags = AnchorFlags::Used;
        status.write_to(&mut status_encoded);

        fill_point_key(anchor_round.get(), anchor.digest().inner(), &mut buf);
        batch.merge_cf(&status_cf, buf.as_slice(), &status_encoded);
        status_encoded.clear();

        status = PointStatusValidated::default();
        for (index, info) in history.iter().enumerate() {
            status.committed = Some(CommitHistoryPart {
                anchor_round,
                seq_no: u32::try_from(index).context("anchor has insanely long history")?,
            });

            status.write_to(&mut status_encoded);

            fill_point_key(info.round().0, info.digest().inner(), &mut buf);
            batch.merge_cf(&status_cf, buf.as_slice(), &status_encoded);
            status_encoded.clear();
        }

        Ok(db.write(batch)?)
    }

    pub fn load_history_since(
        &self,
        bottom_round: u32,
    ) -> BTreeMap<u32, (PointInfo, Vec<PointInfo>)> {
        let store = MempoolStore::new(self.0.clone());

        let Some(last_db_round) = store.last_round() else {
            tracing::warn!("Mempool db is empty");
            return Default::default();
        };

        let mut anchors = FastHashMap::new();

        let mut items = store
            .load_restore(&RangeInclusive::new(Round(bottom_round), last_db_round))
            .into_iter()
            .filter_map(|item| match item {
                PointRestoreSelect::Ready(PointRestore::Validated(info, status)) => {
                    Some((info, status))
                }
                _ => None,
            })
            .inspect(|(info, status)| {
                if status.anchor_flags.contains(AnchorFlags::Used) {
                    anchors.insert(info.round(), info.clone());
                }
            })
            .filter_map(|(info, status)| status.committed.map(|committed| (committed, info)))
            .collect::<Vec<_>>();

        items.sort_unstable_by_key(|(committed, _)| (committed.anchor_round, committed.seq_no));

        let mut by_anchor_round = BTreeMap::new();

        // should not allocate as all items are sorted
        let grouped = items
            .into_iter()
            .chunk_by(|(committed, _)| Round(committed.anchor_round.get()));

        for (anchor_round, group) in &grouped {
            let mut keyed_vec = group.collect::<Vec<_>>();
            // should be a no-op
            keyed_vec.sort_unstable_by_key(|(committed, _)| committed.seq_no);
            let point_vec = keyed_vec.into_iter().map(|(_, info)| info);
            match anchors.remove(&anchor_round) {
                Some(anchor) => {
                    by_anchor_round.insert(anchor_round.0, (anchor.clone(), point_vec.collect()));
                }
                None => {
                    tracing::error!(
                        anchor = anchor_round.0,
                        "cannot reproduce history: no anchor point"
                    );
                }
            }
        }
        by_anchor_round
    }
}
