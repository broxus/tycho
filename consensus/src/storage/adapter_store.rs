use std::sync::Arc;

use ahash::{HashMapExt, HashSetExt};
use anyhow::{Context, Result};
use bumpalo::Bump;
use itertools::Itertools;
use tycho_util::metrics::HistogramGuard;
use tycho_util::{FastHashMap, FastHashSet};
use weedb::rocksdb::{ReadOptions, WriteBatch};

use super::{POINT_KEY_LEN, fill_point_key, fill_point_prefix, format_point_key};
use crate::effects::AltFormat;
use crate::models::{Point, PointInfo, PointStatus, PointStatusValidated, Round};
use crate::storage::MempoolDb;

#[derive(Clone)]
pub struct MempoolAdapterStore(Arc<MempoolDb>);

impl MempoolAdapterStore {
    pub fn new(mempool_db: Arc<MempoolDb>) -> Self {
        Self(mempool_db)
    }

    /// allows to remove no more needed data before sync and store of newly created dag part
    pub fn report_new_start(&self, next_expected_anchor: u32) {
        // set as committed because every anchor is repeatable by stored history (if it exists)
        self.0.commit_finished.set_max_raw(next_expected_anchor);
    }

    /// Next must call [`Self::set_committed`] for GC as watch notification is deferred
    pub fn expand_anchor_history<'b>(
        &self,
        anchor: &PointInfo,
        history: &[PointInfo],
        bump: &'b Bump,
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
        // may skip expand part, but never skip set committed - let it write what it should
        self.set_committed_db(anchor, history)
            .with_context(|| context(anchor, history))
            .expect("DB set committed");
        payloads
    }

    /// may skip [`Self::expand_anchor_history`] part, but never skip this one
    pub fn set_committed(&self, anchor_round: Round) {
        // commit is finished when history payloads is read from DB and marked committed,
        // so that data may be removed consistently with any settings
        self.0.commit_finished.set_max(anchor_round);
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

        let mut buf = [0_u8; POINT_KEY_LEN];

        let db = self.0.db.rocksdb();
        let status_cf = self.0.db.points_status.cf();
        let mut batch = WriteBatch::default();

        let mut status = PointStatusValidated::default();
        status.committed_at_round = Some(anchor.round().0);
        let status_encoded = status.encode();

        for info in history {
            fill_point_key(info.round().0, info.digest().inner(), &mut buf);
            batch.merge_cf(&status_cf, buf.as_slice(), &status_encoded);
        }

        Ok(db.write(batch)?)
    }
}
