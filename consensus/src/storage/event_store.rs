use std::sync::Arc;

use ahash::{HashMapExt, HashSetExt};
use anyhow::{self, Context, Result};
use itertools::Itertools;
use tl_proto::{TlRead, TlWrite};
use tycho_util::{FastHashMap, FastHashSet};
use weedb::rocksdb::{Direction, IteratorMode, MergeOperands, WriteBatch};

use crate::engine::MempoolConfig;
use crate::models::{Round, UnixTime};
use crate::moderator::EventData;
use crate::storage::{
    EVENT_KEY_LEN, EventKey, MempoolDb, POINT_KEY_LEN, ShortEventData, StoredEventData,
    fill_point_key,
};

#[derive(Clone)]
pub struct EventStore(Arc<MempoolDb>);

impl EventStore {
    pub fn new(mempool_db: Arc<MempoolDb>) -> Self {
        Self(mempool_db)
    }

    pub fn load_short_events(&self, since: UnixTime) -> Result<Vec<(EventKey, ShortEventData)>> {
        let mut event_key: [u8; EVENT_KEY_LEN] = [0; _];
        event_key[..UnixTime::MAX_TL_BYTES].copy_from_slice(&since.millis().to_be_bytes());
        (self.0.db.events)
            .iterator(IteratorMode::From(&event_key, Direction::Forward))
            .map_ok(|(k, v)| {
                let key = EventKey::read_from(&mut &k[..])?;
                let short = ShortEventData::read_from(&mut &v[..])?;
                Ok((key, short))
            })
            .flatten()
            .try_collect()
    }

    pub fn store_events(
        &self,
        events: &[(EventKey, &EventData)],
        conf: &MempoolConfig,
    ) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let mut point_key_buf = [0; POINT_KEY_LEN];

        let mut all_points_map = FastHashMap::with_capacity(events.len()); // rough
        let mut all_keys_set = FastHashSet::with_capacity(events.len()); // rough
        let mut stored_events = Vec::with_capacity(events.len());

        // prepare stored events and also unique keys with full point flag

        let mut e_points = Vec::new();
        let mut e_point_keys = Vec::new();
        let mut e_unique_point_keys = FastHashSet::default();
        for (_, data) in events {
            let Some(severity) = data.severity() else {
                continue;
            };

            e_points.clear();
            e_point_keys.clear();
            e_unique_point_keys.clear();

            data.fill_points(&mut e_points, &mut e_point_keys);

            for (round, digest) in &e_point_keys {
                all_keys_set.insert((*round, *digest));
                e_unique_point_keys.insert((*round, *digest));
            }
            for point in &e_points {
                all_points_map.insert((point.info().round(), point.info().digest()), *point);
                e_unique_point_keys.insert((point.info().round(), point.info().digest()));
            }

            let mut point_keys_to_store = Vec::with_capacity(e_unique_point_keys.len());
            for (round, digest) in &e_unique_point_keys {
                fill_point_key(round.0, digest.inner(), &mut point_key_buf);
                point_keys_to_store.push(point_key_buf);
            }
            point_keys_to_store.sort_unstable(); // sort big endian bytes - ready to join

            stored_events.push(StoredEventData {
                tag: data.tag(),
                severity,
                peer_id: *data.peer_id(),
                point_keys: point_keys_to_store,
                message: data.to_string(),
            });
        }
        drop(e_points);
        drop(e_point_keys);
        drop(e_unique_point_keys);

        for key in all_points_map.keys() {
            all_keys_set.remove(key); // will not read points that are in mem
        }

        let mut batch = WriteBatch::with_capacity_bytes(
            ((stored_events.iter()).map(|e| EVENT_KEY_LEN + e.max_size_hint())).sum::<usize>() + {
                (POINT_KEY_LEN + conf.point_max_bytes + Round::MAX_TL_SIZE + UnixTime::MAX_TL_BYTES)
                    * (all_keys_set.len() + all_points_map.len())
            },
        );

        let events_cf = self.0.db.events.cf();
        let event_round_time_cf = self.0.db.event_round_time.cf();
        let mut event_bytes = Vec::with_capacity(
            (stored_events.iter().map(|e| e.max_size_hint()).max())
                .map_or(0, |max| EVENT_KEY_LEN + max),
        );
        for (stored_data, (event_key, _)) in stored_events.iter().zip(events) {
            event_bytes.clear();
            event_key.write_to(&mut event_bytes);
            stored_data.write_to(&mut event_bytes);

            batch.put_cf(
                &events_cf,
                &event_bytes[..EVENT_KEY_LEN],
                &event_bytes[EVENT_KEY_LEN..],
            );
            for point_key in &stored_data.point_keys {
                batch.merge_cf(
                    &event_round_time_cf,
                    &point_key[..Round::MAX_TL_SIZE],
                    &event_bytes[..UnixTime::MAX_TL_BYTES],
                );
            }
        }
        drop(event_bytes);

        let event_points_cf = self.0.db.event_points.cf();

        // full points are most likely not yet stored in DB, but keys are most likely still stored

        // looks like we cannot totally avoid writing duplicates ...

        let mut all_keys_sorted = all_keys_set.iter().collect::<Vec<_>>();
        all_keys_sorted.sort_unstable();

        let mut target_iter = self.0.db.event_points.raw_iterator();
        let mut source_iter = self.0.db.points.raw_iterator();
        for (round, digest) in all_keys_sorted {
            fill_point_key(round.0, digest.inner(), &mut point_key_buf);

            target_iter.seek(point_key_buf);
            target_iter.status().context("target iter after seek")?;

            if target_iter.key().is_some_and(|key| key == point_key_buf) {
                continue; // skip already stored points; target is smaller than source
            }

            source_iter.seek(point_key_buf);
            source_iter.status().context("source iter after seek")?;

            let Some((f_key, value)) = source_iter.item() else {
                break; // source iter is exhausted
            };
            if f_key == point_key_buf {
                batch.put_cf(&event_points_cf, f_key, value);
            } // else: skip not found points, though they shouldn't be GCed yet
        }
        drop(target_iter);
        drop(source_iter);

        for ((round, digest), point) in all_points_map {
            fill_point_key(round.0, digest.inner(), &mut point_key_buf);
            batch.put_cf(&event_points_cf, point_key_buf, point.serialized());
        }

        let db = self.0.db.rocksdb();
        db.write(batch).context("write events batch")?;
        Ok(())
    }
}

/// Merge `event_round_time` cf: simply take the max big-endian value.
pub(super) fn merge_max_value(
    _key: &[u8],
    stored: Option<&[u8]>,
    new_status_queue: &MergeOperands,
) -> Option<Vec<u8>> {
    stored
        .into_iter()
        .chain(new_status_queue)
        .max()
        .map(|a| a.to_vec())
}
