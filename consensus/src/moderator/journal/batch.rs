use ahash::{HashMapExt, HashSetExt};
use tycho_util::{FastHashMap, FastHashSet};

use crate::models::{Point, PointKey};
use crate::moderator::journal::item::JournalItemFull;
use crate::moderator::{RecordKey, RecordValue};

pub struct RecordBatch<'a> {
    /// records to be stored
    pub records: Vec<(&'a RecordKey, RecordValue)>,
    /// points extracted directly from given records that are most likely not stored in main DB
    pub points: FastHashMap<PointKey, &'a Point>,
    /// point keys extracted directly from given records that are most likely stored in main DB
    pub keys: Vec<PointKey>,
    pub max_record_bytes: usize,
    pub all_record_bytes: usize,
}

/// convert in-mem items into stored records with their attributes
pub fn batch(full_items: &[JournalItemFull]) -> RecordBatch<'_> {
    let mut all_keys_set = FastHashSet::with_capacity(full_items.len()); // rough
    let mut batch = RecordBatch {
        points: FastHashMap::with_capacity(full_items.len()), // rough
        keys: Vec::with_capacity(full_items.len()),           // rough
        records: Vec::with_capacity(full_items.len()),
        max_record_bytes: 0,
        all_record_bytes: 0,
    };

    // prepare stored records and also unique points or their keys to copy from main storage

    let mut r_points = Vec::new();
    let mut r_point_keys = Vec::new();
    let mut r_unique_point_keys = FastHashSet::default();

    for JournalItemFull { key, item } in full_items {
        if !item.action().store() {
            continue;
        }

        item.fill_points(&mut r_points, &mut r_point_keys);

        for key in r_point_keys.drain(..) {
            all_keys_set.insert(key);
            r_unique_point_keys.insert(key);
        }
        for point in r_points.drain(..) {
            batch.points.insert(point.info().key(), point);
            r_unique_point_keys.insert(point.info().key());
        }

        let mut linked_point_keys = Vec::with_capacity(r_unique_point_keys.len());
        for key in r_unique_point_keys.drain() {
            linked_point_keys.push(key);
        }
        linked_point_keys.sort_unstable(); // sort big endian bytes - ready to join

        let record_value = RecordValue {
            kind: item.kind(),
            is_ban_related: item.action().is_ban_related(),
            peer_id: *item.peer_id(),
            point_keys: linked_point_keys,
            message: item.to_string(),
        };

        let record_bytes =
            RecordKey::MAX_TL_BYTES + tl_proto::TlWrite::max_size_hint(&record_value);
        batch.max_record_bytes = batch.max_record_bytes.max(record_bytes);
        batch.all_record_bytes += record_bytes;

        batch.records.push((key, record_value));
    }

    for key in batch.points.keys() {
        all_keys_set.remove(key); // will not read points that are in mem
    }

    batch.keys.extend(all_keys_set);
    batch.keys.sort_unstable();

    batch
}
