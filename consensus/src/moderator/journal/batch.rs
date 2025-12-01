use ahash::{HashMapExt, HashSetExt};
use tycho_util::{FastHashMap, FastHashSet};

use crate::models::{Digest, Point, Round};
use crate::moderator::journal::item::JournalItemFull;
use crate::moderator::{RecordKey, RecordValue};
use crate::storage::{POINT_KEY_LEN, fill_point_key};

pub struct RecordBatch<'a> {
    /// records to be stored
    pub records: Vec<(&'a RecordKey, RecordValue)>,
    /// points extracted directly from given records that are most likely not stored in main DB
    pub points: FastHashMap<(Round, &'a Digest), &'a Point>,
    /// point keys extracted directly from given records that are most likely stored in main DB
    pub keys: Vec<(Round, &'a Digest)>,
}

/// convert in-mem items into stored records with their attributes
pub fn batch(full_items: &[JournalItemFull]) -> RecordBatch<'_> {
    if full_items.is_empty() {
        return RecordBatch {
            points: FastHashMap::default(),
            keys: Vec::default(),
            records: Vec::default(),
        };
    }

    let mut point_key_buf = [0; POINT_KEY_LEN];

    let mut all_points_map = FastHashMap::with_capacity(full_items.len()); // rough
    let mut all_keys_set = FastHashSet::with_capacity(full_items.len()); // rough
    let mut records = Vec::with_capacity(full_items.len());

    // prepare stored records and also unique points or their keys to copy from main storage

    let mut r_points = Vec::new();
    let mut r_point_keys = Vec::new();
    let mut r_unique_point_keys = FastHashSet::default();
    for JournalItemFull { key, item } in full_items {
        if !item.action().store() {
            continue;
        }

        r_points.clear();
        r_point_keys.clear();
        r_unique_point_keys.clear();

        item.fill_points(&mut r_points, &mut r_point_keys);

        for (round, digest) in &r_point_keys {
            all_keys_set.insert((*round, *digest));
            r_unique_point_keys.insert((*round, *digest));
        }
        for point in &r_points {
            all_points_map.insert((point.info().round(), point.info().digest()), *point);
            r_unique_point_keys.insert((point.info().round(), point.info().digest()));
        }

        let mut linked_point_keys = Vec::with_capacity(r_unique_point_keys.len());
        for (round, digest) in &r_unique_point_keys {
            fill_point_key(round.0, digest.inner(), &mut point_key_buf);
            linked_point_keys.push(point_key_buf);
        }
        linked_point_keys.sort_unstable(); // sort big endian bytes - ready to join

        records.push((key, RecordValue {
            kind: item.kind(),
            is_ban_related: item.action().is_ban_related(),
            peer_id: *item.peer_id(),
            point_keys: linked_point_keys,
            message: item.to_string(),
        }));
    }

    for key in all_points_map.keys() {
        all_keys_set.remove(key); // will not read points that are in mem
    }

    let mut keys_sorted = all_keys_set.into_iter().collect::<Vec<_>>();
    keys_sorted.sort_unstable();

    RecordBatch {
        points: all_points_map,
        keys: keys_sorted,
        records,
    }
}
