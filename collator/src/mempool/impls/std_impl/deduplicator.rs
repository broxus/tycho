use std::collections::{btree_map, hash_map, BTreeMap};

use tycho_util::{FastHashMap, FastHashSet};

use crate::mempool::MempoolAnchorId;

pub struct Deduplicator {
    // must keep latest round for every hash, until threshold passes
    hash_max_round: FastHashMap<[u8; 32], u32>,
    // must remove outdated (threshold elapsed) from the low end of an ordered map,
    // and also remove hashes from the other map
    round_to_hashes: BTreeMap<u32, FastHashSet<[u8; 32]>>,
    // inclusive amount of rounds to keep; every insert of a hash resets its threshold to 0
    round_threshold: MempoolAnchorId,
}

impl Deduplicator {
    pub fn new(round_threshold: u16) -> Self {
        Self {
            hash_max_round: FastHashMap::default(),
            round_to_hashes: BTreeMap::default(),
            round_threshold: round_threshold as MempoolAnchorId,
        }
    }

    pub fn check_unique(&mut self, anchor_round: MempoolAnchorId, hash: &[u8; 32]) -> bool {
        if self
            .round_to_hashes
            .entry(anchor_round)
            .or_default()
            .insert(*hash)
        {
            // branch: first insert at this round
            if let Some(old_round) = self.hash_max_round.insert(*hash, anchor_round) {
                // branch: cached, i.e. duplicate insert before threshold passed
                if old_round < anchor_round {
                    // branch: remove outdated, as we got updated
                    match self.round_to_hashes.entry(old_round) {
                        btree_map::Entry::Occupied(mut round_to_hashes) => {
                            let hashes = round_to_hashes.get_mut();
                            assert!(hashes.remove(hash), "hash must be in set for round");
                            if hashes.is_empty() {
                                round_to_hashes.remove();
                            }
                        }
                        btree_map::Entry::Vacant(_) => {
                            panic!("set of hashes must not be left empty, must delete entry")
                        }
                    }
                }
                // in case an outdated value was stored - act as if it was already removed;
                // this allows to clean the cache no matter before or after the check
                old_round < anchor_round.saturating_sub(self.round_threshold)
            } else {
                // branch: not cached, i.e. first insert since threshold passed
                true
            }
        } else {
            // branch: duplicate insert at this round
            false
        }
    }

    pub fn clean(&mut self, anchor_round: MempoolAnchorId) {
        let bottom_round = anchor_round.saturating_sub(self.round_threshold);
        while let Some(round_to_hashes) = self.round_to_hashes.first_entry() {
            if *round_to_hashes.key() < bottom_round {
                for hash in round_to_hashes.get() {
                    match self.hash_max_round.entry(*hash) {
                        hash_map::Entry::Occupied(hash_max_round) => {
                            if *hash_max_round.get() < bottom_round {
                                hash_max_round.remove();
                            }
                        }
                        hash_map::Entry::Vacant(_) => {
                            panic!("map of hashes to rounds was not cleaned, must delete entry")
                        }
                    }
                }
                round_to_hashes.remove();
            } else {
                break;
            }
        }
        if self.hash_max_round.capacity() <= self.hash_max_round.len() / 4 {
            self.hash_max_round
                .shrink_to(self.hash_max_round.capacity() / 2);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn dedup_externals_test() {
        let mut cache = Deduplicator::new(100);
        let start_round = 0;
        let first = [u8::MIN; 32];
        let second = [u8::MAX; 32];
        assert!(
            cache.check_unique(start_round, &first),
            "first insert must be unique"
        );
        assert!(
            cache.check_unique(start_round, &second),
            "first insert must be unique"
        );
        for round_id in start_round..=301 {
            if round_id < 150 {
                assert!(
                    !cache.check_unique(round_id, &first),
                    "duplicate insert must not be unique"
                );
            }

            assert!(
                !cache.check_unique(round_id, &second),
                "duplicate insert must not be unique"
            );

            assert!(
                !cache.check_unique(round_id, &first),
                "duplicate insert must not be unique"
            );

            assert_eq!(cache.hash_max_round.len(), 2);
            assert_eq!(cache.round_to_hashes.len(), 1);
            cache.clean(round_id);
            assert_eq!(cache.hash_max_round.len(), 2);
            assert_eq!(cache.round_to_hashes.len(), 1);
        }
    }
}