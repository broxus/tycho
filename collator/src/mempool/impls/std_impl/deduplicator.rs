use std::collections::hash_map;
use std::ops::Bound;

use indexmap::IndexMap;
use tycho_util::{FastHashMap, FastHashSet};

use crate::mempool::MempoolAnchorId;

pub struct Deduplicator {
    // must keep latest round for every hash, until threshold passes
    hash_max_round: FastHashMap<[u8; 32], u32>,
    // must remove outdated (threshold elapsed) from the low end of an ordered map,
    // and also remove hashes from the other map
    round_to_hashes: IndexMap<u32, FastHashSet<[u8; 32]>, ahash::RandomState>,
    // inclusive amount of rounds to keep; every insert of a hash resets its threshold to 0
    round_threshold: MempoolAnchorId,
}

impl Deduplicator {
    pub fn new(round_threshold: u16) -> Self {
        Self {
            hash_max_round: Default::default(),
            round_to_hashes: Default::default(),
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
                // branch: cached, i.e. duplicate insert
                assert!(
                    old_round < anchor_round,
                    "uniqueness check out of rounds order"
                );

                // remove outdated, as we got updated
                if let Some(hashes) = self.round_to_hashes.get_mut(&old_round) {
                    let existed = hashes.remove(hash);
                    // if `hash_max_round` is up to date, then:
                    // * either value must be present in set
                    // * or value can be removed from set just once
                    assert!(
                        existed,
                        "value must be present in set of hashes for this round"
                    );
                    if hashes.is_empty() {
                        // set will be left empty until whole entry (by round) is removed
                        hashes.shrink_to_fit();
                    }
                } // else - nothing to update

                // in case an outdated value was stored - act as if it was already removed;
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
        // bottom round is the least kept in map
        let bottom_round = anchor_round.saturating_sub(self.round_threshold);
        let bottom_index = self
            .round_to_hashes
            .binary_search_keys(&bottom_round)
            .unwrap_or_else(std::convert::identity);
        let drained = self
            .round_to_hashes
            .drain((Bound::Unbounded, Bound::Excluded(bottom_index)));

        for (_, hashes) in drained {
            for hash in hashes {
                match self.hash_max_round.entry(hash) {
                    hash_map::Entry::Occupied(hash_max_round) => {
                        if *hash_max_round.get() < bottom_round {
                            hash_max_round.remove();
                        }
                    }
                    hash_map::Entry::Vacant(_) => {
                        panic!("map of round to hashes must not contain outdated values")
                    }
                }
            }
        }
        if self.round_to_hashes.capacity() > self.round_to_hashes.len().saturating_mul(4) {
            self.round_to_hashes
                .shrink_to(self.round_to_hashes.len().saturating_mul(2));
        }
        if self.hash_max_round.capacity() > self.hash_max_round.len().saturating_mul(4) {
            self.hash_max_round
                .shrink_to(self.hash_max_round.len().saturating_mul(2));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn dedup_externals_test() {
        const START_ROUND: u32 = 0;
        let threshold: u32 = 100;
        let mut round_id = START_ROUND;
        let first = [u8::MIN; 32];
        let second = [u8::MAX; 32];
        let vals = [first, second].iter().cloned().collect::<FastHashSet<_>>();

        let mut cache = Deduplicator::new(threshold as u16);
        assert!(
            cache.check_unique(round_id, &first),
            "first insert must be unique"
        );
        assert!(
            cache.check_unique(round_id, &second),
            "first insert must be unique"
        );

        for i in START_ROUND..=301 {
            round_id = i;

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
            assert_eq!(cache.hash_max_round.get(&first), Some(&round_id));
            assert_eq!(cache.hash_max_round.get(&second), Some(&round_id));

            assert_eq!(cache.round_to_hashes.last(), Some((&round_id, &vals)));

            assert!(cache
                .round_to_hashes
                .iter()
                .all(|(r, h)| *r == round_id || h.is_empty()));

            cache.clean(round_id);

            assert_eq!(
                cache.round_to_hashes.len(),
                round_id.min(threshold) as usize + 1
            );
        }

        round_id += threshold;
        assert!(
            !cache.check_unique(round_id, &first),
            "must be a duplicate insert within threshold"
        );
        cache.clean(round_id);
        assert_eq!(cache.hash_max_round.len(), 2);
        assert_eq!(cache.round_to_hashes.len(), 2);

        round_id += threshold + 1;
        assert!(
            cache.check_unique(round_id, &first),
            "must be a unique insert after threshold"
        );
        cache.clean(round_id);
        assert_eq!(cache.hash_max_round.len(), 1);
        assert_eq!(cache.round_to_hashes.len(), 1);
    }
}
