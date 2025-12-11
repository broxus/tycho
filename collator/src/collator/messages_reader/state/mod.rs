use std::collections::BTreeMap;

use anyhow::anyhow;
use tycho_block_util::queue::QueueKey;

use crate::collator::messages_reader::state::external::{
    ExternalsPartitionReaderState, ExternalsRangeReaderState, ExternalsReaderRange,
    ExternalsReaderState,
};
use crate::collator::messages_reader::state::internal::InternalsReaderState;
use crate::types::processed_upto::{
    ExternalsProcessedUptoStuff, Lt, ProcessedUptoInfoStuff, ProcessedUptoPartitionStuff,
    ShardRangeInfo,
};

pub mod external;
pub mod internal;

#[derive(Default)]
pub struct ReaderState {
    pub externals: ExternalsReaderState,
    pub internals: InternalsReaderState,
}

impl ReaderState {
    pub fn new(processed_upto: &ProcessedUptoInfoStuff) -> Self {
        let mut ext_reader_state = ExternalsReaderState::default();
        for (par_id, par) in &processed_upto.partitions {
            let processed_to = par.externals.processed_to.into();
            ext_reader_state
                .by_partitions
                .insert(*par_id, ExternalsPartitionReaderState {
                    processed_to,
                    curr_processed_offset: 0,
                });
            for (seqno, range_info) in &par.externals.ranges {
                ext_reader_state
                    .ranges
                    .entry(*seqno)
                    .and_modify(|r| {
                        r.by_partitions.insert(*par_id, range_info.into());
                    })
                    .or_insert(ExternalsRangeReaderState {
                        range: ExternalsReaderRange::from_range_info(range_info, processed_to),
                        by_partitions: [(*par_id, range_info.into())].into(),
                        fully_read: false,
                    });
            }
        }
        Self {
            internals: InternalsReaderState {
                partitions: processed_upto
                    .partitions
                    .iter()
                    .map(|(k, v)| (*k, (&v.internals).into()))
                    .collect(),
                cumulative_statistics: None,
            },
            externals: ext_reader_state,
        }
    }

    pub fn get_updated_processed_upto(&self) -> ProcessedUptoInfoStuff {
        let mut processed_upto = ProcessedUptoInfoStuff::default();
        for (par_id, par) in &self.internals.partitions {
            let ext_reader_state_by_partition =
                self.externals.get_state_by_partition(*par_id).unwrap();
            processed_upto
                .partitions
                .insert(*par_id, ProcessedUptoPartitionStuff {
                    externals: ExternalsProcessedUptoStuff {
                        processed_to: ext_reader_state_by_partition.processed_to.into(),
                        ranges: self
                            .externals
                            .ranges
                            .iter()
                            .map(|(k, v)| {
                                let ext_range_reader_state_by_partition =
                                    v.get_state_by_partition(*par_id).unwrap();
                                (*k, (&v.range, ext_range_reader_state_by_partition).into())
                            })
                            .collect(),
                    },
                    internals: par.into(),
                });
        }
        processed_upto
    }

    pub fn check_has_non_zero_processed_offset(&self) -> bool {
        let check_internals = self
            .internals
            .partitions
            .values()
            .any(|par| par.ranges.values().any(|r| r.processed_offset > 0));
        if check_internals {
            return check_internals;
        }

        self.externals
            .ranges
            .values()
            .any(|r| r.by_partitions.values().any(|par| par.processed_offset > 0))
    }

    pub fn has_messages_in_buffers(&self) -> bool {
        self.has_internals_in_buffers() || self.has_externals_in_buffers()
    }

    pub fn has_internals_in_buffers(&self) -> bool {
        self.internals
            .partitions
            .values()
            .any(|par| par.ranges.values().any(|r| r.buffer.msgs_count() > 0))
    }

    pub fn has_externals_in_buffers(&self) -> bool {
        self.externals.ranges.values().any(|r| {
            r.by_partitions
                .values()
                .any(|par| par.buffer.msgs_count() > 0)
        })
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ShardReaderState {
    pub from: Lt,
    pub to: Lt,
    pub current_position: QueueKey,
}

impl ShardReaderState {
    pub fn from_range_info(range_info: &ShardRangeInfo, processed_to: QueueKey) -> Self {
        let current_position = if processed_to.lt < range_info.from {
            QueueKey::max_for_lt(range_info.from)
        } else if processed_to.lt < range_info.to {
            processed_to
        } else {
            QueueKey::max_for_lt(range_info.to)
        };
        Self {
            from: range_info.from,
            to: range_info.to,
            current_position,
        }
    }

    pub fn is_fully_read(&self) -> bool {
        self.current_position >= QueueKey::max_for_lt(self.to)
    }

    pub fn set_fully_read(&mut self) {
        self.current_position = QueueKey::max_for_lt(self.to);
    }
}

struct DisplayShardReaderState<'a>(pub &'a ShardReaderState);

impl std::fmt::Debug for DisplayShardReaderState<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self, f)
    }
}

impl std::fmt::Display for DisplayShardReaderState<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("")
            .field("from", &self.0.from)
            .field("to", &self.0.to)
            .field("current_position", &self.0.current_position)
            .finish()
    }
}

pub fn with_prev_map_and_current<K, V, F, R>(
    map: &mut BTreeMap<K, V>,
    key: K,
    f: F,
) -> anyhow::Result<R>
where
    K: Ord + Clone + std::fmt::Debug,
    F: for<'s> FnOnce(&'s BTreeMap<K, V>, &'s mut V) -> anyhow::Result<R>,
{
    struct SplitGuard<'m, K: Ord + Clone, V> {
        left: &'m mut BTreeMap<K, V>,
        right: BTreeMap<K, V>,
        key: K,
        current: Option<V>,
    }

    impl<'m, K: Ord + Clone, V> Drop for SplitGuard<'m, K, V> {
        fn drop(&mut self) {
            if let Some(current) = self.current.take() {
                self.left.insert(self.key.clone(), current);
            }
            self.left.append(&mut self.right);
        }
    }

    let left = map;
    let mut right = left.split_off(&key);

    let current = right
        .remove(&key)
        .ok_or_else(|| anyhow!("current state not found: {:?}", key))?;

    let mut guard = SplitGuard {
        left,
        right,
        key,
        current: Some(current),
    };

    let prev_map: &BTreeMap<K, V> = &*guard.left;
    let current_state: &mut V = guard.current.as_mut().unwrap();

    f(prev_map, current_state)
}

pub fn with_prev_list_and_current<K, V, F, R>(
    map: &mut BTreeMap<K, V>,
    key: K,
    prev_keys: &[K],
    f: F,
) -> anyhow::Result<R>
where
    K: Ord + Clone + Copy + std::fmt::Debug,
    F: for<'s> FnOnce(Vec<&'s V>, &'s mut V) -> anyhow::Result<R>,
{
    with_prev_map_and_current(map, key, |prev_map, current| {
        let mut prev = Vec::with_capacity(prev_keys.len());
        for k in prev_keys {
            debug_assert!(
                k < &key,
                "prev key must be < current key: prev={k:?} curr={key:?}"
            );
            let st = prev_map
                .get(k)
                .ok_or_else(|| anyhow!("state: {:?} not found", k))?;
            prev.push(st);
        }
        f(prev, current)
    })
}
