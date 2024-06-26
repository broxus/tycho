use std::sync::Arc;

use anyhow::{bail, Result};
use everscale_types::boc::Boc;
use everscale_types::cell::{Cell, Load};
use everscale_types::models::{IntMsgInfo, Message, MsgInfo, ShardIdent};
use everscale_types::prelude::HashBytes;
use tycho_storage::owned_iterator::OwnedIterator;
use tycho_storage::StorageInternalMessageKey;
use tycho_util::FastHashMap;

use crate::internal_queue::state::state_iterator::{MessageWithSource, ShardRange, StateIterator};
use crate::internal_queue::types::{EnqueuedMessage, InternalMessageKey};
use crate::types::IntAdrExt;

pub struct PersistentStateIterator {
    iter: OwnedIterator,
    receiver: ShardIdent,
    ranges: FastHashMap<ShardIdent, ShardRange>,
}

impl PersistentStateIterator {
    pub fn new(
        iter: OwnedIterator,
        receiver: ShardIdent,
        ranges: FastHashMap<ShardIdent, ShardRange>,
    ) -> Self {
        Self {
            iter,
            receiver,
            ranges,
        }
    }

    fn load_message_from_value(value: &[u8]) -> Result<(IntMsgInfo, Cell)> {
        let cell = Boc::decode(value)?;
        let message = Message::load_from(&mut cell.as_slice().unwrap())?;

        match message.info {
            MsgInfo::Int(info) => Ok((info, cell)),
            _ => bail!("Expected internal message"),
        }
    }

    fn create_message_with_source(
        info: IntMsgInfo,
        cell: Cell,
        shard: ShardIdent,
    ) -> Arc<MessageWithSource> {
        let hash = *cell.repr_hash();
        let enqueued_message = EnqueuedMessage { info, cell, hash };
        Arc::new(MessageWithSource::new(shard, Arc::new(enqueued_message)))
    }
}

impl PersistentStateIterator {
    fn save_position(&self) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
        (
            self.iter.key().map(|k| k.to_vec()),
            self.iter.value().map(|v| v.to_vec()),
        )
    }

    fn restore_position(&mut self, position: (Option<Vec<u8>>, Option<Vec<u8>>)) {
        if let (Some(key), Some(_value)) = position {
            self.iter.seek_by_u8(&key);
            if self.iter.key().is_some() && self.iter.value().is_some() {
                return;
            }
        }
        self.iter.seek_to_first();
    }
}

impl StateIterator for PersistentStateIterator {
    fn seek(&mut self, range_start: Option<(&ShardIdent, InternalMessageKey)>) {
        let time_start = std::time::Instant::now();
        match range_start {
            Some((shard_ident, key)) => {
                let start = StorageInternalMessageKey {
                    shard_ident: *shard_ident,
                    dest_address: HashBytes::default(),
                    lt: key.lt,
                    hash: key.hash,
                    dest_workchain: i8::MIN,
                };
                self.iter.seek(start);
            }
            None => self.iter.seek_to_first(),
        };
        tracing::trace!(
            target: "local_debug",
            elapsed = %humantime::format_duration(time_start.elapsed()),
            "Seeked to the start of the range"
        );
    }

    fn next(&mut self) -> Result<Option<Arc<MessageWithSource>>> {
        let mut skip_count = 0;
        let mut skip_count_not_for_us = 0;
        let mut skip_count_processed = 0;

        while let (Some(key), Some(value)) = (self.iter.key(), self.iter.value()) {
            let key = StorageInternalMessageKey::from(key);

            let range = self.ranges.get(&key.shard_ident);

            if let Some(range) = range {
                let int_key = InternalMessageKey {
                    lt: key.lt,
                    hash: key.hash,
                };
                if range.from.clone().unwrap_or_default() >= int_key {
                    self.iter.next();
                    // skip_count += 1;
                    skip_count_processed += 1;
                    continue;
                }

                if range.to.clone().unwrap_or(InternalMessageKey::MAX) < int_key {
                    break;
                }
            } else {
                self.iter.next();
                skip_count += 1;
                continue;
            }

            // if self.receiver.contains_account(&key.dest_address)
            //     && self.receiver.workchain() == key.dest_workchain as i32
            // {
            let (info, cell) = Self::load_message_from_value(value)?;
            let message_with_source = Self::create_message_with_source(info, cell, key.shard_ident);

            if skip_count_not_for_us > 0 || skip_count_processed > 0 || skip_count > 0 {
                tracing::info!(target: "local_debug", "Skipped items before finding a valid message. skip_count_not_for_us {skip_count_not_for_us:?}. skip_count_processed {skip_count_processed:?}. not in range {skip_count:?}. ranges {:?} receiver {:?}", self.ranges, self.receiver);
            }

            self.iter.next();
            return Ok(Some(message_with_source));
            // } else {
            //     self.iter.next();
            //     skip_count_not_for_us += 1;
            // }
        }

        if skip_count_not_for_us > 0 || skip_count_processed > 0 || skip_count > 0 {
            tracing::info!(target: "local_debug", "Total Skipped items before finding a valid message. skip_count_not_for_us {skip_count_not_for_us:?}. skip_count_processed {skip_count_processed:?}. not in range {skip_count:?}. ranges {:?} receiver {:?}", self.ranges, self.receiver);
        }

        Ok(None)
    }

    fn peek(&mut self) -> Result<Option<Arc<MessageWithSource>>> {
        let saved_position = self.save_position();
        let mut skip_count = 0;
        let mut skip_count_not_for_us = 0;
        let mut skip_count_processed = 0;

        while let (Some(key), Some(value)) = (self.iter.key(), self.iter.value()) {
            let key = StorageInternalMessageKey::from(key);

            let range = self.ranges.get(&key.shard_ident);

            if let Some(range) = range {
                let int_key = InternalMessageKey {
                    lt: key.lt,
                    hash: key.hash,
                };
                if range.from.clone().unwrap_or_default() >= int_key {
                    self.iter.next();
                    skip_count_processed += 1;
                    continue;
                }

                if range.to.clone().unwrap_or(InternalMessageKey::MAX) < int_key {
                    break;
                }
            } else {
                self.iter.next();
                skip_count += 1;
                continue;
            }

            let (info, cell) = Self::load_message_from_value(value)?;

            if self.receiver.contains_account(&info.dst.get_address())
                && self.receiver.workchain() == info.dst.workchain()
            {
                let message_with_source =
                    Self::create_message_with_source(info, cell, key.shard_ident);

                self.restore_position(saved_position);

                if skip_count_not_for_us > 0 || skip_count_processed > 0 || skip_count > 0 {
                    tracing::info!(target: "local_debug", "Peek Skipped items before finding a valid message. skip_count_not_for_us {skip_count_not_for_us:?}. skip_count_processed {skip_count_processed:?}. not in range {skip_count:?}. ranges {:?} receiver {:?}", self.ranges, self.receiver);
                }

                return Ok(Some(message_with_source));
            } else {
                self.iter.next();
                skip_count_not_for_us += 1;
            }
        }

        self.restore_position(saved_position);

        if skip_count_not_for_us > 0 || skip_count_processed > 0 || skip_count > 0 {
            tracing::info!(target: "local_debug", "Total peek Skipped items before finding a valid message. skip_count_not_for_us {skip_count_not_for_us:?}. skip_count_processed {skip_count_processed:?}. not in range {skip_count:?}. ranges {:?} receiver {:?}", self.ranges, self.receiver);
        }

        Ok(None)
    }
}
