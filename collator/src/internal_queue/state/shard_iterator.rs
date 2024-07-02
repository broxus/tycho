use everscale_types::boc::Boc;
use everscale_types::cell::{Cell, HashBytes};
use everscale_types::models::ShardIdent;
use tycho_storage::owned_iterator::OwnedIterator;
use tycho_storage::ShardsInternalMessagesKey;

use crate::internal_queue::state::state_iterator::ShardRange;
use crate::internal_queue::types::InternalMessageKey;

pub struct ShardIterator {
    pub(crate) shard_ident: ShardIdent,
    shard_range: ShardRange,
    receiver: ShardIdent,
    // current_key: Option<InternalMessageKey>,
    pub(crate) iterator: OwnedIterator,
    // saved_position: Option<Vec<u8>>,
    // pub(crate) read_until: Option<InternalMessageKey>,
}

impl ShardIterator {
    pub fn new(
        shard_ident: ShardIdent,
        shard_range: ShardRange,
        receiver: ShardIdent,
        iterator: OwnedIterator,
    ) -> Self {
        let mut iterator = iterator;
        match shard_range.from.clone() {
            None => iterator.seek_to_first(),
            Some(range_from) => {
                iterator.seek(ShardsInternalMessagesKey {
                    shard_ident,
                    lt: range_from.lt,
                    hash: range_from.hash,
                });
            }
        }
        Self {
            shard_ident,
            shard_range: shard_range.clone(),
            receiver,
            // current_key: shard_range.from.clone(),
            iterator,
            // saved_position: None,
            // read_until: None,
        }
    }

    pub(crate) fn next_message(&mut self) -> Option<(ShardsInternalMessagesKey, Cell)> {
        let from_range = self.shard_range.clone().from.unwrap_or_default();
        let from = ShardsInternalMessagesKey {
            shard_ident: self.shard_ident,
            lt: from_range.lt,
            hash: from_range.hash,
        };

        let to_range = self.shard_range.to.clone().unwrap_or(InternalMessageKey {
            lt: u64::MAX,
            hash: HashBytes::default(),
        });

        let to = ShardsInternalMessagesKey {
            shard_ident: self.shard_ident,
            lt: to_range.lt,
            hash: to_range.hash,
        };

        let _receiver = self.receiver.clone();
        while let Some((key, value)) = self.load_next_message() {
            // let _dest_workchain = value[0] as i8;
            // let _dest_address = HashBytes::from_slice(&value[1..33]);

            if key <= from {
                self.iterator.next();
                continue;
            }

            if key > to {
                return None;
            }

            let cell = Boc::decode(&value[33..]).expect("failed to load cell");
            return Some((key, cell));

            // TODO check receiver here

            // if dest_workchain as i32 == receiver.workchain()
            //     && receiver.contains_account(&dest_address)
            // {
            //     let cell = Boc::decode(&value[33..]).expect("failed to load cell");
            //     return Some((key, cell));
            // } else {
            //     self.read_until = Some(InternalMessageKey {
            //         lt: key.lt,
            //         hash: key.hash,
            //     });
            //     self.iterator.next();
            // }
        }
        None
    }

    fn load_next_message(&mut self) -> Option<(ShardsInternalMessagesKey, &[u8])> {
        if let (Some(key), Some(value)) = (self.iterator.key(), self.iterator.value()) {
            let key = ShardsInternalMessagesKey::from(key);

            if key.shard_ident != self.shard_ident {
                return None;
            }

            return Some((key, value));
        }
        None
    }
}
