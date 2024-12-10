use std::collections::{btree_map, hash_map, BTreeMap};

use everscale_types::cell::HashBytes;
use everscale_types::models::{ExtInMsgInfo, IntMsgInfo, MsgInfo, ShardIdent};
use tycho_block_util::queue::QueueKey;
use tycho_util::{FastHashMap, FastHashSet};

use super::types::ParsedMessage;
use crate::mempool::MempoolAnchorId;

#[derive(Debug, Default, Clone, Copy)]
pub struct ExternalKey {
    pub anchor_id: MempoolAnchorId,
    pub msg_offset: u64,
}

#[derive(Default)]
pub(super) struct ReaderState {
    pub externals: ExternalsReaderState,
    pub internals: InternalsReaderState,
}

#[derive(Default)]
pub(super) struct ExternalsReaderState {
    /// Buffer to store external messages
    /// before collect them to the next execution group
    pub buffer: MessagesBuffer,

    pub range: ExternalsRangeIteratorState,
    pub processed_to: ExternalKey,
}

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct ExternalsRangeIteratorState {
    pub from: ExternalKey,
    pub to: ExternalKey,
    pub offset: u16,
}

#[derive(Default)]
pub(super) struct InternalsReaderState {
    pub partitions: BTreeMap<u8, PartitionReaderState>,
}

#[derive(Default)]
pub(super) struct PartitionReaderState {
    /// Buffer to store messages from partition
    /// before collect them to the next execution group
    pub buffer: MessagesBuffer,

    pub ranges: BTreeMap<u32, RangeReaderState>,
    pub processed_to: BTreeMap<ShardIdent, QueueKey>,
}

#[derive(Default)]
pub(super) struct RangeReaderState {
    /// Buffer to store messages from the next iterator
    /// for accounts that have messages in previous iterator
    /// until all messages from previous iterator not read
    pub buffer: MessagesBuffer,

    pub shards: BTreeMap<ShardIdent, ShardReaderState>,
    pub offset: u16,

    pub remaning_msgs_stats: FastHashMap<HashBytes, usize>,
}

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct ShardReaderState {
    pub from: u64,
    pub to: u64,
    pub current_position: QueueKey,
}

#[derive(Default)]
pub(super) struct MessagesBuffer {
    #[allow(clippy::vec_box)]
    messages: FastHashMap<HashBytes, Vec<Box<ParsedMessage>>>,
    sorted_index: BTreeMap<usize, FastHashSet<HashBytes>>,
    messages_count: usize,
}

impl MessagesBuffer {
    pub fn sorted_index(&self) -> &BTreeMap<usize, FastHashSet<HashBytes>> {
        &self.sorted_index
    }

    pub fn account_messages_count(&self, account_id: &HashBytes) -> usize {
        self.messages
            .get(account_id)
            .map(|msgs| msgs.len())
            .unwrap_or_default()
    }

    pub fn messages_count(&self) -> usize {
        self.messages_count
    }

    pub fn add_message(&mut self, msg: Box<ParsedMessage>) {
        assert_eq!(
            msg.special_origin, None,
            "unexpected special origin in ordinary messages set"
        );

        let account_id = match &msg.info {
            MsgInfo::ExtIn(ExtInMsgInfo { dst, .. }) | MsgInfo::Int(IntMsgInfo { dst, .. }) => {
                dst.as_std().map(|a| a.address).unwrap_or_default()
            }
            MsgInfo::ExtOut(info) => {
                unreachable!("ext out message in ordinary messages set: {info:?}")
            }
        };

        // insert message to buffer
        let prev_msgs_count = match self.messages.entry(account_id) {
            hash_map::Entry::Vacant(vacant) => {
                vacant.insert(vec![msg]);
                0
            }
            hash_map::Entry::Occupied(mut occupied) => {
                let entry = occupied.get_mut();
                let prev_count = entry.len();
                entry.push(msg);
                prev_count
            }
        };
        self.messages_count += 1;

        // update sorted by count index
        // remove account_id from previous count bracket
        if prev_msgs_count > 0 {
            let accounts = self.sorted_index.get_mut(&prev_msgs_count).unwrap();
            accounts.remove(&account_id);
        }
        // add account_id to the next count bracket
        match self.sorted_index.entry(prev_msgs_count + 1) {
            btree_map::Entry::Vacant(vacant) => {
                vacant.insert([account_id].into_iter().collect());
            }
            btree_map::Entry::Occupied(mut occupied) => {
                occupied.get_mut().insert(account_id);
            }
        }
    }
}
