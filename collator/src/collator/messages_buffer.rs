use std::collections::{BTreeMap, VecDeque, btree_map};

use rayon::iter::IntoParallelIterator;
use tycho_block_util::queue::QueueKey;
use tycho_types::cell::HashBytes;
use tycho_types::models::{ExtInMsgInfo, IntMsgInfo, MsgInfo};
use tycho_util::{FastHashMap, FastHashSet};

use super::types::ParsedMessage;
use crate::types::{DebugIter, DisplayIter, SaturatingAddAssign, Transactional};

#[cfg(test)]
#[path = "tests/messages_buffer_tests.rs"]
pub(super) mod tests;

type SlotId = u16;

type FastIndexMap<K, V> = indexmap::IndexMap<K, V, ahash::RandomState>;
pub type FastIndexSet<K> = indexmap::IndexSet<K, ahash::RandomState>;

#[derive(Default, Clone)]
struct AccountEntry {
    old_val: Option<VecDeque<ParsedMessage>>,
    new_val: Option<VecDeque<ParsedMessage>>,
    is_new: bool,
}

impl AccountEntry {
    fn current(&self) -> Option<&VecDeque<ParsedMessage>> {
        self.new_val.as_ref()
    }

    fn current_mut(&mut self) -> Option<&mut VecDeque<ParsedMessage>> {
        self.new_val.as_mut()
    }
}

#[derive(Clone, Copy)]
pub struct MessagesBufferLimits {
    pub max_count: usize,
    pub slots_count: usize,
    pub slot_vert_size: usize,
}

#[derive(Default)]
pub struct BufferTransaction {
    int_count: usize,
    ext_count: usize,
    min_ext_chain_time: Option<u64>,
}

#[derive(Default)]
pub struct MessagesBuffer {
    msgs: FastIndexMap<HashBytes, AccountEntry>,
    int_count: usize,
    ext_count: usize,
    min_ext_chain_time: Option<u64>,
    tx: Option<BufferTransaction>,
}

impl Transactional for MessagesBuffer {
    fn begin(&mut self) {
        self.tx = Some(BufferTransaction {
            int_count: self.int_count,
            ext_count: self.ext_count,
            min_ext_chain_time: self.min_ext_chain_time,
        });
    }

    fn commit(&mut self) {
        self.tx = None;
        self.msgs.retain(|_, entry| {
            entry.old_val = None;
            entry.is_new = false;
            entry.new_val.is_some()
        });
    }

    fn rollback(&mut self) {
        let Some(snapshot) = self.tx.take() else {
            return;
        };

        self.msgs.retain(|_, entry| {
            if entry.is_new {
                return false;
            }
            if entry.old_val.is_some() {
                entry.new_val = entry.old_val.take();
            }
            entry.new_val.is_some()
        });

        self.int_count = snapshot.int_count;
        self.ext_count = snapshot.ext_count;
        self.min_ext_chain_time = snapshot.min_ext_chain_time;
    }

    fn is_in_transaction(&self) -> bool {
        self.tx.is_some()
    }
}

impl MessagesBuffer {
    fn backup_account(&mut self, account_id: &HashBytes) {
        if self.is_in_transaction() {
            if let Some(entry) = self.msgs.get_mut(account_id) {
                if entry.old_val.is_none() {
                    entry.old_val = entry.new_val.clone();
                }
            }
        }
    }

    pub fn account_messages_count(&self, account_id: &HashBytes) -> usize {
        self.msgs
            .get(account_id)
            .and_then(|entry| entry.current())
            .map(|msgs| msgs.len())
            .unwrap_or_default()
    }

    pub fn msgs_count(&self) -> usize {
        self.int_count + self.ext_count
    }

    fn update_min_ext_chain_time(&mut self, ext_chain_time: Option<u64>) {
        if let Some(ext_ct) = ext_chain_time
            && (matches!(self.min_ext_chain_time, Some(min_ct) if ext_ct < min_ct)
                || self.min_ext_chain_time.is_none())
        {
            self.min_ext_chain_time = Some(ext_ct);
        }
    }

    pub fn min_ext_chain_time(&self) -> u64 {
        self.min_ext_chain_time.unwrap_or(u64::MAX)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&HashBytes, &VecDeque<ParsedMessage>)> {
        self.msgs
            .iter()
            .filter_map(|(k, entry)| entry.current().map(|v| (k, v)))
    }

    pub fn add_message(&mut self, msg: ParsedMessage) {
        assert_eq!(
            msg.special_origin(),
            None,
            "unexpected special origin in ordinary messages set"
        );

        let dst = match &msg.info() {
            MsgInfo::Int(IntMsgInfo { dst, .. }) => {
                self.int_count += 1;
                dst
            }
            MsgInfo::ExtIn(ExtInMsgInfo { dst, .. }) => {
                self.ext_count += 1;
                self.update_min_ext_chain_time(msg.ext_msg_chain_time());
                dst
            }
            MsgInfo::ExtOut(info) => {
                unreachable!("ext out message in ordinary messages set: {info:?}")
            }
        };
        let account_id = dst.as_std().map(|a| a.address).unwrap_or_default();

        self.backup_account(&account_id);

        match self.msgs.entry(account_id) {
            indexmap::map::Entry::Vacant(vacant) => {
                vacant.insert(AccountEntry {
                    old_val: None,
                    new_val: Some([msg].into()),
                    is_new: self.tx.is_some(),
                });
            }
            indexmap::map::Entry::Occupied(mut occupied) => {
                let entry = occupied.get_mut();
                if let Some(msgs) = entry.current_mut() {
                    msgs.push_back(msg);
                } else {
                    entry.new_val = Some([msg].into());
                }
            }
        };
    }

    pub fn remove_int_messages_by_accounts(
        &mut self,
        addresses_to_remove: &FastHashSet<HashBytes>,
    ) {
        for addr in addresses_to_remove {
            self.backup_account(addr);

            if let Some(entry) = self.msgs.get_mut(addr) {
                if let Some(msgs) = entry.current() {
                    self.int_count -= msgs.len();
                }
                entry.new_val = None;
            }
        }
    }

    /// Returns queue keys of collected internal queue messages.
    pub fn fill_message_group<FA, FM>(
        &mut self,
        msg_group: &mut MessageGroup,
        slots_count: usize,
        slot_vert_size: usize,
        already_skipped_accounts: &mut FastHashSet<HashBytes>,
        check_skip_account: FA,
        mut msg_filter: FM,
    ) -> FillMessageGroupResult
    where
        FA: Fn(&HashBytes) -> (bool, u64),
        FM: MessageFilter,
    {
        // evaluate ops count for wu calculation
        let mut ops_count = 0;

        // take slots info from group
        let mut slots_info = std::mem::take(&mut msg_group.slots_info);

        // pre-allocate index by msgs count size
        slots_info.index_by_msgs_count.presize(slot_vert_size);

        // we will collect updates for slots index and apply them at the end
        let mut slots_index_updates = BTreeMap::new();

        // track collected messages
        let mut collected_int_msgs = vec![];
        let mut collected_count = 0;

        // will iterate over accounts in buffer
        let mut buf_account_idx = 0;

        // track already skipped accounts to avoid re-checking them
        let mut check_skip_account_ext = |account_id: &HashBytes, ops_count: &mut u64| {
            if already_skipped_accounts.contains(account_id) {
                ops_count.saturating_add_assign(1);
                true
            } else {
                let (skip_account, check_ops_count) = check_skip_account(account_id);
                ops_count.saturating_add_assign(check_ops_count);
                if skip_account {
                    already_skipped_accounts.insert(*account_id);
                }
                skip_account
            }
        };

        // 1. try to fill remaning slots with messages of accounts which are not included in any slot
        let mut new_used_slots = FastHashSet::<SlotId>::default();
        let mut remaning_slots_count = slots_count.saturating_sub(slots_info.slots.len());
        if remaning_slots_count > 0 {
            // define next empty slot
            let last_slot_id = slots_info.last_slot_id.unwrap_or_default();
            let mut slot_id = last_slot_id + 1;
            remaning_slots_count -= 1;

            let mut not_filled_slots = VecDeque::<SlotId>::default();
            loop {
                // update last slot id
                slots_info.last_slot_id = slots_info
                    .last_slot_id
                    .map(|last_slot_id| last_slot_id.max(slot_id))
                    .or(Some(slot_id));

                // get slot
                let slot = slots_info.slots.entry(slot_id).or_default();
                let mut slot_cx = SlotContext {
                    remaning_capacity: slot_vert_size - slot.msgs_count(),
                    old_int_count: slot.int_count,
                    old_ext_count: slot.ext_count,
                    slot,
                    slot_id,
                };

                // try to get messages of other accounts which are not included in any slot
                while let Some((&account_id, entry)) = self.msgs.get_index(buf_account_idx) {
                    buf_account_idx += 1;

                    // skip removed accounts or accounts without messages
                    let Some(account_msgs) = entry.current() else {
                        continue;
                    };
                    if account_msgs.is_empty() {
                        continue;
                    }

                    if msg_group.msgs.contains_key(&account_id) {
                        // try skip messages from skipped account: maybe they expired
                        self.try_skip_account_msgs(&account_id, &mut msg_filter);

                        continue;
                    }

                    // skip accounts that do not pass the provided check
                    if check_skip_account_ext(&account_id, &mut ops_count) {
                        // try skip messages from skipped account: maybe they expired
                        self.try_skip_account_msgs(&account_id, &mut msg_filter);

                        continue;
                    }

                    let move_res = self.move_account_messages_to_slot(
                        account_id,
                        msg_group,
                        &mut slot_cx,
                        &mut collected_int_msgs,
                        &mut slots_index_updates,
                        &mut msg_filter,
                    );
                    ops_count.saturating_add_assign(move_res.ops_count);
                    collected_count.saturating_add_assign(move_res.collected_count);

                    if slot_cx.slot.msgs_count() > 0 {
                        if slot_cx.remaning_capacity > 0 {
                            not_filled_slots.push_back(slot_id);
                        }

                        break;
                    }
                }

                // if slot still is empty then we unable to fill next slots
                if slot_cx.slot.msgs_count() == 0 {
                    // remove empty slot
                    slots_info.slots.remove(&slot_id);
                    break;
                } else {
                    new_used_slots.insert(slot_id);
                }

                // define next slot
                if remaning_slots_count > 0 {
                    // will get next empty slot
                    slot_id += 1;
                    remaning_slots_count -= 1;
                } else if let Some(not_filled_slot_id) = not_filled_slots.pop_front() {
                    // will get not fully filled slot
                    slot_id = not_filled_slot_id;
                } else {
                    // empty and not fully filled slots do not exist
                    // stop filling message group
                    break;
                }
            }
        }

        // 2. try to fill all slots up to limit
        // including slots which were not fully filled
        // in message group before the previous step
        // iterate buckets with count < slot_vert_size
        for slot_ids in slots_info.index_by_msgs_count.iter_upto(slot_vert_size) {
            for slot_id in slot_ids {
                // skip slot that was used in the previous step
                // we already looked up thru whole buffer to fill them
                // so we unable to fully fill them
                if new_used_slots.contains(slot_id) {
                    continue;
                }

                let slot = slots_info
                    .slots
                    .get_mut(slot_id)
                    .expect("slot should exist because it is present in the index");

                let mut slot_cx = SlotContext {
                    remaning_capacity: slot_vert_size - slot.msgs_count(),
                    old_int_count: slot.int_count,
                    old_ext_count: slot.ext_count,
                    slot,
                    slot_id: *slot_id,
                };

                // try to get messages of accounts which are already included in slot
                for i in 0..slot_cx.slot.accounts.len() {
                    let account_id = slot_cx.slot.accounts[i];

                    // skip accounts that do not pass the provided check
                    if check_skip_account_ext(&account_id, &mut ops_count) {
                        // try skip messages from skipped account: maybe they expired
                        self.try_skip_account_msgs(&account_id, &mut msg_filter);

                        continue;
                    }

                    let move_res = self.move_account_messages_to_slot(
                        account_id,
                        msg_group,
                        &mut slot_cx,
                        &mut collected_int_msgs,
                        &mut slots_index_updates,
                        &mut msg_filter,
                    );
                    ops_count.saturating_add_assign(move_res.ops_count);
                    collected_count.saturating_add_assign(move_res.collected_count);

                    if slot_cx.remaning_capacity == 0 {
                        break;
                    }
                }

                // go to next slot if current is full
                if slot_cx.remaning_capacity == 0 {
                    continue;
                }

                // then try to get messages of other accounts which are not included in any slot
                while let Some((&account_id, entry)) = self.msgs.get_index(buf_account_idx) {
                    buf_account_idx += 1;

                    // skip removed accounts or accounts without messages
                    let Some(account_msgs) = entry.current() else {
                        continue;
                    };
                    if account_msgs.is_empty() {
                        continue;
                    }

                    if msg_group.msgs.contains_key(&account_id) {
                        // try skip messages from skipped account: maybe they expired
                        self.try_skip_account_msgs(&account_id, &mut msg_filter);

                        continue;
                    }

                    // skip accounts that do not pass the provided check
                    if check_skip_account_ext(&account_id, &mut ops_count) {
                        // try skip messages from skipped account: maybe they expired
                        self.try_skip_account_msgs(&account_id, &mut msg_filter);

                        continue;
                    }

                    let move_res = self.move_account_messages_to_slot(
                        account_id,
                        msg_group,
                        &mut slot_cx,
                        &mut collected_int_msgs,
                        &mut slots_index_updates,
                        &mut msg_filter,
                    );
                    ops_count.saturating_add_assign(move_res.ops_count);
                    collected_count.saturating_add_assign(move_res.collected_count);

                    if slot_cx.remaning_capacity == 0 {
                        break;
                    }
                }
            }
        }

        // check and skip messages in remaning accounts if required
        if msg_filter.can_skip() {
            while let Some((&account_id, entry)) = self.msgs.get_index(buf_account_idx) {
                buf_account_idx += 1;

                // skip removed accounts or accounts without messages
                let Some(account_msgs) = entry.current() else {
                    continue;
                };
                if account_msgs.is_empty() {
                    continue;
                }

                self.try_skip_account_msgs(&account_id, &mut msg_filter);
            }
        }

        // remove empty accounts from buffer
        ops_count.saturating_add_assign(self.msgs.len() as u64);

        if !self.is_in_transaction() {
            self.msgs
                .retain(|_, entry| entry.current().map(|v| !v.is_empty()).unwrap_or(false));
        }

        // drop min externals chain time if buffer was drained
        if self.msgs.is_empty() {
            self.min_ext_chain_time = None;
        }

        // 3. update slots index
        let mut remove_slot_ids = BTreeMap::<usize, FastHashSet<u16>>::new();
        for (slot_id, index_update) in slots_index_updates {
            // remove slot from old count bucket
            if index_update.old_count() != 0 {
                remove_slot_ids
                    .entry(index_update.old_count())
                    .and_modify(|to_remove| {
                        to_remove.insert(slot_id);
                    })
                    .or_default();
                slots_info.int_count -= index_update.old_int_count;
                slots_info.ext_count -= index_update.old_ext_count;
            }

            // add slot to a new count bucket
            let slots_ids = slots_info
                .index_by_msgs_count
                .get_mut_or_grow(index_update.new_count());
            slots_ids.insert(slot_id);
            slots_info.int_count += index_update.new_int_count;
            slots_info.ext_count += index_update.new_ext_count;
        }
        for (old_count, to_remove) in remove_slot_ids {
            if slots_info.index_by_msgs_count.len() <= old_count {
                continue;
            }
            let slots_ids = &mut slots_info.index_by_msgs_count[old_count];
            slots_ids.retain(|id| !to_remove.contains(id));
        }

        // put slots info into group
        std::mem::swap(&mut slots_info, &mut msg_group.slots_info);

        FillMessageGroupResult {
            collected_int_msgs,
            collected_count,
            ops_count,
        }
    }

    fn move_account_messages_to_slot<FM>(
        &mut self,
        account_id: HashBytes,
        msg_group: &mut MessageGroup,
        slot_cx: &mut SlotContext<'_>,
        collected_int_msgs: &mut Vec<QueueKey>,
        slots_index_updates: &mut BTreeMap<SlotId, SlotIndexUpdate>,
        mut msg_filter: FM,
    ) -> MoveMessagesResult
    where
        FM: MessageFilter,
    {
        let mut res = MoveMessagesResult::default();

        let mut amount = 0;

        let mut int_collected_count = 0;
        let mut ext_collected_count = 0;

        let mut ext_skipped_count = 0;

        let mut should_add_account_to_slot_index = false;
        self.backup_account(&account_id);

        if let Some(entry) = self.msgs.get_mut(&account_id) {
            let Some(account_msgs) = entry.current_mut() else {
                return res;
            };

            res.ops_count.saturating_add_assign(1);

            amount = account_msgs.len().min(slot_cx.remaning_capacity);

            if amount == 0 {
                return res;
            }

            let slot_account_msgs = msg_group.msgs.entry(account_id).or_default();
            res.ops_count.saturating_add_assign(1);

            should_add_account_to_slot_index = slot_account_msgs.is_empty();

            slot_account_msgs.reserve(amount);

            while int_collected_count + ext_collected_count < amount {
                let Some(msg) = account_msgs.pop_front() else {
                    break;
                };

                res.ops_count.saturating_add_assign(1);

                // check and skip message if required
                if msg_filter.should_skip(&msg) {
                    debug_assert!(
                        msg.info().is_external_in(),
                        "we should only skip extenal messages"
                    );
                    ext_skipped_count += 1;
                    continue;
                }

                // collect message if it was not skipped
                match &msg.info() {
                    MsgInfo::Int(info) => {
                        collected_int_msgs.push(QueueKey {
                            lt: info.created_lt,
                            hash: *msg.cell().repr_hash(),
                        });
                        int_collected_count += 1;
                    }
                    MsgInfo::ExtIn(_) => {
                        ext_collected_count += 1;
                    }
                    MsgInfo::ExtOut(_) => unreachable!("must contain only Int and ExtIn messages"),
                }
                slot_account_msgs.push(msg);
            }

            if slot_account_msgs.is_empty() {
                msg_group.msgs.remove(&account_id);
                res.ops_count.saturating_add_assign(1);
            }
        }

        if amount == 0 {
            return res;
        }

        // update buffer msgs counter by skipped messages
        self.ext_count -= ext_skipped_count;

        res.collected_count = int_collected_count + ext_collected_count;
        if res.collected_count == 0 {
            return res;
        }

        // update buffer msgs counter by collected messages
        self.int_count -= int_collected_count;
        self.ext_count -= ext_collected_count;

        // update slot msgs counter
        slot_cx.slot.int_count += int_collected_count;
        slot_cx.slot.ext_count += ext_collected_count;

        // calc remaining capacity
        let collected_count = int_collected_count + ext_collected_count;
        slot_cx.remaning_capacity -= collected_count;

        // add account to slot index if any message collected
        if should_add_account_to_slot_index && collected_count > 0 {
            slot_cx.slot.accounts.push(account_id);
        }

        // collect slot index updates
        slots_index_updates
            .entry(slot_cx.slot_id)
            .and_modify(|index_update| {
                index_update.new_int_count = slot_cx.slot.int_count;
                index_update.new_ext_count = slot_cx.slot.ext_count;
            })
            .or_insert(SlotIndexUpdate {
                old_int_count: slot_cx.old_int_count,
                old_ext_count: slot_cx.old_ext_count,
                new_int_count: slot_cx.slot.int_count,
                new_ext_count: slot_cx.slot.ext_count,
            });

        res
    }

    fn try_skip_account_msgs<FM>(&mut self, account_id: &HashBytes, mut filter: FM)
    where
        FM: MessageFilter,
    {
        if !filter.can_skip() {
            return;
        }
        self.backup_account(account_id);

        if let Some(entry) = self.msgs.get_mut(account_id) {
            if let Some(account_msgs) = entry.current_mut() {
                account_msgs.retain(|msg| {
                    let skip = filter.should_skip(msg);
                    debug_assert!(
                        !skip || msg.info().is_external_in(),
                        "we should only skip external messages"
                    );
                    self.ext_count -= skip as usize;
                    !skip
                });
            }
        }
    }
}

#[derive(Default)]
pub struct FillMessageGroupResult {
    pub collected_int_msgs: Vec<QueueKey>,
    pub collected_count: usize,
    pub ops_count: u64,
}

#[derive(Default)]
pub struct MoveMessagesResult {
    pub collected_count: usize,
    pub ops_count: u64,
}

pub trait MessageFilter {
    fn can_skip(&self) -> bool;
    fn should_skip(&mut self, msg: &ParsedMessage) -> bool;
}

impl<T: MessageFilter + ?Sized> MessageFilter for &mut T {
    #[inline]
    fn can_skip(&self) -> bool {
        T::can_skip(self)
    }

    #[inline]
    fn should_skip(&mut self, msg: &ParsedMessage) -> bool {
        T::should_skip(self, msg)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct IncludeAllMessages;

impl MessageFilter for IncludeAllMessages {
    #[inline]
    fn can_skip(&self) -> bool {
        false
    }

    #[inline]
    fn should_skip(&mut self, _: &ParsedMessage) -> bool {
        false
    }
}

#[derive(Debug)]
pub struct SkipExpiredExternals<'a> {
    pub chain_time_threshold_ms: u64,
    pub total_skipped: &'a mut u64,
}

impl MessageFilter for SkipExpiredExternals<'_> {
    #[inline]
    fn can_skip(&self) -> bool {
        true
    }

    fn should_skip(&mut self, msg: &ParsedMessage) -> bool {
        let res = msg.info().is_external_in()
            && msg
                .ext_msg_chain_time()
                .expect("external messages must have a chain time set")
                < self.chain_time_threshold_ms;
        *self.total_skipped += res as u64;
        res
    }
}

/// Wrap message filter options in enum to make
/// a cheap runtime filter type selection.
pub enum MsgFilter<'a> {
    SkipExpiredExternals(SkipExpiredExternals<'a>),
    IncludeAll(IncludeAllMessages),
}
impl MessageFilter for MsgFilter<'_> {
    fn can_skip(&self) -> bool {
        match self {
            Self::SkipExpiredExternals(f) => f.can_skip(),
            Self::IncludeAll(f) => f.can_skip(),
        }
    }
    fn should_skip(&mut self, m: &ParsedMessage) -> bool {
        match self {
            Self::SkipExpiredExternals(f) => f.should_skip(m),
            Self::IncludeAll(f) => f.should_skip(m),
        }
    }
}
#[cfg(test)]
struct DebugMessagesBufferIndexMap<'a>(pub &'a FastIndexMap<HashBytes, AccountEntry>);
#[cfg(test)]
impl std::fmt::Debug for DebugMessagesBufferIndexMap<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entries(self.0.iter().filter_map(|(k, entry)| {
                entry.current().map(|v| (DebugHashBytesShort(k), v.len()))
            }))
            .finish()
    }
}
#[derive(Default, Clone, Copy, PartialEq, Eq)]
pub enum BufferFillStateByCount {
    IsFull,
    #[default]
    NotFull,
}

#[derive(Default, Clone, Copy, PartialEq, Eq)]
pub enum BufferFillStateBySlots {
    CanFill,
    #[default]
    CanNotFill,
}

impl MessagesBuffer {
    pub fn check_is_filled(
        &self,
        limits: &MessagesBufferLimits,
    ) -> (BufferFillStateByCount, BufferFillStateBySlots) {
        let by_count = if self.msgs_count() >= limits.max_count {
            BufferFillStateByCount::IsFull
        } else {
            BufferFillStateByCount::NotFull
        };

        // TODO: msgs-v3: check if we can already fill required slots
        let by_slots = BufferFillStateBySlots::CanNotFill;

        (by_count, by_slots)
    }
}

pub(super) struct DebugHashBytesShort<'a>(pub &'a HashBytes);
impl std::fmt::Debug for DebugHashBytesShort<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.9}", format!("{}", self.0))
    }
}

struct SlotContext<'a> {
    slot_id: SlotId,
    slot: &'a mut SlotInfo,
    old_int_count: usize,
    old_ext_count: usize,
    remaning_capacity: usize,
}

#[derive(Default)]
pub struct MessageGroup {
    #[allow(clippy::vec_box)]
    msgs: FastHashMap<HashBytes, Vec<ParsedMessage>>,
    slots_info: SlotsInfo,
}

impl MessageGroup {
    pub fn len(&self) -> usize {
        self.slots_info.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn accounts_count(&self) -> usize {
        self.msgs.len()
    }

    pub fn account_ids(&self) -> impl Iterator<Item = &HashBytes> {
        self.msgs.keys()
    }

    pub fn messages_count(&self) -> usize {
        self.slots_info.msgs_count()
    }

    pub fn check_is_filled(&self, slots_count: usize, slot_vert_size: usize) -> bool {
        // 1. check if has remaning empty slots
        let remaning_slots_count = slots_count.saturating_sub(self.slots_info.slots.len());
        if remaning_slots_count > 0 {
            return false;
        }

        // 2. check if has not fully filled slots
        for bucket in self
            .slots_info
            .index_by_msgs_count
            .iter_upto(slot_vert_size)
        {
            if !bucket.is_empty() {
                return false;
            }
        }

        true
    }

    pub fn contains_account(&self, account_id: &HashBytes) -> bool {
        self.msgs.contains_key(account_id)
    }
}

impl std::ops::Add for MessageGroup {
    type Output = Self;

    fn add(mut self, other: Self) -> Self::Output {
        let last_slot_id = self.slots_info.last_slot_id.as_ref();
        let mut next_slot_id = match last_slot_id {
            Some(slot_id) => *slot_id + 1,
            None => 0,
        };
        for (_, new_slot) in other.slots_info.slots {
            let slots_ids = self
                .slots_info
                .index_by_msgs_count
                .get_mut_or_grow(new_slot.msgs_count());
            slots_ids.insert(next_slot_id);

            self.slots_info.slots.insert(next_slot_id, new_slot);
            self.slots_info.last_slot_id = Some(next_slot_id);

            next_slot_id += 1;
        }
        self.slots_info.int_count += other.slots_info.int_count;
        self.slots_info.ext_count += other.slots_info.ext_count;

        for (account_id, msgs) in other.msgs {
            if self.msgs.insert(account_id, msgs).is_some() {
                panic!("other message group should not contain account that already exists")
            }
        }

        self
    }
}

impl IntoParallelIterator for MessageGroup {
    type Item = (HashBytes, Vec<ParsedMessage>);
    type Iter = rayon::collections::hash_map::IntoIter<HashBytes, Vec<ParsedMessage>>;

    fn into_par_iter(self) -> Self::Iter {
        self.msgs.into_par_iter()
    }
}

impl IntoIterator for MessageGroup {
    type Item = (HashBytes, Vec<ParsedMessage>);
    type IntoIter = std::collections::hash_map::IntoIter<HashBytes, Vec<ParsedMessage>>;

    fn into_iter(self) -> Self::IntoIter {
        self.msgs.into_iter()
    }
}

#[cfg(test)]
impl MessageGroup {
    #[allow(clippy::vec_box)]
    pub fn msgs(&self) -> &FastHashMap<HashBytes, Vec<ParsedMessage>> {
        &self.msgs
    }
}

pub(super) struct DisplayMessageGroup<'a>(pub &'a MessageGroup);
impl std::fmt::Debug for DisplayMessageGroup<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for DisplayMessageGroup<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "int={}, ext={}, accounts={}, slots={}",
            self.0.slots_info.int_count,
            self.0.slots_info.ext_count,
            self.0.msgs.len(),
            self.0.slots_info.slots.len(),
        )
    }
}

pub(super) struct DebugMessageGroup<'a>(pub &'a MessageGroup);
impl std::fmt::Debug for DebugMessageGroup<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for DebugMessageGroup<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {}",
            DisplayMessageGroup(self.0),
            DisplayIter(self.0.slots_info.slots.values().map(|s| s.msgs_count())),
        )
    }
}

#[cfg(test)]
pub(super) struct DebugMessageGroupDetailed<'a>(pub &'a MessageGroup);
#[cfg(test)]
impl std::fmt::Debug for DebugMessageGroupDetailed<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageGroup")
            .field("msgs", &DebugMessageGroupHashMap(&self.0.msgs))
            .field("slots_info", &DebugSlotsInfo(&self.0.slots_info))
            .finish()
    }
}

#[cfg(test)]
#[allow(clippy::vec_box)]
struct DebugMessageGroupHashMap<'a>(pub &'a FastHashMap<HashBytes, Vec<ParsedMessage>>);
#[cfg(test)]
impl std::fmt::Debug for DebugMessageGroupHashMap<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entries(
                self.0
                    .iter()
                    .map(|(k, v)| (DebugHashBytesShort(k), v.len())),
            )
            .finish()
    }
}

#[derive(Debug, Default)]
pub(super) struct SlotsInfo {
    slots: FastHashMap<SlotId, SlotInfo>,
    last_slot_id: Option<SlotId>,
    /// Buckets of slot ids by messages count
    index_by_msgs_count: Vec<FastIndexSet<SlotId>>,
    int_count: usize,
    ext_count: usize,
}

impl SlotsInfo {
    fn msgs_count(&self) -> usize {
        self.int_count + self.ext_count
    }
}

#[cfg(test)]
struct DebugSlotsInfo<'a>(pub &'a SlotsInfo);
#[cfg(test)]
impl std::fmt::Debug for DebugSlotsInfo<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlotsInfo")
            .field("int_count", &self.0.int_count)
            .field("ext_count", &self.0.ext_count)
            .field("index_by_msgs_count", &self.0.index_by_msgs_count)
            .field("slots", &self.0.slots)
            .finish()
    }
}

#[derive(Default)]
pub(super) struct SlotInfo {
    accounts: Vec<HashBytes>,
    int_count: usize,
    ext_count: usize,
}

impl SlotInfo {
    fn msgs_count(&self) -> usize {
        self.int_count + self.ext_count
    }
}

impl std::fmt::Debug for SlotInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("")
            .field("int_count", &self.int_count)
            .field("ext_count", &self.ext_count)
            .field(
                "accounts",
                &DebugIter(self.accounts.iter().map(DebugHashBytesShort)),
            )
            .finish()
    }
}

struct SlotIndexUpdate {
    old_int_count: usize,
    old_ext_count: usize,
    new_int_count: usize,
    new_ext_count: usize,
}

impl SlotIndexUpdate {
    fn old_count(&self) -> usize {
        self.old_int_count + self.old_ext_count
    }
    fn new_count(&self) -> usize {
        self.new_int_count + self.new_ext_count
    }
}

trait IndexByMsgsCountExt<T> {
    /// Iterate over items up to specified max len
    fn iter_upto(&self, max_len: usize) -> std::slice::Iter<'_, T>;

    /// Pre-allocate index size
    fn presize(&mut self, len: usize);

    /// Mutable access to an item with enlarging vec capacity if required
    fn get_mut_or_grow(&mut self, idx: usize) -> &mut T;
}

impl<T> IndexByMsgsCountExt<T> for Vec<T>
where
    T: Default,
{
    #[inline]
    fn iter_upto(&self, max_len: usize) -> std::slice::Iter<'_, T> {
        let upper = max_len.min(self.len());
        self[..upper].iter()
    }

    fn presize(&mut self, len: usize) {
        if self.len() <= len {
            self.resize_with(len + 1, T::default);
        }
    }

    #[inline]
    fn get_mut_or_grow(&mut self, idx: usize) -> &mut T {
        self.presize(idx);
        &mut self[idx]
    }
}

#[cfg(test)]
pub(super) struct DebugMessagesBuffer<'a>(pub &'a MessagesBuffer);
#[cfg(test)]
impl std::fmt::Debug for DebugMessagesBuffer<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessagesBuffer")
            .field("int_count", &self.0.int_count)
            .field("ext_count", &self.0.ext_count)
            .field("msgs", &DebugMessagesBufferIndexMap(&self.0.msgs))
            .finish()
    }
}

#[cfg(test)]
mod transaction_tests {
    use tycho_types::cell::{Cell, HashBytes};

    use super::*;

    // Helper to create a test account ID
    fn account(n: u8) -> HashBytes {
        let mut bytes = [0u8; 32];
        bytes[0] = n;
        HashBytes(bytes)
    }

    // Helper to create a test internal message
    fn make_test_int_message(account_id: HashBytes, lt: u64) -> ParsedMessage {
        ParsedMessage::new(
            MsgInfo::Int(IntMsgInfo::default()),
            false,
            Cell::default(),
            None,
            None,
            None,
            None,
        )
    }

    // Helper to create a test external message
    fn make_test_ext_message(account_id: HashBytes, chain_time: u64) -> ParsedMessage {
        ParsedMessage::new(
            MsgInfo::ExtIn(ExtInMsgInfo::default()),
            false,
            Cell::default(),
            None,
            None,
            None,
            None,
        )
    }

    // ==================== BASIC TRANSACTION TESTS ====================

    #[test]
    fn test_transaction_not_active_by_default() {
        let buffer = MessagesBuffer::default();
        assert!(!buffer.is_in_transaction());
    }

    #[test]
    fn test_begin_starts_transaction() {
        let mut buffer = MessagesBuffer::default();
        buffer.begin();
        assert!(buffer.is_in_transaction());
    }

    #[test]
    fn test_commit_ends_transaction() {
        let mut buffer = MessagesBuffer::default();
        buffer.begin();
        buffer.commit();
        assert!(!buffer.is_in_transaction());
    }

    #[test]
    fn test_rollback_ends_transaction() {
        let mut buffer = MessagesBuffer::default();
        buffer.begin();
        buffer.rollback();
        assert!(!buffer.is_in_transaction());
    }

    // ==================== COMMIT TESTS ====================

    #[test]
    fn test_commit_preserves_added_messages() {
        let mut buffer = MessagesBuffer::default();
        let acc = account(1);

        buffer.begin();
        buffer.add_message(make_test_int_message(acc, 100));
        buffer.add_message(make_test_int_message(acc, 101));

        assert_eq!(buffer.int_count, 2);
        assert_eq!(buffer.account_messages_count(&acc), 2);

        buffer.commit();

        // After commit, changes should persist
        assert_eq!(buffer.int_count, 2);
        assert_eq!(buffer.account_messages_count(&acc), 2);
    }

    #[test]
    fn test_commit_preserves_removed_messages() {
        let mut buffer = MessagesBuffer::default();
        let acc = account(1);

        // Add messages outside transaction
        buffer.add_message(make_test_int_message(acc, 100));
        buffer.add_message(make_test_int_message(acc, 101));
        assert_eq!(buffer.int_count, 2);

        // Remove in transaction
        buffer.begin();
        let mut to_remove = FastHashSet::default();
        to_remove.insert(acc);
        buffer.remove_int_messages_by_accounts(&to_remove);

        assert_eq!(buffer.int_count, 0);
        assert_eq!(buffer.account_messages_count(&acc), 0);

        buffer.commit();

        // After commit, removal should persist
        assert_eq!(buffer.int_count, 0);
        assert_eq!(buffer.account_messages_count(&acc), 0);
    }

    // ==================== ROLLBACK TESTS ====================

    #[test]
    fn test_rollback_reverts_added_messages() {
        let mut buffer = MessagesBuffer::default();
        let acc = account(1);

        buffer.begin();
        buffer.add_message(make_test_int_message(acc, 100));
        buffer.add_message(make_test_int_message(acc, 101));

        assert_eq!(buffer.int_count, 2);
        assert_eq!(buffer.account_messages_count(&acc), 2);

        buffer.rollback();

        // After rollback, buffer should be empty
        assert_eq!(buffer.int_count, 0);
        assert_eq!(buffer.ext_count, 0);
        assert_eq!(buffer.account_messages_count(&acc), 0);
    }

    #[test]
    fn test_rollback_reverts_removed_messages() {
        let mut buffer = MessagesBuffer::default();
        let acc = account(1);

        // Add messages outside transaction
        buffer.add_message(make_test_int_message(acc, 100));
        buffer.add_message(make_test_int_message(acc, 101));
        assert_eq!(buffer.int_count, 2);

        // Remove in transaction
        buffer.begin();
        let mut to_remove = FastHashSet::default();
        to_remove.insert(acc);
        buffer.remove_int_messages_by_accounts(&to_remove);

        assert_eq!(buffer.int_count, 0);

        buffer.rollback();

        // After rollback, messages should be restored
        assert_eq!(buffer.int_count, 2);
        assert_eq!(buffer.account_messages_count(&acc), 2);
    }

    #[test]
    fn test_rollback_reverts_to_pre_transaction_state() {
        let mut buffer = MessagesBuffer::default();
        let acc1 = account(1);
        let acc2 = account(2);

        // Setup: add messages to acc1 outside transaction
        buffer.add_message(make_test_int_message(acc1, 100));
        buffer.add_message(make_test_int_message(acc1, 101));

        let initial_int_count = buffer.int_count;
        let initial_acc1_count = buffer.account_messages_count(&acc1);

        // Transaction: modify acc1, add acc2
        buffer.begin();

        // Add more to acc1
        buffer.add_message(make_test_int_message(acc1, 102));

        // Add new account
        buffer.add_message(make_test_int_message(acc2, 200));
        buffer.add_message(make_test_int_message(acc2, 201));

        assert_eq!(buffer.int_count, 5);
        assert_eq!(buffer.account_messages_count(&acc1), 3);
        assert_eq!(buffer.account_messages_count(&acc2), 2);

        buffer.rollback();

        // Should revert to initial state
        assert_eq!(buffer.int_count, initial_int_count);
        assert_eq!(buffer.account_messages_count(&acc1), initial_acc1_count);
        assert_eq!(buffer.account_messages_count(&acc2), 0);
    }

    #[test]
    fn test_rollback_restores_min_ext_chain_time() {
        let mut buffer = MessagesBuffer::default();
        let acc = account(1);

        // Add external message outside transaction
        buffer.add_message(make_test_ext_message(acc, 1000));
        let initial_min_time = buffer.min_ext_chain_time();

        buffer.begin();

        // Add external with smaller chain time
        buffer.add_message(make_test_ext_message(acc, 500));
        assert_eq!(buffer.min_ext_chain_time(), 500);

        buffer.rollback();

        // Should restore original min time
        assert_eq!(buffer.min_ext_chain_time(), initial_min_time);
    }

    // ==================== EDGE CASES ====================

    #[test]
    fn test_rollback_without_begin_is_noop() {
        let mut buffer = MessagesBuffer::default();
        let acc = account(1);

        buffer.add_message(make_test_int_message(acc, 100));

        // Rollback without begin should do nothing
        buffer.rollback();

        assert_eq!(buffer.int_count, 1);
        assert_eq!(buffer.account_messages_count(&acc), 1);
    }

    #[test]
    fn test_multiple_modifications_same_account_in_transaction() {
        let mut buffer = MessagesBuffer::default();
        let acc = account(1);

        // Initial state
        buffer.add_message(make_test_int_message(acc, 100));

        buffer.begin();

        // Multiple adds
        buffer.add_message(make_test_int_message(acc, 101));
        buffer.add_message(make_test_int_message(acc, 102));
        buffer.add_message(make_test_int_message(acc, 103));

        assert_eq!(buffer.account_messages_count(&acc), 4);

        buffer.rollback();

        // Should restore to single message
        assert_eq!(buffer.account_messages_count(&acc), 1);
        assert_eq!(buffer.int_count, 1);
    }

    #[test]
    fn test_backup_only_happens_once_per_account() {
        let mut buffer = MessagesBuffer::default();
        let acc = account(1);

        // Initial: 2 messages
        buffer.add_message(make_test_int_message(acc, 100));
        buffer.add_message(make_test_int_message(acc, 101));

        buffer.begin();

        // First modification - should backup [msg100, msg101]
        buffer.add_message(make_test_int_message(acc, 102));

        // Second modification - should NOT re-backup
        buffer.add_message(make_test_int_message(acc, 103));

        // Third modification
        buffer.add_message(make_test_int_message(acc, 104));

        assert_eq!(buffer.account_messages_count(&acc), 5);

        buffer.rollback();

        // Should restore to original 2 messages, not intermediate states
        assert_eq!(buffer.account_messages_count(&acc), 2);
    }

    #[test]
    fn test_add_to_new_account_then_rollback() {
        let mut buffer = MessagesBuffer::default();
        let acc1 = account(1);
        let acc2 = account(2);

        // acc1 exists before transaction
        buffer.add_message(make_test_int_message(acc1, 100));

        buffer.begin();

        // acc2 is new in transaction
        buffer.add_message(make_test_int_message(acc2, 200));

        assert_eq!(buffer.account_messages_count(&acc2), 1);

        buffer.rollback();

        // acc2 should be completely removed
        assert_eq!(buffer.account_messages_count(&acc2), 0);
        assert_eq!(buffer.int_count, 1); // only acc1's message
    }

    #[test]
    fn test_remove_then_add_same_account_in_transaction() {
        let mut buffer = MessagesBuffer::default();
        let acc = account(1);

        // Initial state
        buffer.add_message(make_test_int_message(acc, 100));
        buffer.add_message(make_test_int_message(acc, 101));

        buffer.begin();

        // Remove all messages
        let mut to_remove = FastHashSet::default();
        to_remove.insert(acc);
        buffer.remove_int_messages_by_accounts(&to_remove);

        assert_eq!(buffer.account_messages_count(&acc), 0);

        // Add new messages to same account
        buffer.add_message(make_test_int_message(acc, 200));

        assert_eq!(buffer.account_messages_count(&acc), 1);

        buffer.rollback();

        // Should restore original messages
        assert_eq!(buffer.account_messages_count(&acc), 2);
        assert_eq!(buffer.int_count, 2);
    }

    // ==================== FILL MESSAGE GROUP TESTS ====================

    #[test]
    fn test_fill_message_group_with_rollback() {
        let mut buffer = MessagesBuffer::default();
        let acc1 = account(1);
        let acc2 = account(2);

        // Add messages
        buffer.add_message(make_test_int_message(acc1, 100));
        buffer.add_message(make_test_int_message(acc1, 101));
        buffer.add_message(make_test_int_message(acc2, 200));

        let initial_count = buffer.int_count;

        buffer.begin();

        let mut msg_group = MessageGroup::default();
        let mut skipped = FastHashSet::default();

        buffer.fill_message_group(
            &mut msg_group,
            10, // slots_count
            10, // slot_vert_size
            &mut skipped,
            |_| (false, 1), // don't skip any account
            IncludeAllMessages,
        );

        // Messages should be moved to group
        assert!(buffer.int_count < initial_count);

        buffer.rollback();

        // Buffer should be restored
        assert_eq!(buffer.int_count, initial_count);
        assert_eq!(buffer.account_messages_count(&acc1), 2);
        assert_eq!(buffer.account_messages_count(&acc2), 1);
    }

    #[test]
    fn test_fill_message_group_with_commit() {
        let mut buffer = MessagesBuffer::default();
        let acc = account(1);

        buffer.add_message(make_test_int_message(acc, 100));
        buffer.add_message(make_test_int_message(acc, 101));

        buffer.begin();

        let mut msg_group = MessageGroup::default();
        let mut skipped = FastHashSet::default();

        let result = buffer.fill_message_group(
            &mut msg_group,
            10,
            10,
            &mut skipped,
            |_| (false, 1),
            IncludeAllMessages,
        );

        let collected = result.collected_count;

        buffer.commit();

        // After commit, changes persist
        assert_eq!(buffer.int_count, 2 - collected);
    }

    // ==================== SKIP MESSAGES TESTS ====================

    #[test]
    fn test_try_skip_account_msgs_with_rollback() {
        let mut buffer = MessagesBuffer::default();
        let acc = account(1);

        // Add external messages with different chain times
        buffer.add_message(make_test_ext_message(acc, 100));
        buffer.add_message(make_test_ext_message(acc, 200));
        buffer.add_message(make_test_ext_message(acc, 300));

        let initial_ext_count = buffer.ext_count;

        buffer.begin();

        // Skip messages with chain_time < 250
        let mut total_skipped = 0u64;
        let filter = SkipExpiredExternals {
            chain_time_threshold_ms: 250,
            total_skipped: &mut total_skipped,
        };
        buffer.try_skip_account_msgs(&acc, filter);

        // Some messages should be skipped
        assert!(buffer.ext_count < initial_ext_count);

        buffer.rollback();

        // Should restore all messages
        assert_eq!(buffer.ext_count, initial_ext_count);
        assert_eq!(buffer.account_messages_count(&acc), 3);
    }

    // ==================== COUNTERS CONSISTENCY ====================

    #[test]
    fn test_int_ext_counters_consistency_after_rollback() {
        let mut buffer = MessagesBuffer::default();
        let acc = account(1);

        // Mix of int and ext messages
        buffer.add_message(make_test_int_message(acc, 100));
        buffer.add_message(make_test_ext_message(acc, 1000));
        buffer.add_message(make_test_int_message(acc, 101));

        let initial_int = buffer.int_count;
        let initial_ext = buffer.ext_count;

        buffer.begin();

        buffer.add_message(make_test_int_message(acc, 102));
        buffer.add_message(make_test_ext_message(acc, 2000));

        assert_eq!(buffer.int_count, initial_int + 1);
        assert_eq!(buffer.ext_count, initial_ext + 1);

        buffer.rollback();

        assert_eq!(buffer.int_count, initial_int);
        assert_eq!(buffer.ext_count, initial_ext);
    }

    #[test]
    fn test_msgs_count_equals_sum_of_int_and_ext() {
        let mut buffer = MessagesBuffer::default();
        let acc = account(1);

        buffer.add_message(make_test_int_message(acc, 100));
        buffer.add_message(make_test_ext_message(acc, 1000));

        assert_eq!(buffer.msgs_count(), buffer.int_count + buffer.ext_count);

        buffer.begin();
        buffer.add_message(make_test_int_message(acc, 101));
        assert_eq!(buffer.msgs_count(), buffer.int_count + buffer.ext_count);

        buffer.rollback();
        assert_eq!(buffer.msgs_count(), buffer.int_count + buffer.ext_count);
    }
}
