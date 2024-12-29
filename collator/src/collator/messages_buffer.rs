use std::collections::{btree_map, BTreeMap, VecDeque};

use everscale_types::cell::HashBytes;
use everscale_types::models::{ExtInMsgInfo, IntMsgInfo, MsgInfo};
use rayon::iter::IntoParallelIterator;
use tycho_util::{FastHashMap, FastHashSet};

use super::types::{ParsedMessage, ParsedMessageKind};

#[cfg(test)]
#[path = "tests/messages_buffer_tests.rs"]
pub(super) mod tests;

type SlotId = u16;

type FastIndexMap<K, V> = indexmap::IndexMap<K, V, ahash::RandomState>;
pub type FastIndexSet<K> = indexmap::IndexSet<K, ahash::RandomState>;

#[derive(Clone, Copy)]
pub struct MessagesBufferLimits {
    pub max_count: usize,
    pub slots_count: usize,
    pub slot_vert_size: usize,
}

#[derive(Default)]
pub struct MessagesBufferV2 {
    msgs: FastIndexMap<HashBytes, VecDeque<Box<ParsedMessage>>>,
    int_count: usize,
    ext_count: usize,
    sorted_index: BTreeMap<usize, FastHashSet<HashBytes>>,
}

impl MessagesBufferV2 {
    pub fn account_messages_count(&self, account_id: &HashBytes) -> usize {
        self.msgs
            .get(account_id)
            .map(|msgs| msgs.len())
            .unwrap_or_default()
    }

    pub fn msgs_count(&self) -> usize {
        self.int_count + self.ext_count
    }

    pub fn add_message(&mut self, msg: Box<ParsedMessage>) {
        assert_eq!(
            msg.special_origin, None,
            "unexpected special origin in ordinary messages set"
        );

        let dst = match &msg.info {
            MsgInfo::Int(IntMsgInfo { dst, .. }) => {
                self.int_count += 1;
                dst
            }
            MsgInfo::ExtIn(ExtInMsgInfo { dst, .. }) => {
                self.ext_count += 1;
                dst
            }
            MsgInfo::ExtOut(info) => {
                unreachable!("ext out message in ordinary messages set: {info:?}")
            }
        };
        let account_id = dst.as_std().map(|a| a.address).unwrap_or_default();

        // insert message to buffer
        let prev_msgs_count = match self.msgs.entry(account_id) {
            indexmap::map::Entry::Vacant(vacant) => {
                vacant.insert([msg].into());
                0
            }
            indexmap::map::Entry::Occupied(mut occupied) => {
                let entry = occupied.get_mut();
                let prev_count = entry.len();
                entry.push_back(msg);
                prev_count
            }
        };

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

    /// Returns the set of unused accounts from buffer that we can use
    /// to fill group from the same buffer but with grater limits.
    ///
    /// E.g. we filled 5 slots from buffer 1, then tried to fill up to 8 slots
    /// from buffer 2, but filled only 6 slots. So we can try to fill the rest slots
    /// up to 8 from buffer 1 again.
    pub fn fill_message_group<F>(
        &mut self,
        msg_group: &mut MessageGroup,
        slots_count: usize,
        slot_vert_size: usize,
        unused_buffer_accounts: Option<FastIndexSet<HashBytes>>,
        check_skip_account: F,
    ) -> FastIndexSet<HashBytes>
    where
        F: Fn(&HashBytes) -> bool,
    {
        // take slots info from group
        let mut slots_info = std::mem::take(&mut msg_group.slots_info);

        // we will collect updates for slots index and apply them at the end
        let mut slots_index_updates = BTreeMap::<SlotId, SlotIndexUpdate>::default();

        // cache buffer accounts to track whose messages were not used in group
        let mut unused_buffer_accounts =
            unused_buffer_accounts.unwrap_or_else(|| self.msgs.keys().copied().collect());

        // 1. try to fill remaning slots with messages of accounts which are not included in any slot
        let mut new_used_slots = FastHashSet::<SlotId>::default();
        let mut remaning_slots_count = slots_count.saturating_sub(slots_info.slots.len());
        if remaning_slots_count > 0 {
            // define next empty slot
            let last_slot_id = slots_info
                .slots
                .last_key_value()
                .map(|(k, _)| *k)
                .unwrap_or_default();
            let mut slot_id = last_slot_id + 1;
            remaning_slots_count -= 1;

            let mut not_filled_slots = VecDeque::<SlotId>::default();
            loop {
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
                let mut used_buffer_accounts = vec![];
                for account_id in &unused_buffer_accounts {
                    if msg_group.msgs.contains_key(account_id) {
                        continue;
                    }

                    // skip accounts that do not pass the provided check
                    if check_skip_account(account_id) {
                        used_buffer_accounts.push(*account_id);
                        continue;
                    }

                    self.move_account_messages_to_slot(
                        *account_id,
                        msg_group,
                        &mut slot_cx,
                        &mut used_buffer_accounts,
                        &mut slots_index_updates,
                    );

                    if slot_cx.slot.msgs_count() > 0 {
                        if slot_cx.remaning_capacity > 0 {
                            not_filled_slots.push_back(slot_id);
                        }

                        break;
                    }
                }
                for account_id in used_buffer_accounts {
                    unused_buffer_accounts.shift_remove(&account_id);
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
        for (_, slot_ids) in slots_info.index_by_msgs_count.range(..slot_vert_size) {
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
                let mut used_buffer_accounts = vec![];
                for account_id in slot_cx.slot.accounts.clone() {
                    self.move_account_messages_to_slot(
                        account_id,
                        msg_group,
                        &mut slot_cx,
                        &mut used_buffer_accounts,
                        &mut slots_index_updates,
                    );

                    if slot_cx.remaning_capacity == 0 {
                        break;
                    }
                }
                for account_id in used_buffer_accounts {
                    unused_buffer_accounts.shift_remove(&account_id);
                }

                // go to next slot if current is full
                if slot_cx.remaning_capacity == 0 {
                    continue;
                }

                // then try to get messages of other accounts which are not included in any slot
                let mut used_buffer_accounts = vec![];
                for account_id in &unused_buffer_accounts {
                    if msg_group.msgs.contains_key(account_id) {
                        continue;
                    }

                    // skip accounts that do not pass the provided check
                    if check_skip_account(account_id) {
                        used_buffer_accounts.push(*account_id);
                        continue;
                    }

                    self.move_account_messages_to_slot(
                        *account_id,
                        msg_group,
                        &mut slot_cx,
                        &mut used_buffer_accounts,
                        &mut slots_index_updates,
                    );

                    if slot_cx.remaning_capacity == 0 {
                        break;
                    }
                }
                for account_id in used_buffer_accounts {
                    unused_buffer_accounts.shift_remove(&account_id);
                }
            }
        }

        // NOTE: `unused_buffer_accounts` will not contain accounts which we already used in previous step

        // 3. update slots index
        for (slot_id, index_update) in slots_index_updates {
            // remove slot from old count basket
            if index_update.old_count() != 0 {
                let slots_ids = slots_info
                    .index_by_msgs_count
                    .entry(index_update.old_count())
                    .or_default();
                slots_ids.shift_remove(&slot_id);
                slots_info.int_count -= index_update.old_int_count;
                slots_info.ext_count -= index_update.old_ext_count;
            }

            // add slot to a new count basket
            let slots_ids = slots_info
                .index_by_msgs_count
                .entry(index_update.new_count())
                .or_default();
            slots_ids.insert(slot_id);
            slots_info.int_count += index_update.new_int_count;
            slots_info.ext_count += index_update.new_ext_count;
        }

        // put slots info into group
        std::mem::swap(&mut slots_info, &mut msg_group.slots_info);

        unused_buffer_accounts
    }

    fn move_account_messages_to_slot(
        &mut self,
        account_id: HashBytes,
        msg_group: &mut MessageGroup,
        slot_cx: &mut SlotContext<'_>,
        used_buffer_accounts: &mut Vec<HashBytes>,
        slots_index_updates: &mut BTreeMap<SlotId, SlotIndexUpdate>,
    ) {
        let mut amount = 0;
        let mut int_count = 0;
        let mut ext_count = 0;
        if let indexmap::map::Entry::Occupied(mut occupied) = self.msgs.entry(account_id) {
            let remaning_account_msgs_count = {
                let account_msgs = occupied.get_mut();

                amount = account_msgs.len().min(slot_cx.remaning_capacity);
                if amount == 0 {
                    return;
                }

                used_buffer_accounts.push(account_id);

                let slot_account_msgs = msg_group.msgs.entry(account_id).or_default();
                if slot_account_msgs.is_empty() {
                    slot_cx.slot.accounts.insert(account_id);
                }

                for msg in account_msgs.drain(..amount) {
                    match msg.kind() {
                        ParsedMessageKind::Int => int_count += 1,
                        ParsedMessageKind::ExtIn => ext_count += 1,
                        _ => unreachable!("must contain only Int and ExtIn messages"),
                    }
                    slot_account_msgs.push(msg);
                }

                account_msgs.len()
            };

            // remove account from buffer if msgs queue was drained
            if remaning_account_msgs_count == 0 {
                occupied.shift_remove();
            }
        }

        if amount == 0 {
            return;
        }

        // update buffer msgs counter
        self.int_count -= int_count;
        self.ext_count -= ext_count;

        // update slot msgs counter
        slot_cx.slot.int_count += int_count;
        slot_cx.slot.ext_count += ext_count;

        // calc remaining capacity
        slot_cx.remaning_capacity -= amount;

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

        // TODO: update buffer index?
    }
}

pub(super) struct DebugMessagesBuffer<'a>(pub &'a MessagesBufferV2);
impl std::fmt::Debug for DebugMessagesBuffer<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessagesBuffer")
            .field("int_count", &self.0.int_count)
            .field("ext_count", &self.0.ext_count)
            .field("msgs", &DebugMessagesBufferIndexMap(&self.0.msgs))
            .finish()
    }
}

pub enum BufferFillStateByCount {
    IsFull,
    NotFull,
}

pub enum BufferFillStateBySlots {
    CanFill,
    CanNotFill,
}

impl MessagesBufferV2 {
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

struct DebugMessagesBufferIndexMap<'a>(
    pub &'a FastIndexMap<HashBytes, VecDeque<Box<ParsedMessage>>>,
);
impl std::fmt::Debug for DebugMessagesBufferIndexMap<'_> {
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

pub(super) struct DebugHashBytesShort<'a>(pub &'a HashBytes);
impl std::fmt::Debug for DebugHashBytesShort<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "'{}'", self.0[0])
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
    msgs: FastHashMap<HashBytes, Vec<Box<ParsedMessage>>>,
    slots_info: SlotsInfo,
}

impl MessageGroup {
    pub fn len(&self) -> usize {
        self.slots_info.slots.len()
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
        if self
            .slots_info
            .index_by_msgs_count
            .range(..slot_vert_size)
            .next()
            .is_some()
        {
            return false;
        }

        true
    }
}

impl IntoParallelIterator for MessageGroup {
    type Item = (HashBytes, Vec<Box<ParsedMessage>>);
    type Iter = rayon::collections::hash_map::IntoIter<HashBytes, Vec<Box<ParsedMessage>>>;

    fn into_par_iter(self) -> Self::Iter {
        self.msgs.into_par_iter()
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
            "int={}, ext={}, ",
            self.0.slots_info.int_count, self.0.slots_info.ext_count,
        )?;
        f.debug_list()
            .entries(self.0.msgs.values().map(|v| v.len()))
            .finish()
    }
}

pub(super) struct DebugMessageGroup<'a>(pub &'a MessageGroup);
impl std::fmt::Debug for DebugMessageGroup<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageGroup")
            .field("msgs", &DebugMessageGroupHashMap(&self.0.msgs))
            .field("slots_info", &DebugSlotsInfo(&self.0.slots_info))
            .finish()
    }
}

#[allow(clippy::vec_box)]
struct DebugMessageGroupHashMap<'a>(pub &'a FastHashMap<HashBytes, Vec<Box<ParsedMessage>>>);
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
    slots: BTreeMap<SlotId, SlotInfo>,
    index_by_account: FastHashMap<HashBytes, SlotId>,
    index_by_msgs_count: BTreeMap<usize, FastIndexSet<SlotId>>,
    int_count: usize,
    ext_count: usize,
}

impl SlotsInfo {
    fn msgs_count(&self) -> usize {
        self.int_count + self.ext_count
    }
}

struct DebugSlotsInfo<'a>(pub &'a SlotsInfo);
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
    accounts: FastIndexSet<HashBytes>,
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
            .field("accounts", &DebugIndexSetOfHashBytesShort(&self.accounts))
            .finish()
    }
}

struct DebugIndexSetOfHashBytesShort<'a>(pub &'a FastIndexSet<HashBytes>);
impl std::fmt::Debug for DebugIndexSetOfHashBytesShort<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set()
            .entries(self.0.iter().map(DebugHashBytesShort))
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
