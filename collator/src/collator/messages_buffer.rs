use std::collections::{BTreeMap, VecDeque, btree_map};

use rayon::iter::IntoParallelIterator;
use tycho_block_util::queue::QueueKey;
use tycho_types::cell::HashBytes;
use tycho_types::models::{ExtInMsgInfo, IntMsgInfo, MsgInfo};
use tycho_util::{FastHashMap, FastHashSet};

use super::types::ParsedMessage;
use crate::types::{DebugIter, DisplayIter};

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
pub struct MessagesBuffer {
    msgs: FastIndexMap<HashBytes, VecDeque<Box<ParsedMessage>>>,
    int_count: usize,
    ext_count: usize,
    sorted_index: BTreeMap<usize, FastHashSet<HashBytes>>,
}

impl MessagesBuffer {
    pub fn account_messages_count(&self, account_id: &HashBytes) -> usize {
        self.msgs
            .get(account_id)
            .map(|msgs| msgs.len())
            .unwrap_or_default()
    }

    pub fn msgs_count(&self) -> usize {
        self.int_count + self.ext_count
    }

    pub fn iter(&self) -> impl Iterator<Item = (&HashBytes, &VecDeque<Box<ParsedMessage>>)> {
        self.msgs.iter()
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

    pub fn remove_messages_by_accounts(&mut self, addresses_to_remove: &FastHashSet<HashBytes>) {
        self.msgs.retain(|k, v| {
            if addresses_to_remove.contains(k) {
                self.int_count -= v.len();
                false
            } else {
                true
            }
        });
    }

    /// Returns queue keys of collected internal queue messages.
    pub fn fill_message_group<FA, FM>(
        &mut self,
        msg_group: &mut MessageGroup,
        slots_count: usize,
        slot_vert_size: usize,
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

        // we will collect updates for slots index and apply them at the end
        let mut slots_index_updates = BTreeMap::new();

        // track collected queue messages
        let mut collected_queue_msgs_keys = vec![];

        // track accounts whose messages were not used to fill group
        let mut buffer_accounts: VecDeque<HashBytes> = self.msgs.keys().copied().collect();

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
                while let Some(account_id) = buffer_accounts.pop_front() {
                    if msg_group.msgs.contains_key(&account_id) {
                        // try skip messages from skipped account: maybe they expired
                        self.try_skip_account_msgs(&account_id, &mut msg_filter);

                        continue;
                    }

                    // skip accounts that do not pass the provided check
                    let (skip_account, check_ops_count) = check_skip_account(&account_id);
                    ops_count.saturating_add_assign(check_ops_count);
                    if skip_account {
                        // try skip messages from skipped account: maybe they expired
                        self.try_skip_account_msgs(&account_id, &mut msg_filter);

                        continue;
                    }

                    let move_ops_count = self.move_account_messages_to_slot(
                        account_id,
                        msg_group,
                        &mut slot_cx,
                        &mut collected_queue_msgs_keys,
                        &mut slots_index_updates,
                        &mut msg_filter,
                    );
                    ops_count.saturating_add_assign(move_ops_count);

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
                for account_id in slot_cx.slot.accounts.clone() {
                    // TODO: we may not check account that was already skipped before
                    // skip accounts that do not pass the provided check
                    let (skip_account, check_ops_count) = check_skip_account(&account_id);
                    ops_count.saturating_add_assign(check_ops_count);
                    if skip_account {
                        // try skip messages from skipped account: maybe they expired
                        self.try_skip_account_msgs(&account_id, &mut msg_filter);

                        continue;
                    }

                    let move_ops_count = self.move_account_messages_to_slot(
                        account_id,
                        msg_group,
                        &mut slot_cx,
                        &mut collected_queue_msgs_keys,
                        &mut slots_index_updates,
                        &mut msg_filter,
                    );
                    ops_count.saturating_add_assign(move_ops_count);

                    if slot_cx.remaning_capacity == 0 {
                        break;
                    }
                }

                // go to next slot if current is full
                if slot_cx.remaning_capacity == 0 {
                    continue;
                }

                // then try to get messages of other accounts which are not included in any slot

                while let Some(account_id) = buffer_accounts.pop_front() {
                    if msg_group.msgs.contains_key(&account_id) {
                        // try skip messages from skipped account: maybe they expired
                        self.try_skip_account_msgs(&account_id, &mut msg_filter);

                        continue;
                    }

                    // skip accounts that do not pass the provided check
                    let (skip_account, check_ops_count) = check_skip_account(&account_id);
                    ops_count.saturating_add_assign(check_ops_count);
                    if skip_account {
                        // try skip messages from skipped account: maybe they expired
                        self.try_skip_account_msgs(&account_id, &mut msg_filter);

                        continue;
                    }

                    let move_ops_count = self.move_account_messages_to_slot(
                        account_id,
                        msg_group,
                        &mut slot_cx,
                        &mut collected_queue_msgs_keys,
                        &mut slots_index_updates,
                        &mut msg_filter,
                    );
                    ops_count.saturating_add_assign(move_ops_count);

                    if slot_cx.remaning_capacity == 0 {
                        break;
                    }
                }
            }
        }

        // check and skip messages in remaning accounts if required
        if msg_filter.can_skip() {
            while let Some(account_id) = buffer_accounts.pop_front() {
                self.try_skip_account_msgs(&account_id, &mut msg_filter);
            }
        }

        // remove empty accounts from buffer
        ops_count.saturating_add_assign(self.msgs.len() as u64);
        self.msgs.retain(|_, account_msgs| !account_msgs.is_empty());

        // 3. update slots index
        let mut remove_slot_ids = BTreeMap::<usize, FastHashSet<u16>>::new();
        for (slot_id, index_update) in slots_index_updates {
            // remove slot from old count basket
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

            // add slot to a new count basket
            let slots_ids = slots_info
                .index_by_msgs_count
                .entry(index_update.new_count())
                .or_default();
            slots_ids.insert(slot_id);
            slots_info.int_count += index_update.new_int_count;
            slots_info.ext_count += index_update.new_ext_count;
        }
        for (old_count, to_remove) in remove_slot_ids {
            let slots_ids = slots_info.index_by_msgs_count.entry(old_count).or_default();
            slots_ids.retain(|id| !to_remove.contains(id));
        }

        // put slots info into group
        std::mem::swap(&mut slots_info, &mut msg_group.slots_info);

        FillMessageGroupResult {
            collected_queue_msgs_keys,
            ops_count,
        }
    }

    fn move_account_messages_to_slot<FM>(
        &mut self,
        account_id: HashBytes,
        msg_group: &mut MessageGroup,
        slot_cx: &mut SlotContext<'_>,
        collected_queue_msgs_keys: &mut Vec<QueueKey>,
        slots_index_updates: &mut BTreeMap<SlotId, SlotIndexUpdate>,
        mut msg_filter: FM,
    ) -> u64
    where
        FM: MessageFilter,
    {
        // evaluate ops count for wu calculation
        let mut ops_count = 0;

        let mut amount = 0;

        let mut int_collected_count = 0;
        let mut ext_collected_count = 0;

        let mut ext_skipped_count = 0;

        let mut should_add_account_to_slot_index = false;

        if let Some(account_msgs) = self.msgs.get_mut(&account_id) {
            ops_count.saturating_add_assign(1);

            amount = account_msgs.len().min(slot_cx.remaning_capacity);
            if amount == 0 {
                return ops_count;
            }

            let slot_account_msgs = msg_group.msgs.entry(account_id).or_default();
            ops_count.saturating_add_assign(1);

            should_add_account_to_slot_index = slot_account_msgs.is_empty();

            slot_account_msgs.reserve(amount);
            while int_collected_count + ext_collected_count < amount {
                let Some(msg) = account_msgs.pop_front() else {
                    break;
                };
                ops_count.saturating_add_assign(1);

                // check and skip message if required
                if msg_filter.should_skip(&msg) {
                    debug_assert!(
                        msg.info.is_external_in(),
                        "we should only skip extenal messages"
                    );
                    ext_skipped_count += 1;
                    continue;
                }

                // collect message if it was not skipped
                match &msg.info {
                    MsgInfo::Int(info) => {
                        collected_queue_msgs_keys.push(QueueKey {
                            lt: info.created_lt,
                            hash: *msg.cell.repr_hash(),
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
                ops_count.saturating_add_assign(1);
            }
        }

        if amount == 0 {
            return ops_count;
        }

        // update buffer msgs counter by skipped messages
        self.ext_count -= ext_skipped_count;

        let collected_count = int_collected_count + ext_collected_count;
        if collected_count == 0 {
            return ops_count;
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

        ops_count
    }

    fn try_skip_account_msgs<FM>(&mut self, account_id: &HashBytes, mut filter: FM)
    where
        FM: MessageFilter,
    {
        if !filter.can_skip() {
            return;
        }

        if let Some(account_msgs) = self.msgs.get_mut(account_id) {
            account_msgs.retain(|msg| {
                let skip = filter.should_skip(msg);
                debug_assert!(
                    !skip || msg.info.is_external_in(),
                    "we should only skip external messages"
                );
                self.ext_count -= skip as usize;
                !skip
            });
        }
    }
}

pub struct FillMessageGroupResult {
    pub collected_queue_msgs_keys: Vec<QueueKey>,
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
        let res = msg.info.is_external_in()
            && msg
                .ext_msg_chain_time
                .expect("external messages must have a chain time set")
                < self.chain_time_threshold_ms;
        *self.total_skipped += res as u64;
        res
    }
}

pub trait SaturatingAddAssign {
    fn saturating_add_assign(&mut self, rhs: Self);
}
impl SaturatingAddAssign for u64 {
    fn saturating_add_assign(&mut self, rhs: Self) {
        *self = self.saturating_add(rhs);
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

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum BufferFillStateByCount {
    IsFull,
    NotFull,
}

impl Default for BufferFillStateByCount {
    fn default() -> Self {
        Self::NotFull
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum BufferFillStateBySlots {
    CanFill,
    CanNotFill,
}

impl Default for BufferFillStateBySlots {
    fn default() -> Self {
        Self::CanNotFill
    }
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

#[cfg(test)]
struct DebugMessagesBufferIndexMap<'a>(
    pub &'a FastIndexMap<HashBytes, VecDeque<Box<ParsedMessage>>>,
);
#[cfg(test)]
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
    msgs: FastHashMap<HashBytes, Vec<Box<ParsedMessage>>>,
    slots_info: SlotsInfo,
}

impl MessageGroup {
    pub fn len(&self) -> usize {
        self.slots_info.slots.len()
    }

    pub fn accounts_count(&self) -> usize {
        self.msgs.len()
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

    pub fn add(mut self, other: Self) -> Self {
        let last_slot_id = self.slots_info.last_slot_id.as_ref();
        let mut next_slot_id = match last_slot_id {
            Some(slot_id) => *slot_id + 1,
            None => 0,
        };
        for (_, new_slot) in other.slots_info.slots {
            let slots_ids = self
                .slots_info
                .index_by_msgs_count
                .entry(new_slot.msgs_count())
                .or_default();
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

    pub fn contains_account(&self, account_id: &HashBytes) -> bool {
        self.msgs.contains_key(account_id)
    }
}

impl IntoParallelIterator for MessageGroup {
    type Item = (HashBytes, Vec<Box<ParsedMessage>>);
    type Iter = rayon::collections::hash_map::IntoIter<HashBytes, Vec<Box<ParsedMessage>>>;

    fn into_par_iter(self) -> Self::Iter {
        self.msgs.into_par_iter()
    }
}

impl IntoIterator for MessageGroup {
    type Item = (HashBytes, Vec<Box<ParsedMessage>>);
    type IntoIter = std::collections::hash_map::IntoIter<HashBytes, Vec<Box<ParsedMessage>>>;

    fn into_iter(self) -> Self::IntoIter {
        self.msgs.into_iter()
    }
}

#[cfg(test)]
impl MessageGroup {
    #[allow(clippy::vec_box)]
    pub fn msgs(&self) -> &FastHashMap<HashBytes, Vec<Box<ParsedMessage>>> {
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
struct DebugMessageGroupHashMap<'a>(pub &'a FastHashMap<HashBytes, Vec<Box<ParsedMessage>>>);
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
    index_by_msgs_count: BTreeMap<usize, FastIndexSet<SlotId>>,
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
