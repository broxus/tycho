use everscale_types::cell::HashBytes;
use everscale_types::models::{ExtInMsgInfo, IntMsgInfo, MsgInfo, ShardIdent};
use tycho_block_util::queue::QueueKey;
use tycho_util::FastHashMap;

use super::types::{MessageGroup, ParsedMessage};
use crate::collator::types::DisplayMessageGroup;
use crate::tracing_targets;

#[derive(Default)]
pub(super) struct MessageGroupContainerV1 {
    pub inner: MessageGroup,
    filling: usize,
    is_full: bool,
}

impl MessageGroupContainerV1 {
    pub fn is_full(&self) -> bool {
        self.is_full
    }

    pub fn insert(
        &mut self,
        msg: &mut Option<Box<ParsedMessage>>,
        is_int: bool,
        account_id: HashBytes,
        group_limit: usize,
        group_vert_size: usize,
    ) {
        if self
            .inner
            .insert(msg, is_int, account_id, group_limit, group_vert_size)
        {
            self.filling += 1;
            if self.filling == group_limit {
                self.is_full = true;
            }
        }
    }
}

#[derive(Default)]
pub(super) struct MessageGroupsV1 {
    shard_id: ShardIdent,

    offset: u32,
    max_message_key: QueueKey,
    groups: FastHashMap<u32, MessageGroupContainerV1>,

    int_messages_count: usize,
    ext_messages_count: usize,

    group_limit: usize,
    group_vert_size: usize,
}

impl MessageGroupsV1 {
    pub fn new(shard_id: ShardIdent, group_limit: usize, group_vert_size: usize) -> Self {
        Self {
            shard_id,
            group_limit,
            group_vert_size,
            ..Default::default()
        }
    }

    pub fn reset(&mut self) {
        self.offset = 0;
        self.max_message_key = QueueKey::MIN;
        self.groups.clear();
        self.int_messages_count = 0;
        self.ext_messages_count = 0;
    }

    pub fn offset(&self) -> u32 {
        self.offset
    }

    pub fn max_message_key(&self) -> &QueueKey {
        &self.max_message_key
    }

    pub fn len(&self) -> usize {
        self.groups.len()
    }

    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    pub fn messages_count(&self) -> usize {
        self.int_messages_count + self.ext_messages_count
    }

    pub fn int_messages_count(&self) -> usize {
        self.int_messages_count
    }

    pub fn ext_messages_count(&self) -> usize {
        self.ext_messages_count
    }

    fn increment_counters(&mut self, is_int: bool) {
        if is_int {
            self.int_messages_count += 1;
        } else {
            self.ext_messages_count += 1;
        }
    }

    /// add message adjusting groups,
    pub fn add_message(&mut self, msg: Box<ParsedMessage>) {
        assert_eq!(
            msg.special_origin, None,
            "unexpected special origin in ordinary messages set"
        );

        let (account_id, is_int) = match &msg.info {
            MsgInfo::ExtIn(ExtInMsgInfo { dst, .. }) => {
                (dst.as_std().map(|a| a.address).unwrap_or_default(), false)
            }
            MsgInfo::Int(IntMsgInfo {
                dst, created_lt, ..
            }) => {
                self.max_message_key = self.max_message_key.max(QueueKey {
                    lt: *created_lt,
                    hash: *msg.cell.repr_hash(),
                });
                (dst.as_std().map(|a| a.address).unwrap_or_default(), true)
            }
            MsgInfo::ExtOut(info) => {
                unreachable!("ext out message in ordinary messages set: {info:?}")
            }
        };

        self.increment_counters(is_int);

        let mut offset = self.offset;
        let mut msg = Some(msg);
        loop {
            let group_entry = self.groups.entry(offset).or_default();

            offset += 1;

            if group_entry.is_full() {
                continue;
            }

            group_entry.insert(
                &mut msg,
                is_int,
                account_id,
                self.group_limit,
                self.group_vert_size,
            );

            if msg.is_none() {
                break;
            }
        }

        let labels = [("workchain", self.shard_id.workchain().to_string())];
        metrics::gauge!("tycho_do_collate_msgs_exec_buffer_messages_count", &labels)
            .set(self.messages_count() as f64);
    }

    pub fn first_group_is_full(&self) -> bool {
        if let Some(first_group) = self.groups.get(&self.offset) {
            // FIXME: check if first group is full by stats on adding message
            // let first_group_is_full = first_group.len() >= self.group_limit
            //     && first_group
            //         .inner
            //         .values()
            //         .all(|account_msgs| account_msgs.len() >= self.group_vert_size);
            // first_group_is_full

            first_group.is_full()
        } else {
            false
        }
    }

    pub fn extract_first_group(&mut self) -> Option<MessageGroup> {
        let first_group_opt = self.extract_first_group_inner();
        if first_group_opt.is_some() {
            self.offset += 1;
        }
        if let Some(first_group) = first_group_opt.as_ref() {
            tracing::debug!(target: tracing_targets::COLLATOR,
                "extracted first message group from message_groups buffer: offset={}, buffer int={}, ext={}, group {}",
                self.offset(), self.int_messages_count(), self.ext_messages_count(),
                DisplayMessageGroup(first_group),
            );
        }
        first_group_opt
    }

    fn extract_first_group_inner(&mut self) -> Option<MessageGroup> {
        if let Some(first_group) = self.groups.remove(&self.offset) {
            self.int_messages_count -= first_group.inner.int_messages_count();
            self.ext_messages_count -= first_group.inner.ext_messages_count();

            Some(first_group.inner)
        } else {
            None
        }
    }

    pub fn extract_merged_group(&mut self) -> Option<MessageGroup> {
        let mut merged_group_opt: Option<MessageGroup> = None;
        while let Some(next_group) = self.extract_first_group_inner() {
            if let Some(merged_group) = merged_group_opt.as_mut() {
                for (account_id, mut account_msgs) in next_group {
                    if let Some(existing_account_msgs) = merged_group.get_mut(&account_id) {
                        existing_account_msgs.append(&mut account_msgs);
                    } else {
                        merged_group.insert_raw(account_id, account_msgs);
                    }
                }
            } else {
                self.offset += 1;
                merged_group_opt = Some(next_group);
            }
        }
        if let Some(merged_group) = merged_group_opt.as_ref() {
            tracing::debug!(target: tracing_targets::COLLATOR,
                "extracted merged message group of new messages from message_groups buffer: buffer int={}, ext={}, group {}",
                self.int_messages_count(), self.ext_messages_count(),
                DisplayMessageGroup(merged_group),
            );
        }
        merged_group_opt
    }
}

#[allow(dead_code)]
pub(super) struct DisplayMessageGroups<'a>(pub &'a MessageGroupsV1);

impl std::fmt::Debug for DisplayMessageGroups<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl std::fmt::Display for DisplayMessageGroups<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut m = f.debug_map();
        for (k, v) in self.0.groups.iter() {
            m.entry(k, &DisplayMessageGroup(&v.inner));
        }
        m.finish()
    }
}

#[cfg(test)]
mod tests {
    use everscale_types::cell::{Cell, HashBytes};
    use everscale_types::models::{IntAddr, StdAddr};

    use super::*;

    #[test]
    pub fn groups_v1() {
        let shard_id = ShardIdent::new_full(0);
        let group_vert_size = 10;
        let group_limit = 100;
        let externals_group_count = 25000_u32;
        // let externals_group_count = 2500;
        let externals_group_len = 4;
        let accounts_count = 1000_u32;
        let messages_buffer_limit = 20000;

        let mut count_buffer_limit = 0;
        let mut count_first_group_is_full = 0;
        let mut count_last_groups = 0;
        let mut all_messages_count = 0;
        let mut all_messages_from_group_count = 0;

        let timer = std::time::Instant::now();
        let mut old = MessageGroupsV1::new(shard_id, group_limit, group_vert_size);
        for i in 0..externals_group_count {
            let mut address = HashBytes::ZERO;
            address.0[0] = (i % accounts_count) as u8;
            address.0[1] = ((i % accounts_count) >> 8) as u8;

            let info = MsgInfo::ExtIn(ExtInMsgInfo {
                dst: IntAddr::Std(StdAddr::new(0, address)),
                ..Default::default()
            });

            // let messages_count = externals_group_len + i % accounts_count >> 2;
            let messages_count = externals_group_len;
            for _ in 0..messages_count {
                let msg = Box::new(ParsedMessage {
                    info: info.clone(),
                    dst_in_current_shard: true,
                    cell: Cell::default(),
                    special_origin: None,
                    dequeued: None,
                });
                old.add_message(msg);
                all_messages_count += 1;
            }

            if old.messages_count() >= messages_buffer_limit {
                count_buffer_limit += 1;
                all_messages_from_group_count +=
                    old.extract_first_group().unwrap().messages_count();
            }

            if old.first_group_is_full() {
                count_first_group_is_full += 1;
                all_messages_from_group_count +=
                    old.extract_first_group().unwrap().messages_count();
            }
        }
        while let Some(group) = old.extract_first_group() {
            count_last_groups += 1;
            all_messages_from_group_count += group.messages_count();
        }

        let e = timer.elapsed();
        println!("old: {}ms", e.as_millis());
        println!("old count_buffer_limit = {}", count_buffer_limit);
        println!(
            "old count_first_group_is_full = {}",
            count_first_group_is_full
        );
        println!("new count_last_groups = {}", count_last_groups);
        println!("old all_messages_count = {}", all_messages_count);
        println!(
            "old all_messages_from_group_count = {}",
            all_messages_from_group_count
        );
    }
}
