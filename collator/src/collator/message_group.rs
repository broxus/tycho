use std::collections::hash_map::Entry;

use everscale_types::cell::HashBytes;
use everscale_types::models::{ExtInMsgInfo, IntMsgInfo, MsgInfo, ShardIdent};
use tycho_block_util::queue::QueueKey;
use tycho_util::FastHashMap;

use super::types::ParsedMessage;
use crate::tracing_targets;

#[derive(Default)]
pub(super) struct MessageGroups {
    shard_id: ShardIdent,

    offset: u32,
    max_message_key: QueueKey,
    groups: FastHashMap<u32, MessageGroup>,

    int_messages_count: usize,
    ext_messages_count: usize,

    group_limit: usize,
    group_vert_size: usize,
}

impl MessageGroups {
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

    fn incriment_counters(&mut self, is_int: bool) {
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

        self.incriment_counters(is_int);

        let mut offset = self.offset;
        loop {
            let group_entry = self.groups.entry(offset).or_default();

            if group_entry.is_full {
                offset += 1;
                continue;
            }

            let group_len = group_entry.inner.len();
            match group_entry.inner.entry(account_id) {
                Entry::Vacant(entry) => {
                    if group_len < self.group_limit {
                        entry.insert(vec![msg]);
                        group_entry.incriment_counters(is_int);
                        break;
                    }

                    offset += 1;
                }
                Entry::Occupied(mut entry) => {
                    let msgs = entry.get_mut();
                    if msgs.len() < self.group_vert_size {
                        msgs.push(msg);

                        if msgs.len() == self.group_vert_size {
                            group_entry.filling += 1;
                            if group_entry.filling == self.group_limit {
                                group_entry.is_full = true;
                            }
                        }

                        group_entry.incriment_counters(is_int);

                        break;
                    }

                    offset += 1;
                }
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

            first_group.is_full
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
            self.int_messages_count -= first_group.int_messages_count;
            self.ext_messages_count -= first_group.ext_messages_count;

            Some(first_group)
        } else {
            None
        }
    }

    pub fn extract_merged_group(&mut self) -> Option<MessageGroup> {
        let mut merged_group_opt: Option<MessageGroup> = None;
        while let Some(next_group) = self.extract_first_group_inner() {
            if let Some(merged_group) = merged_group_opt.as_mut() {
                for (account_id, mut account_msgs) in next_group.inner {
                    if let Some(existing_account_msgs) = merged_group.inner.get_mut(&account_id) {
                        existing_account_msgs.append(&mut account_msgs);
                    } else {
                        merged_group.inner.insert(account_id, account_msgs);
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

// pub(super) type MessageGroup = FastHashMap<HashBytes, Vec<Box<ParsedMessage>>>;
#[derive(Default)]
pub(super) struct MessageGroup {
    #[allow(clippy::vec_box)]
    inner: FastHashMap<HashBytes, Vec<Box<ParsedMessage>>>,
    int_messages_count: usize,
    ext_messages_count: usize,
    filling: usize,
    is_full: bool,
}

impl MessageGroup {
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn messages_count(&self) -> usize {
        self.int_messages_count + self.ext_messages_count
    }

    fn incriment_counters(&mut self, is_int: bool) {
        if is_int {
            self.int_messages_count += 1;
        } else {
            self.ext_messages_count += 1;
        }
    }
}

impl IntoIterator for MessageGroup {
    type Item = (HashBytes, Vec<Box<ParsedMessage>>);
    type IntoIter = std::collections::hash_map::IntoIter<HashBytes, Vec<Box<ParsedMessage>>>;
    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
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
            self.0.int_messages_count, self.0.ext_messages_count
        )?;
        let mut l = f.debug_list();
        for messages in self.0.inner.values() {
            l.entry(&messages.len());
        }
        l.finish()
    }
}

#[allow(dead_code)]
pub(super) struct DisplayMessageGroups<'a>(pub &'a MessageGroups);

impl std::fmt::Debug for DisplayMessageGroups<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl std::fmt::Display for DisplayMessageGroups<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut m = f.debug_map();
        for (k, v) in self.0.groups.iter() {
            m.entry(k, &DisplayMessageGroup(v));
        }
        m.finish()
    }
}

#[cfg(test)]
mod tests {
    use everscale_types::cell::Cell;
    use everscale_types::models::{IntAddr, StdAddr};

    use super::*;

    #[test]
    pub fn groups_new() {
        let shard_id = ShardIdent::new_full(0);
        let group_vert_size = 10;
        let group_limit = 100;
        // let externals_group_count = 25000;
        let externals_group_count = 2500;
        let externals_group_len = 4;
        let accounts_count = 1000;
        let messages_buffer_limit = 20000;

        let mut count_buffer_limit = 0;
        let mut count_first_group_is_full = 0;
        let mut count_last_groups = 0;
        let mut all_messages_count = 0;
        let mut all_messages_from_group_count = 0;

        let timer = std::time::Instant::now();
        let mut old = MessageGroups::new(shard_id, group_limit, group_vert_size);
        for i in 0..externals_group_count {
            let mut address = HashBytes::ZERO;
            address.0[0] = (i % accounts_count) as u8;
            address.0[1] = (i % accounts_count >> 8) as u8;

            let info = MsgInfo::ExtIn(ExtInMsgInfo {
                dst: IntAddr::Std(StdAddr::new(0, address)),
                ..Default::default()
            });

            let messages_count = externals_group_len + i % accounts_count >> 2;
            // let messages_count = externals_group_len;
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
