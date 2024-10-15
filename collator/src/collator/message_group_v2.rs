use std::collections::{BTreeMap, VecDeque};

use everscale_types::cell::HashBytes;
use everscale_types::models::{ExtInMsgInfo, IntMsgInfo, MsgInfo, ShardIdent};
use tycho_block_util::queue::QueueKey;
use tycho_util::FastHashMap;

use super::types::{DisplayMessageGroup, MessageGroup, ParsedMessage};
use crate::tracing_targets;

pub struct Message {
    pub inner: Box<ParsedMessage>,
    pub is_internal: bool,
}

#[derive(Default)]
pub(super) struct MessageGroupContainerV2 {
    inner: MessageGroup,
    limit: usize,
    vert_size: usize,
}

impl MessageGroupContainerV2 {
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn messages_count(&self) -> usize {
        self.inner.messages_count()
    }

    pub fn limit_reached(&self) -> bool {
        self.len() == self.limit
    }

    fn insert(&mut self, hash: HashBytes, message: Message) {
        let Message { inner, is_internal } = message;
        self.inner.insert(
            &mut Some(inner),
            is_internal,
            hash,
            self.limit,
            self.vert_size,
        );
    }

    fn is_vert_size_reached(&mut self, hash: HashBytes) -> bool {
        if let Some(messages) = self.inner.get(&hash) {
            messages.len() == self.vert_size
        } else {
            false
        }
    }
}

impl IntoIterator for MessageGroupContainerV2 {
    type Item = (HashBytes, Vec<Box<ParsedMessage>>);
    type IntoIter = std::collections::hash_map::IntoIter<HashBytes, Vec<Box<ParsedMessage>>>;
    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

#[derive(Default)]
pub(super) struct MessageGroupsV2 {
    shard_id: ShardIdent,

    offset: u32,
    max_message_key: QueueKey,

    current_messages: FastHashMap<HashBytes, VecDeque<Message>>,
    new_messages: BTreeMap<HashBytes, VecDeque<Message>>,
    new_messages_full: BTreeMap<HashBytes, VecDeque<Message>>,

    int_messages_count: usize,
    ext_messages_count: usize,

    group_limit: usize,
    group_vert_size: usize,
}

impl MessageGroupsV2 {
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
        self.current_messages = Default::default();
        self.new_messages = Default::default();
        self.new_messages_full = Default::default();
        self.int_messages_count = 0;
        self.ext_messages_count = 0;
    }

    pub fn offset(&self) -> u32 {
        self.offset
    }

    pub fn max_message_key(&self) -> &QueueKey {
        &self.max_message_key
    }

    pub fn is_empty(&self) -> bool {
        self.current_messages.is_empty()
            && self.new_messages.is_empty()
            && self.new_messages_full.is_empty()
    }

    pub fn len(&self) -> usize {
        let current_len: usize = self
            .current_messages
            .values()
            .filter(|m| !m.is_empty())
            .count();
        let new_len_full: usize = self.new_messages_full.len();
        let new_len: usize = self.new_messages.len();

        let mut len = (current_len + new_len + new_len_full) / self.group_limit;

        if (current_len + new_len + new_len_full) % self.group_limit > 0 {
            len += 1;
        }
        len
    }

    pub fn first_group_is_full(&self) -> bool {
        let current_count = self
            .current_messages
            .iter()
            .filter(|(_, m)| m.len() >= self.group_vert_size)
            .count();

        if current_count >= self.group_limit {
            return true;
        }

        let new_count = self.new_messages_full.len();

        if current_count + new_count >= self.group_limit {
            return true;
        }

        false
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

        if let Some(messages) = self.current_messages.get_mut(&account_id) {
            messages.push_back(Message {
                inner: msg,
                is_internal: is_int,
            });
        } else if let Some(messages) = self.new_messages_full.get_mut(&account_id) {
            messages.push_back(Message {
                inner: msg,
                is_internal: is_int,
            });
        } else {
            let messages = self.new_messages.entry(account_id).or_default();
            messages.push_back(Message {
                inner: msg,
                is_internal: is_int,
            });
            if messages.len() == self.group_vert_size {
                let messages = self.new_messages.remove(&account_id).unwrap();
                self.new_messages_full.insert(account_id, messages);
            }
        }

        self.increment_counters(is_int);

        let labels = [("workchain", self.shard_id.workchain().to_string())];
        metrics::gauge!("tycho_do_collate_msgs_exec_buffer_messages_count", &labels)
            .set(self.messages_count() as f64);
    }

    pub fn extract_first_group(&mut self) -> Option<MessageGroup> {
        let first_group_opt = self.extract_first_group_inner();
        if let Some(first_group) = first_group_opt.as_ref() {
            self.offset += 1;
            tracing::debug!(target: tracing_targets::COLLATOR,
                "extracted first message group from message_groups buffer: offset={}, buffer int={}, ext={}, group {}",
                self.offset(), self.int_messages_count(), self.ext_messages_count(),
                DisplayMessageGroup(first_group),
            );
        }
        first_group_opt
    }

    fn extract_first_group_inner(&mut self) -> Option<MessageGroup> {
        let mut group = MessageGroupContainerV2 {
            inner: MessageGroup::default(),
            limit: self.group_limit,
            vert_size: self.group_vert_size,
        };
        let mut hashes_to_remove = vec![];

        'hashes: for (hash, messages) in self.current_messages.iter_mut() {
            if messages.is_empty() {
                hashes_to_remove.push(*hash);
                continue;
            }
            while let Some(message) = messages.pop_front() {
                group.insert(*hash, message);
                if group.is_vert_size_reached(*hash) {
                    break;
                }
            }
            if group.limit_reached() {
                break 'hashes;
            }
        }

        for hash in hashes_to_remove {
            self.current_messages.remove(&hash);
        }

        // take only messages with vert size >= group_vert_size, to fill group faster
        if !group.limit_reached() {
            'hashes: while let Some((hash, mut messages)) = self.new_messages_full.pop_first() {
                while let Some(message) = messages.pop_front() {
                    group.insert(hash, message);
                    if group.is_vert_size_reached(hash) {
                        break;
                    }
                }
                self.current_messages.insert(hash, messages);

                if group.limit_reached() {
                    break 'hashes;
                }
            }
        }

        // take all messages remaining
        if !group.limit_reached() {
            'hashes: while let Some((hash, mut messages)) = self.new_messages.pop_first() {
                while let Some(message) = messages.pop_front() {
                    group.insert(hash, message);
                    if group.is_vert_size_reached(hash) {
                        break;
                    }
                }
                self.current_messages.insert(hash, messages);
                if group.limit_reached() {
                    break 'hashes;
                }
            }
        }

        if group.messages_count() == 0 {
            None
        } else {
            self.int_messages_count -= group.inner.int_messages_count();
            self.ext_messages_count -= group.inner.ext_messages_count();
            Some(group.inner)
        }
    }

    pub fn extract_merged_group(&mut self) -> Option<MessageGroup> {
        let mut messages: Vec<(HashBytes, VecDeque<Message>)> =
            self.current_messages.drain().collect();

        while let Some((key, val)) = self.new_messages_full.pop_first() {
            messages.push((key, val));
        }

        while let Some((key, val)) = self.new_messages.pop_first() {
            messages.push((key, val));
        }

        if messages.is_empty() {
            return None;
        }

        let group = MessageGroup::new(
            messages
                .into_iter()
                .map(|(h, m)| (h, m.into_iter().map(|m| m.inner).collect()))
                .collect(),
            self.int_messages_count,
            self.ext_messages_count,
        );

        tracing::debug!(target: tracing_targets::COLLATOR,
            "extracted merged message group of new messages from message_groups buffer: buffer int={}, ext={}, group {}",
            self.int_messages_count(), self.ext_messages_count(),
            DisplayMessageGroup(&group),
        );

        self.int_messages_count = 0;
        self.ext_messages_count = 0;

        Some(group)
    }
}

#[cfg(test)]
mod tests {
    use everscale_types::cell::Cell;
    use everscale_types::models::{IntAddr, StdAddr};

    use super::*;

    #[test]
    pub fn groups_v2() {
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
        let mut new = MessageGroupsV2::new(shard_id, group_limit, group_vert_size);
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
                new.add_message(msg);
                all_messages_count += 1;
            }

            if new.messages_count() >= messages_buffer_limit {
                count_buffer_limit += 1;
                all_messages_from_group_count +=
                    new.extract_first_group().unwrap().messages_count();
            }

            if new.first_group_is_full() {
                count_first_group_is_full += 1;
                all_messages_from_group_count +=
                    new.extract_first_group().unwrap().messages_count();
            }
        }
        while let Some(group) = new.extract_first_group() {
            count_last_groups += 1;
            all_messages_from_group_count += group.messages_count();
        }
        let e = timer.elapsed();
        println!("new: {}ms", e.as_millis());
        println!("new count_buffer_limit = {}", count_buffer_limit);
        println!(
            "new count_first_group_is_full = {}",
            count_first_group_is_full
        );
        println!("new count_last_groups = {}", count_last_groups);
        println!("new all_messages_count = {}", all_messages_count);
        println!(
            "new all_messages_from_group_count = {}",
            all_messages_from_group_count
        );
    }

    #[test]
    pub fn functional_groups_v2() {
        let shard_id = ShardIdent::new_full(0);
        let group_vert_size: usize = 2;
        let group_limit = 3;
        let mut new = MessageGroupsV2::new(shard_id, group_limit, group_vert_size);

        let mut address = HashBytes::ZERO;

        println!("A1,A2,B1,C1,D1,D2,E1,F1,A3,B2,B3,G1,C2,E2,E3");
        //  A1
        address.0[0] = 0_u8;
        let a1 = create_new_parsed_message(address);
        new.add_message(a1);
        extract_first_group_if_is_full_or_exceed_limit(&mut new);

        // A2
        address.0[0] = 0_u8;
        let a2 = create_new_parsed_message(address);
        new.add_message(a2);
        extract_first_group_if_is_full_or_exceed_limit(&mut new);

        // B1
        address.0[0] = 1_u8;
        let b1 = create_new_parsed_message(address);
        new.add_message(b1);
        extract_first_group_if_is_full_or_exceed_limit(&mut new);

        // C1
        address.0[0] = 2_u8;
        let c1 = create_new_parsed_message(address);
        new.add_message(c1);
        extract_first_group_if_is_full_or_exceed_limit(&mut new);

        // D1
        address.0[0] = 3_u8;
        let d1 = create_new_parsed_message(address);
        new.add_message(d1);
        extract_first_group_if_is_full_or_exceed_limit(&mut new);

        // D2
        address.0[0] = 3_u8;
        let d2 = create_new_parsed_message(address);
        new.add_message(d2);
        extract_first_group_if_is_full_or_exceed_limit(&mut new);

        // E1
        address.0[0] = 4_u8;
        let e1 = create_new_parsed_message(address);
        new.add_message(e1);
        extract_first_group_if_is_full_or_exceed_limit(&mut new);

        // F1
        address.0[0] = 5_u8;
        let f1 = create_new_parsed_message(address);
        new.add_message(f1);
        extract_first_group_if_is_full_or_exceed_limit(&mut new);

        // A3,
        address.0[0] = 0_u8;
        let a3 = create_new_parsed_message(address);
        new.add_message(a3);
        extract_first_group_if_is_full_or_exceed_limit(&mut new);

        // B2
        address.0[0] = 1_u8;
        let b2 = create_new_parsed_message(address);
        new.add_message(b2);
        extract_first_group_if_is_full_or_exceed_limit(&mut new);

        // B3
        address.0[0] = 1_u8;
        let b3 = create_new_parsed_message(address);
        new.add_message(b3);
        extract_first_group_if_is_full_or_exceed_limit(&mut new);

        // G1
        address.0[0] = 6_u8;
        let g1 = create_new_parsed_message(address);
        new.add_message(g1);
        extract_first_group_if_is_full_or_exceed_limit(&mut new);

        // C2
        address.0[0] = 2_u8;
        let c2 = create_new_parsed_message(address);
        new.add_message(c2);
        extract_first_group_if_is_full_or_exceed_limit(&mut new);

        // E2
        address.0[0] = 4_u8;
        let e2 = create_new_parsed_message(address);
        new.add_message(e2);
        extract_first_group_if_is_full_or_exceed_limit(&mut new);

        // E3
        address.0[0] = 4_u8;
        let e3 = create_new_parsed_message(address);
        new.add_message(e3);

        while let Some(group) = new.extract_first_group() {
            print_group(group);
            println!("offset - {}", new.offset());
        }
    }

    fn create_new_parsed_message(address: HashBytes) -> Box<ParsedMessage> {
        let info = MsgInfo::ExtIn(ExtInMsgInfo {
            dst: IntAddr::Std(StdAddr::new(0, address)),
            ..Default::default()
        });
        Box::new(ParsedMessage {
            info: info.clone(),
            dst_in_current_shard: true,
            cell: Cell::default(),
            special_origin: None,
            dequeued: None,
        })
    }

    fn extract_first_group_if_is_full_or_exceed_limit(new: &mut MessageGroupsV2) {
        let messages_buffer_limit = 10;
        if new.first_group_is_full() || new.messages_count() >= messages_buffer_limit {
            let group = new.extract_first_group().unwrap();
            print_group(group);
            println!("offset - {}", new.offset());
        }
    }

    fn print_group(group: MessageGroup) {
        for (address, m) in group {
            let s: &str = match address.0[0] {
                0 => "A",
                1 => "B",
                2 => "C",
                3 => "D",
                4 => "E",
                5 => "F",
                6 => "G",
                _ => unreachable!(),
            };
            println!("{} - {} messages", s, m.len());
        }
        println!("=================");
    }
}
