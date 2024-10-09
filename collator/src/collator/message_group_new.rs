use std::collections::{BTreeMap, HashSet, VecDeque};

use everscale_types::cell::HashBytes;
use everscale_types::models::{ExtInMsgInfo, IntMsgInfo, MsgInfo, ShardIdent};
use tycho_block_util::queue::QueueKey;
use tycho_util::FastHashMap;

use super::types::ParsedMessage;
use crate::tracing_targets;

pub struct Message {
    pub inner: Box<ParsedMessage>,
    pub is_internal: bool,
}

#[derive(Default)]
pub(super) struct MessageGroupsNew {
    shard_id: ShardIdent,

    offset: u32,
    max_message_key: QueueKey,

    current_messages: FastHashMap<HashBytes, VecDeque<Message>>,
    new_messages: BTreeMap<HashBytes, VecDeque<Message>>,

    int_messages_count: usize,
    ext_messages_count: usize,

    group_limit: usize,
    group_vert_size: usize,
}

impl MessageGroupsNew {
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
        self.current_messages.is_empty() && self.new_messages.is_empty()
    }

    pub fn len(&self) -> usize {
        let current_len: usize = self.current_messages.values().map(|m| m.len()).sum();

        let new_len: usize = self.new_messages.values().map(|m| m.len()).sum();

        let mut len = (current_len + new_len) / self.group_limit;

        if (current_len + new_len) % self.group_limit > 0 {
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

        let new_count = self
            .new_messages
            .iter()
            .filter(|(_, m)| m.len() >= self.group_vert_size)
            .count();

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
        } else {
            let messages = self.new_messages.entry(account_id).or_default();
            messages.push_back(Message {
                inner: msg,
                is_internal: is_int,
            });
        }

        self.increment_counters(is_int);

        let labels = [("workchain", self.shard_id.workchain().to_string())];
        metrics::gauge!("tycho_do_collate_msgs_exec_buffer_messages_count", &labels)
            .set(self.messages_count() as f64);
    }

    pub fn extract_first_group(&mut self) -> Option<MessageGroupNew> {
        let first_group_opt = self.extract_first_group_inner();
        if let Some(first_group) = first_group_opt.as_ref() {
            self.offset += 1;
            tracing::debug!(target: tracing_targets::COLLATOR,
                "extracted first message group from message_groups buffer: offset={}, buffer int={}, ext={}, group {}",
                self.offset(), self.int_messages_count(), self.ext_messages_count(),
                DisplayMessageGroupNew(first_group),
            );
        }
        first_group_opt
    }

    fn extract_first_group_inner(&mut self) -> Option<MessageGroupNew> {
        let mut group = MessageGroupNew {
            inner: FastHashMap::default(),
            int_messages_count: 0,
            ext_messages_count: 0,
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
            if group.is_full() {
                break 'hashes;
            }
        }

        for hash in hashes_to_remove {
            self.current_messages.remove(&hash);
        }

        if !group.is_full() {
            let mut new_current_messages: HashSet<HashBytes> = HashSet::default();
            'hashes: for (hash, messages) in self
                .new_messages
                .iter_mut()
                .filter(|(_, m)| m.len() >= self.group_vert_size)
            {
                while let Some(message) = messages.pop_front() {
                    group.insert(*hash, message);
                    new_current_messages.insert(*hash);
                    if group.is_vert_size_reached(*hash) {
                        break;
                    }
                }
                if group.is_full() {
                    break 'hashes;
                }
            }

            for hash in new_current_messages {
                self.current_messages
                    .insert(hash, self.new_messages.remove(&hash).unwrap());
            }
        }

        if !group.is_full() {
            let mut new_current_messages: HashSet<HashBytes> = HashSet::default();
            'hashes: for (hash, messages) in self.new_messages.iter_mut() {
                if group.is_vert_size_reached(*hash) {
                    continue;
                }
                while let Some(message) = messages.pop_front() {
                    group.insert(*hash, message);
                    new_current_messages.insert(*hash);
                    if group.is_vert_size_reached(*hash) {
                        break;
                    }
                }
                if group.is_full() {
                    break 'hashes;
                }
            }

            for hash in new_current_messages {
                self.current_messages
                    .insert(hash, self.new_messages.remove(&hash).unwrap());
            }
        }

        if group.messages_count() == 0 {
            None
        } else {
            self.int_messages_count -= group.int_messages_count;
            self.ext_messages_count -= group.ext_messages_count;
            Some(group)
        }
    }

    pub fn extract_merged_group(&mut self) -> Option<MessageGroupNew> {
        let mut merged_group_opt: Option<MessageGroupNew> = None;
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
                DisplayMessageGroupNew(merged_group),
            );
        }
        merged_group_opt
    }
}

#[derive(Default)]
pub(super) struct MessageGroupNew {
    #[allow(clippy::vec_box)]
    inner: FastHashMap<HashBytes, Vec<Box<ParsedMessage>>>,
    int_messages_count: usize,
    ext_messages_count: usize,
    limit: usize,
    vert_size: usize,
}

impl MessageGroupNew {
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn messages_count(&self) -> usize {
        self.int_messages_count + self.ext_messages_count
    }

    pub fn is_full(&self) -> bool {
        self.len() == self.limit
    }

    fn increment_counters(&mut self, is_int: bool) {
        if is_int {
            self.int_messages_count += 1;
        } else {
            self.ext_messages_count += 1;
        }
    }
    fn insert(&mut self, hash: HashBytes, message: Message) {
        let messages = self.inner.entry(hash).or_default();
        let Message { inner, is_internal } = message;
        messages.push(inner);
        self.increment_counters(is_internal);
    }

    fn is_vert_size_reached(&mut self, hash: HashBytes) -> bool {
        if let Some(messages) = self.inner.get(&hash) {
            messages.len() == self.vert_size
        } else {
            false
        }
    }
}

impl IntoIterator for MessageGroupNew {
    type Item = (HashBytes, Vec<Box<ParsedMessage>>);
    type IntoIter = std::collections::hash_map::IntoIter<HashBytes, Vec<Box<ParsedMessage>>>;
    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

pub(super) struct DisplayMessageGroupNew<'a>(pub &'a MessageGroupNew);

impl std::fmt::Debug for DisplayMessageGroupNew<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl std::fmt::Display for DisplayMessageGroupNew<'_> {
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
pub(super) struct DisplayMessageGroupsNew<'a>(pub &'a MessageGroupsNew);

impl std::fmt::Debug for DisplayMessageGroupsNew<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl std::fmt::Display for DisplayMessageGroupsNew<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut m = f.debug_map();
        for (k, v) in self.0.current_messages.iter() {
            m.entry(k, &v.len());
        }
        for (k, v) in self.0.new_messages.iter() {
            m.entry(k, &v.len());
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

        let mut timer = std::time::Instant::now();
        let mut new = MessageGroupsNew::new(shard_id, group_limit, group_vert_size);
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
    pub fn functional_groups_new() {
        let shard_id = ShardIdent::new_full(0);
        let group_vert_size: usize = 2;
        let group_limit = 3;
        let mut new = MessageGroupsNew::new(shard_id, group_limit, group_vert_size);

        let mut address = HashBytes::ZERO;

        println!("A1,A2,B1,C1,D1,D2,E1,F1,A3,B2,B3,G1,C2,E2,E3");
        //  A1
        address.0[0] = 0 as u8;
        let a1 = create_new_parsed_message(address);
        new.add_message(a1);
        extract_first_group_if_is_full_or_exceed_limit(&mut new);

        // A2
        address.0[0] = 0 as u8;
        let a2 = create_new_parsed_message(address);
        new.add_message(a2);
        extract_first_group_if_is_full_or_exceed_limit(&mut new);

        // B1
        address.0[0] = 1 as u8;
        let b1 = create_new_parsed_message(address);
        new.add_message(b1);
        extract_first_group_if_is_full_or_exceed_limit(&mut new);

        // C1
        address.0[0] = 2 as u8;
        let c1 = create_new_parsed_message(address);
        new.add_message(c1);
        extract_first_group_if_is_full_or_exceed_limit(&mut new);

        // D1
        address.0[0] = 3 as u8;
        let d1 = create_new_parsed_message(address);
        new.add_message(d1);
        extract_first_group_if_is_full_or_exceed_limit(&mut new);

        // D2
        address.0[0] = 3 as u8;
        let d2 = create_new_parsed_message(address);
        new.add_message(d2);
        extract_first_group_if_is_full_or_exceed_limit(&mut new);

        // E1
        address.0[0] = 4 as u8;
        let e1 = create_new_parsed_message(address);
        new.add_message(e1);
        extract_first_group_if_is_full_or_exceed_limit(&mut new);

        // F1
        address.0[0] = 5 as u8;
        let f1 = create_new_parsed_message(address);
        new.add_message(f1);
        extract_first_group_if_is_full_or_exceed_limit(&mut new);

        // A3,
        address.0[0] = 0 as u8;
        let a3 = create_new_parsed_message(address);
        new.add_message(a3);
        extract_first_group_if_is_full_or_exceed_limit(&mut new);

        // B2
        address.0[0] = 1 as u8;
        let b2 = create_new_parsed_message(address);
        new.add_message(b2);
        extract_first_group_if_is_full_or_exceed_limit(&mut new);

        // B3
        address.0[0] = 1 as u8;
        let b3 = create_new_parsed_message(address);
        new.add_message(b3);
        extract_first_group_if_is_full_or_exceed_limit(&mut new);

        // G1
        address.0[0] = 6 as u8;
        let g1 = create_new_parsed_message(address);
        new.add_message(g1);
        extract_first_group_if_is_full_or_exceed_limit(&mut new);

        // C2
        address.0[0] = 2 as u8;
        let c2 = create_new_parsed_message(address);
        new.add_message(c2);
        extract_first_group_if_is_full_or_exceed_limit(&mut new);

        // E2
        address.0[0] = 4 as u8;
        let e2 = create_new_parsed_message(address);
        new.add_message(e2);
        extract_first_group_if_is_full_or_exceed_limit(&mut new);

        // E3
        address.0[0] = 4 as u8;
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

    fn extract_first_group_if_is_full_or_exceed_limit(new: &mut MessageGroupsNew) {
        let messages_buffer_limit = 10;
        if new.first_group_is_full() {
            let group = new.extract_first_group().unwrap();
            print_group(group);
            println!("offset - {}", new.offset());
        }
    }

    fn print_group(group: MessageGroupNew) {
        for (address, m) in group.into_iter() {
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
