use std::sync::Arc;

use everscale_types::cell::HashBytes;
use indexmap::map::Entry;
use indexmap::IndexMap;

use crate::mempool::types::ExternalMessage;

pub struct ExternalMessageCache {
    external_message_cache: IndexMap<HashBytes, u32>,
    round_window_size: u16,
}

impl ExternalMessageCache {
    pub fn new(round_window_size: u16) -> Self {
        Self {
            external_message_cache: IndexMap::new(),
            round_window_size,
        }
    }
    pub fn add_dedup_messages(
        &mut self,
        round_id: u32,
        mut messages: Vec<Arc<ExternalMessage>>,
    ) -> Vec<Arc<ExternalMessage>> {
        messages.retain(
            |message| match self.external_message_cache.entry(*message.hash()) {
                Entry::Vacant(e) => {
                    e.insert(round_id);
                    true
                }
                Entry::Occupied(_) => false,
            },
        );

        let mut split_index: usize = 0;

        for (index, (_, value)) in self.external_message_cache.iter().enumerate() {
            split_index = index;
            if *value > round_id.saturating_sub(self.round_window_size as u32) {
                break;
            }
        }

        self.external_message_cache = self.external_message_cache.split_off(split_index);

        messages
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use everscale_types::cell::CellFamily;
    use everscale_types::models::ExtInMsgInfo;

    use crate::mempool::external_message_cache::ExternalMessageCache;
    use crate::mempool::types::ExternalMessage;

    #[test]
    pub fn cache_test() {
        let mut cache = ExternalMessageCache::new(100);
        let executed_rounds = 1000;
        for i in 0..executed_rounds {
            let mut messages = Vec::new();
            if i < 100 {
                let message = Arc::new(ExternalMessage::new(
                    everscale_types::cell::Cell::empty_cell(),
                    ExtInMsgInfo::default(),
                ));
                let message2 = Arc::new(ExternalMessage::new(
                    everscale_types::cell::Cell::empty_cell(),
                    ExtInMsgInfo::default(),
                ));
                messages.push(message);
                messages.push(message2);
            }

            let mut builder = everscale_types::cell::CellBuilder::new();
            builder.store_bit_one().expect("OK");
            builder.store_bit_one().expect("OK");
            let message = Arc::new(ExternalMessage::new(
                builder.build().expect("OK"),
                ExtInMsgInfo::default(),
            ));
            messages.push(message);

            let message2 = Arc::new(ExternalMessage::new(
                everscale_types::cell::Cell::empty_cell(),
                ExtInMsgInfo::default(),
            ));
            messages.push(message2);

            if !messages.is_empty() {
                let _ = cache.add_dedup_messages(i, messages);
            }
        }

        assert_eq!(cache.external_message_cache.len(), 2);
    }
}
