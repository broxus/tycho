use anyhow::Result;
use everscale_types::models::ShardIdent;
use tycho_block_util::queue::QueueKey;
use tycho_storage::InternalQueueMessagesIter;

use crate::types::ShortAddr;

pub enum IterResult<'a> {
    Value(&'a [u8]),
    Skip(Option<(ShardIdent, QueueKey)>),
}

pub struct ShardIterator {
    receiver: ShardIdent,
    iterator: InternalQueueMessagesIter,
}

impl ShardIterator {
    pub fn new(receiver: ShardIdent, mut iterator: InternalQueueMessagesIter) -> Self {
        iterator.seek_to_first();

        Self { receiver, iterator }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<IterResult<'_>>> {
        let Some(msg) = self.iterator.next()? else {
            return Ok(None);
        };

        let short_addr = ShortAddr::new(msg.workchain as i32, msg.prefix);

        Ok(Some(if self.receiver.contains_address(&short_addr) {
            IterResult::Value(msg.message_boc)
        } else {
            IterResult::Skip(Some((msg.key.shard_ident, msg.key.internal_message_key)))
        }))
    }
}
