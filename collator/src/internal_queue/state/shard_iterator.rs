use anyhow::Result;
use everscale_types::models::ShardIdent;
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};
use tycho_storage::model::ShardsInternalMessagesKey;
use tycho_storage::InternalQueueMessagesIter;

use crate::types::ShortAddr;

#[derive(Clone, Debug)]
struct Range {
    from: ShardsInternalMessagesKey,
    to: ShardsInternalMessagesKey,
}

impl Range {
    pub fn contains(&self, key: &ShardsInternalMessagesKey) -> bool {
        key > &self.from && key <= &self.to
    }
}

impl From<(QueuePartitionIdx, ShardIdent, QueueKey, QueueKey)> for Range {
    fn from(value: (QueuePartitionIdx, ShardIdent, QueueKey, QueueKey)) -> Self {
        let (partition, shard_ident, from, to) = value;

        let from = ShardsInternalMessagesKey::new(partition, shard_ident, from);
        let to = ShardsInternalMessagesKey::new(partition, shard_ident, to);

        Range { from, to }
    }
}

pub enum IterResult<'a> {
    Value(&'a [u8]),
    Skip(Option<(ShardIdent, QueueKey)>),
}

pub struct ShardIterator {
    range: Range,
    receiver: ShardIdent,
    iterator: InternalQueueMessagesIter,
}

impl ShardIterator {
    pub fn new(
        partition: QueuePartitionIdx,
        shard_ident: ShardIdent,
        from: QueueKey,
        to: QueueKey,
        receiver: ShardIdent,
        mut iterator: InternalQueueMessagesIter,
    ) -> Self {
        iterator.seek(&ShardsInternalMessagesKey::new(
            partition,
            shard_ident,
            from,
        ));

        let range = Range::from((partition, shard_ident, from, to));

        Self {
            range,
            receiver,
            iterator,
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<IterResult<'_>>> {
        let Some(msg) = self.iterator.next()? else {
            return Ok(None);
        };

        // skip first key if it is equal to `from`
        if msg.key == self.range.from {
            return Ok(Some(IterResult::Skip(None)));
        }

        if !self.range.contains(&msg.key) {
            return Ok(None);
        }

        let short_addr = ShortAddr::new(msg.workchain as i32, msg.prefix);

        Ok(Some(if self.receiver.contains_address(&short_addr) {
            IterResult::Value(msg.message_boc)
        } else {
            IterResult::Skip(Some((msg.key.shard_ident, msg.key.internal_message_key)))
        }))
    }
}
