use anyhow::{Context, Result};
use everscale_types::models::ShardIdent;
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};
use tycho_storage::model::ShardsInternalMessagesKey;
use tycho_storage::owned_iterator::OwnedIterator;

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
    iterator: OwnedIterator,
}

impl ShardIterator {
    pub fn new(
        partition: QueuePartitionIdx,
        shard_ident: ShardIdent,
        from: QueueKey,
        to: QueueKey,
        receiver: ShardIdent,
        mut iterator: OwnedIterator,
    ) -> Self {
        iterator.seek(ShardsInternalMessagesKey::new(partition, shard_ident, from));

        let range = Range::from((partition, shard_ident, from, to));

        Self {
            range,
            receiver,
            iterator,
        }
    }

    pub fn shift(&mut self) {
        self.iterator.next();
    }

    pub fn current(&mut self) -> Result<Option<IterResult<'_>>> {
        if let Some(key) = self.iterator.key() {
            let key = ShardsInternalMessagesKey::from(key);

            // skip first key if it is equal to `from`
            if key == self.range.from {
                return Ok(Some(IterResult::Skip(None)));
            }

            if !self.range.contains(&key) {
                return Ok(None);
            }

            let value = self.iterator.value().context("Failed to get value")?;
            let dest_workchain = value[0] as i8;
            let dest_prefix = u64::from_be_bytes(
                value[1..9]
                    .try_into()
                    .context("Failed to deserialize destination prefix")?,
            );
            let short_addr = ShortAddr::new(dest_workchain as i32, dest_prefix);

            return if self.receiver.contains_address(&short_addr) {
                Ok(Some(IterResult::Value(&value[9..])))
            } else {
                Ok(Some(IterResult::Skip(Some((
                    key.shard_ident,
                    key.internal_message_key,
                )))))
            };
        }
        Ok(None)
    }
}
