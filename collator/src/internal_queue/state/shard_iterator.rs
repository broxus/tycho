use anyhow::{Context, Result};
use everscale_types::models::ShardIdent;
use tycho_block_util::queue::QueueKey;
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

impl From<(QueueKey, QueueKey, ShardIdent)> for Range {
    fn from(value: (QueueKey, QueueKey, ShardIdent)) -> Self {
        let from = ShardsInternalMessagesKey::new(value.2, value.0);
        let to = ShardsInternalMessagesKey::new(value.2, value.1);

        Range { from, to }
    }
}

pub enum IterResult<'a> {
    Value(&'a [u8]),
    Skip(Option<ShardsInternalMessagesKey>),
}

pub struct ShardIterator {
    range: Range,
    source: ShardIdent,
    receiver: ShardIdent,
    iterator: OwnedIterator,
}

impl ShardIterator {
    pub fn new(
        source: ShardIdent,
        from: QueueKey,
        to: QueueKey,
        receiver: ShardIdent,
        mut iterator: OwnedIterator,
    ) -> Self {
        iterator.seek(ShardsInternalMessagesKey::new(source, from));

        let range = Range::from((from, to, source));

        Self {
            range,
            receiver,
            source,
            iterator,
        }
    }

    pub fn shift(&mut self) {
        self.iterator.next();
    }

    pub fn current(&mut self) -> Result<Option<IterResult<'_>>> {
        if let Some(key) = self.iterator.key() {
            let key = ShardsInternalMessagesKey::from(key);

            if key.shard_ident != self.source {
                return Ok(None);
            }

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
                Ok(Some(IterResult::Skip(Some(key))))
            };
        }
        Ok(None)
    }
}
