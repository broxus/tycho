use anyhow::{Context, Result};
use everscale_types::models::ShardIdent;
use tycho_block_util::queue::QueueKey;
use tycho_storage::model::{ShardsInternalMessagesKey, ValueType};
use tycho_storage::owned_iterator::OwnedIterator;
use crate::internal_queue::state::shard_iterator::IterResultValue::Message;

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
    Value(IterResultValue<'a >),
    Skip(Option<ShardsInternalMessagesKey>),
}

pub enum IterResultValue<'a> {
    Message(&'a [u8]),
    DiffEnd(QueueKey)
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

            // TODO !!! check range
            if !self.range.contains(&key) {
                return Ok(None);
            }

            let value = self.iterator.value().context("Failed to get value")?;
            let value_type = value[0].try_into()?;

            return match value_type {
                ValueType::Message => {
                    let dest_workchain = value[1] as i8;
                    let dest_prefix = u64::from_be_bytes(
                        value[2..10]
                            .try_into()
                            .context("Failed to deserialize destination prefix")?,
                    );
                    let short_addr = ShortAddr::new(dest_workchain as i32, dest_prefix);

                    if self.receiver.contains_address(&short_addr) {
                        Ok(Some(IterResult::Value(Message(&value[10..]))))
                    } else {
                        Ok(Some(IterResult::Skip(Some(key))))
                    }
                }
                ValueType::DiffEnd => {
                    println!("DIFF END FOUND");
                    Ok(Some(IterResult::Value(IterResultValue::DiffEnd(key.internal_message_key))))
                }
            }
        }
        Ok(None)
    }
}
