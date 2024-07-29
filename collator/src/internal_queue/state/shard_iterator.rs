use anyhow::{Context, Result};
use everscale_types::models::ShardIdent;
use tycho_storage::model::ShardsInternalMessagesKey;
use tycho_storage::owned_iterator::OwnedIterator;

use crate::internal_queue::types::InternalMessageKey;
use crate::types::ShortAddr;

#[derive(Clone)]
struct Range {
    from: ShardsInternalMessagesKey,
    to: ShardsInternalMessagesKey,
}

pub enum IterResult<'a> {
    Value(&'a [u8]),
    Skip,
    End,
}

pub struct ShardIterator {
    range: Range,
    receiver: ShardIdent,
    iterator: OwnedIterator,
    current_position: Option<InternalMessageKey>,
}

impl ShardIterator {
    pub fn new(
        shard_ident: ShardIdent,
        from: InternalMessageKey,
        to: InternalMessageKey,
        receiver: ShardIdent,
        mut iterator: OwnedIterator,
    ) -> Self {
        iterator.seek(ShardsInternalMessagesKey::new(
            shard_ident,
            from.clone().into(),
        ));

        Self {
            range: Range {
                from: ShardsInternalMessagesKey::new(shard_ident, from.clone().into()),
                to: ShardsInternalMessagesKey::new(shard_ident, to.into()),
            },
            receiver,
            iterator,
            current_position: None,
        }
    }

    pub fn shift_position(&mut self) {
        self.iterator.next();
    }

    pub fn current_position(&self) -> Option<InternalMessageKey> {
        self.current_position.clone()
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<IterResult<'_>> {
        if let Some(key) = self.iterator.key() {
            let key = ShardsInternalMessagesKey::from(key);

            if key <= self.range.from {
                return Ok(IterResult::Skip);
            }

            if key > self.range.to {
                return Ok(IterResult::End);
            }

            self.current_position = Some(key.internal_message_key.clone().into());

            let value = self.iterator.value().context("Failed to get value")?;
            let dest_workchain = value[0] as i32;
            let dest_prefix = u64::from_be_bytes(
                value[1..9]
                    .try_into()
                    .context("Failed to deserialize destination prefix")?,
            );
            let short_addr = ShortAddr::new(dest_workchain, dest_prefix);

            return if self.receiver.contains_address(&short_addr) {
                Ok(IterResult::Value(&value[9..]))
            } else {
                Ok(IterResult::Skip)
            };
        }
        Ok(IterResult::End)
    }
}
