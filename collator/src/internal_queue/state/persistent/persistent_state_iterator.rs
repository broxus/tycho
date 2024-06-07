use std::sync::Arc;

use anyhow::{bail, Result};
use everscale_types::boc::Boc;
use everscale_types::cell::{Cell, Load};
use everscale_types::models::{IntMsgInfo, Message, MsgInfo, ShardIdent};
use tycho_storage::owned_iterator::OwnedIterator;
use tycho_storage::InternalMessageKey;

use crate::internal_queue::state::state_iterator::{MessageWithSource, StateIterator};
use crate::internal_queue::types::EnqueuedMessage;
use crate::types::IntAdrExt;

pub struct PersistentStateIterator {
    iter: OwnedIterator,
    receiver: ShardIdent,
}

impl PersistentStateIterator {
    pub fn new(iter: OwnedIterator, receiver: ShardIdent) -> Self {
        Self { iter, receiver }
    }

    fn load_message_from_value(value: &[u8]) -> Result<(IntMsgInfo, Cell)> {
        let cell = Boc::decode(value)?;
        let message = Message::load_from(&mut cell.as_slice().unwrap())?;

        match message.info {
            MsgInfo::Int(info) => Ok((info, cell)),
            _ => bail!("Expected internal message"),
        }
    }

    fn create_message_with_source(
        info: IntMsgInfo,
        cell: Cell,
        shard: ShardIdent,
    ) -> Arc<MessageWithSource> {
        let hash = *cell.repr_hash();
        let enqueued_message = EnqueuedMessage { info, cell, hash };
        Arc::new(MessageWithSource::new(shard, Arc::new(enqueued_message)))
    }
}

impl StateIterator for PersistentStateIterator {
    fn next(&mut self) -> Result<Option<Arc<MessageWithSource>>> {
        if let (Some(key), Some(value)) = (self.iter.key(), self.iter.value()) {
            let key = InternalMessageKey::from(key);
            let (info, cell) = Self::load_message_from_value(value)?;

            if self.receiver.contains_account(&info.dst.get_address()) {
                let message_with_source =
                    Self::create_message_with_source(info, cell, key.shard_ident);
                self.iter.next();
                Ok(Some(message_with_source))
            } else {
                self.iter.next();
                self.next()
            }
        } else {
            Ok(None)
        }
    }

    fn peek(&self) -> Result<Option<Arc<MessageWithSource>>> {
        if let (Some(key), Some(value)) = (self.iter.key(), self.iter.value()) {
            let message_key = InternalMessageKey::from(key);
            let source_shard = message_key.shard_ident;

            let (info, cell) = Self::load_message_from_value(value)?;

            if self.receiver.contains_account(&info.dst.get_address()) {
                let message_with_source =
                    Self::create_message_with_source(info, cell, source_shard);
                Ok(Some(message_with_source))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}
