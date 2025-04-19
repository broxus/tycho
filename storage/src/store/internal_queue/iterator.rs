use anyhow::Result;
use weedb::OwnedRawIterator;

use crate::model::{InternalQueueMessage, ShardsInternalMessagesKey};
use crate::util::StoredValue;

/// Iterator for internal queue messages.
pub struct InternalQueueMessagesIter {
    pub inner: OwnedRawIterator,
    pub first: bool,
}

impl InternalQueueMessagesIter {
    /// Moves the iterator to the specified key.
    ///
    /// # Arguments
    /// * `key` - The key to seek to.
    pub fn seek(&mut self, key: &ShardsInternalMessagesKey) {
        self.inner.seek(key.to_vec());
        self.first = true;
    }

    /// Moves the iterator to the first element.
    pub fn seek_to_first(&mut self) {
        self.inner.seek_to_first();
        self.first = true;
    }

    /// Retrieves the next message from the iterator.
    ///
    /// # Returns
    /// * `Ok(Some(InternalQueueMessage<'_>))` if there is a next message.
    /// * `Ok(None)` if the iterator has reached the end.
    /// * `Err(anyhow::Error)` if an error occurs.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<InternalQueueMessage<'_>>> {
        if !std::mem::take(&mut self.first) {
            self.inner.next();
        }

        let Some((key, value)) = self.inner.item() else {
            return match self.inner.status() {
                Ok(()) => Ok(None),
                Err(e) => Err(e.into()),
            };
        };

        let key = ShardsInternalMessagesKey::from(key);
        Ok(Some(InternalQueueMessage {
            key,
            workchain: value[0] as i8,
            prefix: u64::from_le_bytes(value[1..9].try_into().unwrap()),
            message_boc: &value[9..],
        }))
    }
}
