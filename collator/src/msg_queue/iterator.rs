/*
There are 2 options to implement iteration:
  1) implement an iterator directly for the MessageQueue trait
  2) implement separate MessageQueueIterator over items in MessageQueue
(you'll find stubs for both options down)

The next question is what kind of iterator to implement: (a) a consuming iterator
or (b) a non-consuming iterator. Finally, we should remove processed messages from
the current queue state (commit). But also we must have an option to roll back and
process messages again if the collation attempt fails. Moreover, we don't know if
we need to return the item value from the iterator or if we can return just the refs.

When implementing a non-consuming iterator we can move items to some kind of
"remove" buffer and then clear it on commit. Or we can just remember processed
items and then clear them from the queue state. We should choose the most efficient
implementation regarding the memory and CPU utilization. We also need to consider
that the iterator should have the ability to continue iteration after the commit
with minimal overhead (we shouldn't seek for the last position).

When implementing the separate MessageQueueIterator it should take ownership of
the source MessageQueue to lazy load more items chunks. After the iteration, we can
convert the iterator into MessageQueue back.
 */

use super::queue::MessageQueue;
use super::{cache_persistent::*, state_persistent::*, storage::*, types::*};

// Option (1) - MessageQueue implement iterator by itself

impl<'a, CH, ST, DB> Iterator for &'a dyn MessageQueue<CH, ST, DB> {
    type Item = &'a MessageEnvelope;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

// Option (2) - using the separate MessageQueueIterator

pub struct MessageQueueIterator<CH, ST, DB>
where
    CH: PersistentCacheService,
    ST: PersistentStateService,
    DB: StorageService,
{
    queue: Box<dyn MessageQueue<CH, ST, DB>>,
}
impl<CH, ST, DB> MessageQueueIterator<CH, ST, DB>
where
    CH: PersistentCacheService,
    ST: PersistentStateService,
    DB: StorageService,
{
    fn create_iterator(queue: impl MessageQueue<CH, ST, DB> + 'static) -> Self {
        Self {
            queue: Box::new(queue),
        }
    }
}
impl<'a, CH, ST, DB> Iterator for &'a MessageQueueIterator<CH, ST, DB>
where
    CH: PersistentCacheService,
    ST: PersistentStateService,
    DB: StorageService,
{
    type Item = &'a MessageEnvelope;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
