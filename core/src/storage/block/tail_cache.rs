use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::oneshot;
use tycho_block_util::block::BlockStuff;
use tycho_types::models::BlockId;

#[derive(Default)]
struct InnerState {
    current_master: Option<BlockId>,
    current_max_tail_len: u32,
    finalized: BTreeMap<u32, u32>, // seqno -> max_tail_len
    waiters: HashMap<u32, Vec<oneshot::Sender<u32>>>, // seqno -> waiters
}

#[derive(Clone)]
pub struct TailDiffCache {
    inner: Arc<Mutex<InnerState>>,
}

impl TailDiffCache {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(InnerState::default())),
        }
    }

    /// PRECONDITION:
    /// MUST be called by `BlockSaver`.
    /// This function assumes that block strider firstly processes master, then shard blocks in an unspecified order
    /// without duplicates.
    pub fn update_block(&self, block: &BlockStuff) {
        let mut state = self.inner.lock();

        let block_id = *block.id();
        let is_master = block_id.is_masterchain();
        let tail_len = block.block().out_msg_queue_updates.tail_len;

        if is_master {
            // Finalize previous master if exists
            if let Some(prev_master) = state.current_master.take() {
                let max_tail = state.current_max_tail_len;
                state.finalized.insert(prev_master.seqno, max_tail);

                // Notify waiters
                if let Some(waiters) = state.waiters.remove(&prev_master.seqno) {
                    for waiter in waiters {
                        let _ = waiter.send(max_tail);
                    }
                }
            }

            // Start new round with master's tail_len as initial max
            state.current_master = Some(block_id);
            state.current_max_tail_len = tail_len;
        } else {
            // Update max if this shard has longer tail
            state.current_max_tail_len = state.current_max_tail_len.max(tail_len);
        }
    }

    pub async fn get(&self, seqno: u32) -> Option<u32> {
        let maybe_rx: Option<oneshot::Receiver<u32>>;
        {
            let mut state = self.inner.lock();

            // First check if already finalized
            if let Some(&tail_len) = state.finalized.get(&seqno) {
                return Some(tail_len);
            }

            // If current_master > seqno and not finalized, it's too old
            if let Some(current) = state.current_master {
                if current.seqno > seqno {
                    return None;
                }
            }

            // If we are here, the value is not finalized and not "too old".
            // We must wait.
            let (tx, rx) = oneshot::channel();
            state.waiters.entry(seqno).or_default().push(tx);
            maybe_rx = Some(rx);
        }

        // It's needed cause gc subscriber is ticked as soon as master block is finalized, and we have
        // stats only after master + 1 is processed.
        maybe_rx.expect("we have returned if it's None").await.ok()
    }

    pub fn cleanup(&self, target_seqno: u32) {
        let mut state = self.inner.lock();
        state.finalized.retain(|&seqno, _| seqno > target_seqno);
    }
}

impl Default for TailDiffCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use tycho_types::models::ShardIdent;

    use super::*;

    #[tokio::test]
    async fn test_tail_cache_basic_flow() {
        let cache = TailDiffCache::new();

        // Master block 1 with tail_len = 10
        let master1 = BlockStuff::new_with(ShardIdent::MASTERCHAIN, 1, |block| {
            block.out_msg_queue_updates.tail_len = 10;
        });

        cache.update_block(&master1);

        // Should not be finalized yet
        let cache_clone = cache.clone();
        let wait_task = tokio::spawn(async move { cache_clone.get(1).await });

        // Shard blocks with different tail lengths
        let shard1 = BlockStuff::new_with(ShardIdent::BASECHAIN, 100, |block| {
            block.out_msg_queue_updates.tail_len = 20;
        });
        cache.update_block(&shard1);

        let shard2 = BlockStuff::new_with(ShardIdent::new_full(0), 101, |block| {
            block.out_msg_queue_updates.tail_len = 15;
        });
        cache.update_block(&shard2);

        let shard3 = BlockStuff::new_with(ShardIdent::new_full(1), 102, |block| {
            block.out_msg_queue_updates.tail_len = 25;
        });
        cache.update_block(&shard3);

        // Master block 2 - this should finalize master block 1
        // The max tail_len should be 25 (from shard3)
        let master2 = BlockStuff::new_with(ShardIdent::MASTERCHAIN, 2, |block| {
            block.out_msg_queue_updates.tail_len = 5;
        });

        cache.update_block(&master2);

        // Now the wait_task should complete with the finalized value
        assert_eq!(wait_task.await.unwrap(), Some(25));

        // Also check directly
        assert_eq!(cache.get(1).await, Some(25));

        // Master block 2 should not be finalized yet - spawn task to check
        let cache_clone2 = cache.clone();
        let wait_task2 = tokio::spawn(async move { cache_clone2.get(2).await });

        // Add one more master block to finalize block 2
        let master3 = BlockStuff::new_empty(ShardIdent::MASTERCHAIN, 3);
        cache.update_block(&master3);

        // Now wait_task2 should complete
        assert_eq!(wait_task2.await.unwrap(), Some(5));

        // Also check directly
        assert_eq!(cache.get(2).await, Some(5));

        // Test cleanup
        cache.cleanup(1);
        assert_eq!(cache.get(1).await, None);
        assert_eq!(cache.get(2).await, Some(5)); // Should still exist

        // Test "too old" case - asking for block 0 when current master is 3
        assert_eq!(cache.get(0).await, None); // Should return None immediately
    }
}
