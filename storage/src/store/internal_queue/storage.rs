use std::fs::File;

use anyhow::Result;
use everscale_types::models::{BlockId, IntAddr, Message, MsgInfo, OutMsgQueueUpdates, ShardIdent};
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx, RouterAddr, RouterPartitions};
use tycho_util::FastHashMap;

use crate::model::{
    CommitPointerKey, CommitPointerValue, DiffInfo, DiffInfoKey, DiffTailKey,
    ShardsInternalMessagesKey, StatKey,
};
use crate::store::internal_queue::snapshot::InternalQueueSnapshot;
use crate::store::internal_queue::transaction::InternalQueueTransaction;
use crate::util::StoredValue;
use crate::{BaseDb, MappedFile, QueueStateReader, INT_QUEUE_LAST_COMMITTED_MC_BLOCK_ID_KEY};
#[derive(Clone)]
pub struct InternalQueueStorage {
    db: BaseDb,
}

impl InternalQueueStorage {
    pub fn new(db: BaseDb) -> Self {
        Self { db }
    }

    pub fn begin_transaction(&self) -> InternalQueueTransaction {
        InternalQueueTransaction {
            db: self.db.clone(),
            batch: Default::default(),
            buffer: Vec::new(),
        }
    }

    pub fn make_snapshot(&self) -> InternalQueueSnapshot {
        InternalQueueSnapshot {
            db: self.db.clone(),
            snapshot: self.db.owned_snapshot(),
        }
    }

    /// Initializes the internal queue storage from a file.
    ///
    /// # Arguments
    /// * `top_update` - The top-level diff and tail len.
    /// * `file` - The file containing queue state data.
    /// * `block_id` - The key block identifier.
    ///
    /// # Returns
    /// * `Ok(())` if the import was successful.
    /// * `Err(anyhow::Error)` if an error occurs during import.
    pub async fn import_from_file(
        &self,
        top_update: &OutMsgQueueUpdates,
        file: File,
        block_id: BlockId,
    ) -> Result<()> {
        tracing::info!("Importing internal queue from file for block {block_id}");
        use everscale_types::boc::ser::BocHeader;

        let top_update = top_update.clone();
        let this = self.clone();

        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let get_partition = |partitions: &RouterPartitions, router_addr: &RouterAddr| {
                for (p, addresses) in partitions {
                    if addresses.contains(router_addr) {
                        return Some(*p);
                    }
                }
                None
            };

            let mapped = MappedFile::from_existing_file(file)?;

            let mut reader = QueueStateReader::begin_from_mapped(mapped.as_slice(), &top_update)?;

            let messages_cf = this.db.shard_internal_messages.cf();
            let stats_cf = this.db.internal_message_stats.cf();
            let var_cf = this.db.internal_message_var.cf();
            let diffs_tail_cf = this.db.internal_message_diffs_tail.cf();
            let diff_infos_cf = this.db.internal_message_diff_info.cf();
            let commit_pointers_cf = this.db.internal_message_commit_pointer.cf();

            let mut first_diff_read = false;

            let mut batch = weedb::rocksdb::WriteBatch::default();

            let mut buffer = Vec::new();
            let mut statistics: FastHashMap<QueuePartitionIdx, FastHashMap<RouterAddr, u64>> =
                FastHashMap::default();
            while let Some(mut part) = reader.read_next_queue_diff()? {
                let mut shards_messages_count = FastHashMap::default();

                while let Some(cell) = part.read_next_message()? {
                    let msg_hash = cell.repr_hash();
                    let msg = cell.parse::<Message<'_>>()?;
                    let MsgInfo::Int(int_msg_info) = &msg.info else {
                        anyhow::bail!("non-internal message in the queue in msg {msg_hash}");
                    };

                    let IntAddr::Std(dest) = &int_msg_info.dst else {
                        anyhow::bail!("non-std destination address in msg {msg_hash}");
                    };

                    let IntAddr::Std(src) = &int_msg_info.src else {
                        anyhow::bail!("non-std destination address in msg {msg_hash}");
                    };

                    let src_addr = RouterAddr {
                        workchain: src.workchain,
                        account: src.address,
                    };

                    let dest_addr = RouterAddr {
                        workchain: dest.workchain,
                        account: dest.address,
                    };

                    // TODO after split/merge implementation we should use detailed counter for 256 shards
                    let dest_shard = ShardIdent::new_full(dest_addr.workchain as i32);

                    shards_messages_count
                        .entry(dest_shard)
                        .and_modify(|count| *count += 1)
                        .or_insert(1);

                    let queue_diff = part.queue_diff();
                    let partition = get_partition(&queue_diff.router_partitions_dst, &dest_addr)
                        .or_else(|| get_partition(&queue_diff.router_partitions_src, &src_addr))
                        .unwrap_or_default();

                    let key = ShardsInternalMessagesKey {
                        partition,
                        shard_ident: block_id.shard,
                        internal_message_key: QueueKey {
                            lt: int_msg_info.created_lt,
                            hash: *msg_hash,
                        },
                    };

                    buffer.clear();
                    buffer.push(dest.workchain as u8);
                    buffer.extend_from_slice(&dest.prefix().to_le_bytes());
                    BocHeader::<ahash::RandomState>::with_root(cell.as_ref()).encode(&mut buffer);
                    batch.put_cf(&messages_cf, key.to_vec(), &buffer);

                    let partition_stats = statistics.entry(partition).or_default();
                    *partition_stats.entry(dest_addr).or_insert(0) += 1;
                }

                let queue_diff = part.queue_diff();

                if !first_diff_read {
                    // set commit pointer
                    let commit_pointer_key = CommitPointerKey {
                        shard_ident: queue_diff.shard_ident,
                    };

                    let commit_pointer_value = CommitPointerValue {
                        queue_key: queue_diff.max_message,
                        seqno: queue_diff.seqno,
                    };

                    batch.put_cf(
                        &commit_pointers_cf,
                        commit_pointer_key.to_vec(),
                        commit_pointer_value.to_vec(),
                    );
                }

                first_diff_read = true;

                // insert diff tail
                let diff_tail_key = DiffTailKey {
                    shard_ident: queue_diff.shard_ident,
                    max_message: queue_diff.max_message,
                };

                batch.put_cf(
                    &diffs_tail_cf,
                    diff_tail_key.to_vec(),
                    queue_diff.seqno.to_le_bytes(),
                );

                // insert diff info
                let diff_info_key = DiffInfoKey {
                    shard_ident: queue_diff.shard_ident,
                    seqno: queue_diff.seqno,
                };

                let diff_info = DiffInfo {
                    min_message: queue_diff.min_message,
                    max_message: queue_diff.max_message,
                    shards_messages_count,
                    hash: queue_diff.hash,
                    processed_to: queue_diff.processed_to.clone(),
                    router_partitions_src: queue_diff.router_partitions_src.clone(),
                    router_partitions_dst: queue_diff.router_partitions_dst.clone(),
                    seqno: queue_diff.seqno,
                };

                batch.put_cf(
                    &diff_infos_cf,
                    diff_info_key.to_vec(),
                    tl_proto::serialize(diff_info),
                );

                for (partition, statistics) in statistics.drain() {
                    for (dest, count) in statistics.iter() {
                        let key = StatKey {
                            shard_ident: queue_diff.shard_ident,
                            partition,
                            max_message: queue_diff.max_message,
                            dest: *dest,
                        };

                        batch.put_cf(&stats_cf, key.to_vec(), count.to_le_bytes());
                    }
                }
            }

            // insert last applied diff
            if block_id.is_masterchain() {
                batch.put_cf(
                    &var_cf,
                    INT_QUEUE_LAST_COMMITTED_MC_BLOCK_ID_KEY,
                    block_id.to_vec(),
                );
            }

            reader.finish()?;

            this.db.rocksdb().write(batch)?;
            Ok(())
        })
        .await?
    }
}
