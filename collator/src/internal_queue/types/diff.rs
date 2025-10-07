use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::sync::Arc;

use tycho_block_util::queue::{QueueDiff, QueueDiffStuff, QueueKey};
use tycho_types::cell::Load;
use tycho_types::models::{MsgInfo, OutMsgDescr};

use crate::internal_queue::state::state_iterator::MessageExt;
use crate::internal_queue::types::message::{EnqueuedMessage, InternalMessageValue};
use crate::internal_queue::types::router::PartitionRouter;
use crate::types::ProcessedTo;

#[derive(Default, Debug, Clone)]
pub struct QueueDiffWithMessages<V: InternalMessageValue> {
    pub messages: BTreeMap<QueueKey, Arc<V>>,
    pub processed_to: ProcessedTo,
    pub partition_router: PartitionRouter,
}

impl<V: InternalMessageValue> QueueDiffWithMessages<V> {
    pub fn new() -> Self {
        Self {
            messages: BTreeMap::new(),
            processed_to: BTreeMap::new(),
            partition_router: Default::default(),
        }
    }

    pub fn min_message(&self) -> Option<&QueueKey> {
        self.messages.keys().next()
    }

    pub fn max_message(&self) -> Option<&QueueKey> {
        self.messages.keys().next_back()
    }
}

impl QueueDiffWithMessages<EnqueuedMessage> {
    pub fn from_queue_diff(
        queue_diff_stuff: &QueueDiffStuff,
        out_msg_description: &OutMsgDescr,
    ) -> anyhow::Result<Self> {
        let QueueDiff {
            processed_to,
            router_partitions_src,
            router_partitions_dst,
            ..
        } = queue_diff_stuff.as_ref();

        let partition_router =
            PartitionRouter::with_partitions(router_partitions_src, router_partitions_dst);

        let mut messages: BTreeMap<QueueKey, Arc<_>> = BTreeMap::new();
        for msg in queue_diff_stuff.zip(out_msg_description) {
            let lazy_msg = msg?;
            let cell = lazy_msg.into_inner();
            let hash = *cell.repr_hash();
            let info = MsgInfo::load_from(&mut cell.as_slice()?)?;
            if let MsgInfo::Int(out_msg_info) = info {
                let created_lt = out_msg_info.created_lt;
                let value = EnqueuedMessage::from((out_msg_info, cell));
                messages.insert((created_lt, hash).into(), Arc::new(value));
            }
        }

        Ok(Self {
            messages,
            processed_to: processed_to.clone(),
            partition_router,
        })
    }
}

pub struct QueueFullDiff<V: InternalMessageValue> {
    pub diff: QueueDiffWithMessages<V>,
    pub messages_for_current_shard: BinaryHeap<Reverse<MessageExt<V>>>,
}

#[derive(Debug)]
pub enum DiffZone {
    Committed,
    Uncommitted,
    Both,
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use tycho_block_util::queue::{QueuePartitionIdx, RouterAddr, RouterPartitions};
    use tycho_types::cell::HashBytes;
    use tycho_types::models::ShardIdent;
    use tycho_util::FastHashMap;

    use super::*;
    use crate::storage::models::DiffInfo;

    #[test]
    fn test_diff_info_value_serialization() {
        // 1) Create example data
        let mut map = FastHashMap::default();
        map.insert(ShardIdent::MASTERCHAIN, 123);
        map.insert(ShardIdent::BASECHAIN, 999);

        let mut processed_to = BTreeMap::new();
        processed_to.insert(ShardIdent::MASTERCHAIN, QueueKey {
            lt: 222,
            hash: HashBytes::from([0xCC; 32]),
        });

        let mut router_partitions_src = RouterPartitions::new();
        router_partitions_src.insert(
            QueuePartitionIdx(1),
            BTreeSet::from([RouterAddr {
                workchain: 0,
                account: HashBytes([0x01; 32]),
            }]),
        );

        let mut router_partitions_dst = RouterPartitions::new();
        router_partitions_dst.insert(
            QueuePartitionIdx(2),
            BTreeSet::from([RouterAddr {
                workchain: 0,
                account: HashBytes([0x02; 32]),
            }]),
        );

        let original = DiffInfo {
            min_message: QueueKey {
                lt: 111,
                hash: HashBytes::from([0xAA; 32]),
            },
            shards_messages_count: map,
            hash: HashBytes::from([0xBB; 32]),
            processed_to,
            router_partitions_src,
            max_message: QueueKey {
                lt: 222,
                hash: HashBytes::from([0xBB; 32]),
            },
            router_partitions_dst,
            seqno: 123,
        };

        // 2) Serialize
        let serialized = tl_proto::serialize(&original);

        // 3) Deserialize
        let deserialized = tl_proto::deserialize::<DiffInfo>(&serialized)
            .expect("Failed to deserialize DiffInfoValue");

        // 4) Compare original and deserialized
        assert_eq!(original, deserialized);
    }
}
