use std::str::FromStr;
use std::sync::Arc;

use anyhow::anyhow;
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockIdShort, IntAddr, ShardIdent, StdAddr};
use tycho_block_util::queue::QueueKey;
use tycho_collator::internal_queue::queue::{Queue, QueueFactory, QueueFactoryStdImpl, QueueImpl};
use tycho_collator::internal_queue::state::persistent_state::{
    PersistentStateImplFactory, PersistentStateStdImpl,
};
use tycho_collator::internal_queue::state::session_state::{
    SessionStateImplFactory, SessionStateStdImpl,
};
use tycho_collator::internal_queue::state::states_iterators_manager::StatesIteratorsManager;
use tycho_collator::internal_queue::types::{InternalMessageValue, QueueDiffWithMessages};
use tycho_collator::test_utils::prepare_test_storage;
use tycho_util::FastHashMap;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd)]
struct StoredObject {
    key: u64,
    dest: IntAddr,
}

impl Ord for StoredObject {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

impl InternalMessageValue for StoredObject {
    fn deserialize(bytes: &[u8]) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let key = u64::from_be_bytes(bytes[..8].try_into()?);
        let workchain = i8::from_be_bytes(bytes[8].to_be_bytes());
        let addr = HashBytes::from_slice(&bytes[9..]);
        let dest = IntAddr::Std(StdAddr::new(workchain, addr));
        Ok(StoredObject { key, dest })
    }

    fn serialize(&self) -> anyhow::Result<Vec<u8>>
    where
        Self: Sized,
    {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.key.to_be_bytes());
        match &self.dest {
            IntAddr::Std(addr) => {
                bytes.extend_from_slice(&addr.workchain.to_be_bytes());
                bytes.extend_from_slice(&addr.address.0);
            }
            _ => return Err(anyhow!("Unsupported address type")),
        }
        Ok(bytes)
    }

    fn destination(&self) -> &IntAddr {
        &self.dest
    }

    fn key(&self) -> QueueKey {
        QueueKey {
            lt: self.key,
            hash: HashBytes::default(),
        }
    }
}

async fn create_stored_object(key: u64, dest_str: &str) -> anyhow::Result<Arc<StoredObject>> {
    let dest = IntAddr::Std(StdAddr::from_str(dest_str)?);
    Ok(Arc::new(StoredObject { key, dest }))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue() -> anyhow::Result<()> {
    let storage = prepare_test_storage().await?;

    let queue_factory = QueueFactoryStdImpl {
        session_state_factory: SessionStateImplFactory {
            storage: storage.clone(),
        },
        persistent_state_factory: PersistentStateImplFactory { storage },
        gc_queue_buffer_size: 100,
    };

    let queue: QueueImpl<SessionStateStdImpl, PersistentStateStdImpl, StoredObject> =
        queue_factory.create();
    let block = BlockIdShort {
        shard: ShardIdent::new_full(0),
        seqno: 0,
    };
    let mut diff = QueueDiffWithMessages::new();

    let stored_objects = vec![
        create_stored_object(
            1,
            "1:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )
        .await?,
        create_stored_object(
            2,
            "1:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )
        .await?,
        create_stored_object(
            3,
            "0:7d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )
        .await?,
        create_stored_object(
            4,
            "1:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )
        .await?,
        create_stored_object(
            5,
            "1:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )
        .await?,
    ];

    for stored_object in &stored_objects {
        diff.messages
            .insert(stored_object.key(), stored_object.clone());
    }

    queue.apply_diff(diff, block).await?;

    let mut top_blocks = vec![];

    top_blocks.push((block, true));

    queue.commit_diff(top_blocks).await?;

    let block2 = BlockIdShort {
        shard: ShardIdent::new_full(1),
        seqno: 1,
    };

    let mut diff = QueueDiffWithMessages::new();

    let stored_objects2 = vec![
        create_stored_object(
            1,
            "0:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )
        .await?,
        create_stored_object(
            2,
            "0:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )
        .await?,
        create_stored_object(
            3,
            "0:7d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )
        .await?,
        create_stored_object(
            4,
            "0:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )
        .await?,
        create_stored_object(
            5,
            "0:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )
        .await?,
    ];

    for stored_object in &stored_objects2 {
        diff.messages
            .insert(stored_object.key(), stored_object.clone());
    }

    let mut top_blocks = vec![];

    top_blocks.push((block2, true));

    queue.apply_diff(diff, block2).await?;
    queue.commit_diff(top_blocks).await?;

    let mut ranges = FastHashMap::default();
    ranges.insert(
        ShardIdent::new_full(0),
        (
            QueueKey {
                lt: 1,
                hash: HashBytes::default(),
            },
            QueueKey {
                lt: 4,
                hash: HashBytes::default(),
            },
        ),
    );

    let iterators = queue.iterator(&ranges, ShardIdent::new_full(1)).await;

    let mut iterator_manager = StatesIteratorsManager::new(iterators);

    let mut loaded_stored_object = iterator_manager.next();
    loaded_stored_object = iterator_manager.next();

    let loaded_stored_object = loaded_stored_object.unwrap().unwrap();
    assert_eq!(stored_objects[3], loaded_stored_object.message);

    let current_position = iterator_manager.current_position();
    let mut expected_position = FastHashMap::default();
    expected_position.insert(ShardIdent::new_full(0), QueueKey {
        lt: 4,
        hash: HashBytes::default(),
    });

    assert_eq!(expected_position, current_position);

    Ok(())
}
