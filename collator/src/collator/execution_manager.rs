use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::*;
use everscale_types::prelude::*;

use crate::collator::types::{AccountId, AsyncMessage, ShardAccountStuff};

pub(super) struct ExecutionManager {
    #[allow(clippy::type_complexity)]
    pub changed_accounts: HashMap<
        AccountId,
        (
            tokio::sync::mpsc::Sender<Arc<AsyncMessage>>,
            tokio::task::JoinHandle<Result<ShardAccountStuff>>,
        ),
    >,

    // receive_tr: tokio::sync::mpsc::Receiver<Option<(Arc<AsyncMessage>, Result<Transaction>)>>,
    // wait_tr: Arc<Wait<(Arc<AsyncMessage>, Result<Transaction>)>>,
    max_collate_threads: u16,
    pub libraries: Dict<HashBytes, LibDescr>,

    gen_utime: u32,

    // block's start logical time
    start_lt: u64,
    // actual maximum logical time
    max_lt: Arc<AtomicU64>,
    // this time is used if account's lt is smaller
    min_lt: Arc<AtomicU64>,
    // block random seed
    seed_block: HashBytes,

    config: BlockchainConfig,
}

impl ExecutionManager {
    pub fn new(
        gen_utime: u32,
        start_lt: u64,
        max_lt: u64,
        seed_block: HashBytes,
        libraries: Dict<HashBytes, LibDescr>,
        config: BlockchainConfig,
        max_collate_threads: u16,
    ) -> Self {
        Self {
            changed_accounts: HashMap::new(),
            max_collate_threads,
            libraries,
            gen_utime,
            start_lt,
            max_lt: Arc::new(AtomicU64::new(max_lt)),
            min_lt: Arc::new(AtomicU64::new(max_lt)),
            seed_block,
            config,
        }
    }
}
