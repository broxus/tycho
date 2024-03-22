use anyhow::Result;
use async_trait::async_trait;

use crate::{
    mempool::MempoolAdapter,
    msg_queue::{MessageQueueAdapter, QueueIterator},
    state_node::StateNodeAdapter,
};

use super::{
    collator_processor::{CollatorProcessorSpecific, CollatorProcessorStdImpl},
    CollatorEventEmitter,
};

#[async_trait]
pub(super) trait DoCollate<MQ, MP, ST>:
    CollatorProcessorSpecific<MQ, MP, ST> + CollatorEventEmitter + Sized + Send + Sync + 'static
{
    async fn do_collate(&mut self) -> Result<()> {
        todo!()
    }
}

#[async_trait]
impl<MQ, QI, MP, ST> DoCollate<MQ, MP, ST> for CollatorProcessorStdImpl<MQ, QI, MP, ST>
where
    MQ: MessageQueueAdapter,
    QI: QueueIterator + Send + Sync + 'static,
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
}
