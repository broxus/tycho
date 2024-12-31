pub use self::proto::{
    DestAddr, QueueDiff, QueueKey, QueuePartition, QueueState, QueueStateHeader, QueueStateRef,
};
pub use self::queue_diff::{
    QueueDiffMessagesIter, QueueDiffStuff, QueueDiffStuffAug, SerializedQueueDiff,
};

mod proto;
mod queue_diff;
