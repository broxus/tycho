use std::path::PathBuf;

type ShardIdent = i64;
type Lt = u64;

// need to load types from types crate
type MessageHash = String;
type Address = String;

struct QueueDiff {}
struct Message {}
struct QueueIterator {}

struct MessageEnvelope {
    lt: u64,
    hash: String,
    message: Message,
    from_contract: Address,
    to_contract: Address,
}

trait MessageQueue {
    // Factory methods for initialization and loading
    fn init(directory: PathBuf, shard_id: ShardIdent, load_if_exists: bool) -> Self;

    // Methods for queue management
    fn apply_diff(&mut self, diff: QueueDiff);
    fn save_to_storage(&self);
    fn commit_current_state(&mut self);

    // Differential and state management
    fn get_current_diff(&self) -> Option<QueueDiff>;
    fn undo_state_to_block(&mut self, diff: Vec<QueueDiff>);

    // Message handling
    fn add_message(&mut self, message: MessageEnvelope);
    fn add_processed_upto(&mut self, lt: Lt, hash: MessageHash);

    // Queue navigation
    fn create_iterator(&self) -> QueueIterator;
}

impl Iterator for QueueIterator {
    type Item = Message;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
