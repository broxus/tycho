use std::path::PathBuf;

type ShardIdent = i64;
type LtHash = u64;

struct QueueDiff {}
struct Message {}
struct QueueIterator {}

trait MessageQueue {
    // Factory methods for initialization and loading
    fn new(directory: PathBuf, shard_id: ShardIdent) -> Self;
    fn load_from_storage(directory: PathBuf, shard_id: ShardIdent) -> Self;

    // Methods for queue management
    fn apply_diff(&mut self, diff: QueueDiff);
    fn save_to_storage(&self, directory: PathBuf);
    fn commit_current_state(&mut self);

    // Differential and state management
    fn get_current_diff(&self) -> Option<Vec<QueueDiff>>;
    fn undo_state_to_block(&mut self, diff: Vec<QueueDiff>);

    // Message handling
    fn add_message(&mut self, lt_hash: LtHash, message: Message);
    fn upto_message(&mut self, lt_hash: LtHash);

    // Queue navigation and sharding
    fn create_iterator(&self, lt_hash: LtHash) -> QueueIterator;
    fn next_element(&self, iterator: &QueueIterator) -> Option<Message>;
}
