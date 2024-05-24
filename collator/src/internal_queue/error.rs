use everscale_types::models::ShardIdent;

#[derive(Debug)]
pub enum QueueError {
    ShardNotFound(ShardIdent),
    ShardAlreadyExists(ShardIdent),
    Other(anyhow::Error),
}

impl std::fmt::Display for QueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            QueueError::ShardNotFound(ref shard_id) => write!(f, "Shard not found: {:?}", shard_id),
            QueueError::ShardAlreadyExists(ref shard_id) => {
                write!(f, "Shard already exists: {:?}", shard_id)
            }
            QueueError::Other(ref err) => write!(f, "An error occurred: {}", err),
        }
    }
}

impl std::error::Error for QueueError {}

impl From<anyhow::Error> for QueueError {
    fn from(err: anyhow::Error) -> QueueError {
        QueueError::Other(err)
    }
}
