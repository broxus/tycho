use everscale_types::models::BlockId;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct BlockFull {
    pub id: BlockId,
    pub block: Vec<u8>,
    pub proof: Vec<u8>,
    pub is_link: bool,
}
