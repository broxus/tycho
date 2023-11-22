use anyhow::Result;

use crate::types::{Direction, PeerId};

pub struct Connection {
    inner: quinn::Connection,
    peer_id: PeerId,
    origin: Direction,
}

impl Connection {
    pub fn new(inner: quinn::Connection, origin: Direction) -> Result<Self> {
        todo!()
    }
}
