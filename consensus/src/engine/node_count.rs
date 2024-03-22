#[derive(Copy, Clone)]
pub struct NodeCount(u8);

impl From<NodeCount> for usize {
    fn from(count: NodeCount) -> Self {
        count.0 as usize
    }
}

impl NodeCount {
    pub fn new(total_peers: usize) -> Self {
        if total_peers < 3 {
            panic!("Fatal: node count {total_peers} < 3");
        }
        let count = ((total_peers + 2) / 3) * 3 + 1;
        let count = u8::try_from(count).unwrap_or_else(|e| {
            panic!("Fatal: node count {total_peers} exceeds u8 after rounding to 3F+1: {e:?}");
        });
        NodeCount(count)
    }

    pub fn majority_with_me(&self) -> Self {
        Self((self.0 / 3) * 2 + 1)
    }

    pub fn majority_except_me(&self) -> Self {
        Self((self.0 / 3) * 2)
    }

    pub fn reliable_minority(&self) -> Self {
        Self(self.0 / 3 + 1)
    }

    pub fn unreliable(&self) -> Self {
        Self(self.0 / 3)
    }
}
