#[derive(Copy, Clone)]
pub struct NodeCount(usize);

impl TryFrom<usize> for NodeCount {
    type Error = &'static str;
    fn try_from(total_peers: usize) -> Result<Self, Self::Error> {
        // may occur if peer_schedule is empty
        let count = if total_peers < 3 {
            return Err("not enough nodes to run consensus");
        } else {
            ((total_peers + 2) / 3) * 3 + 1 // ceil up to 3F+1
        };
        if count < total_peers {
            panic!("node count {total_peers} overflows after rounding up to 3F+1");
        }
        Ok(NodeCount((count - 1) / 3)) // 1F
    }
}

impl NodeCount {
    pub const GENESIS: Self = Self(0);
    /*
    pub fn full(&self) -> usize {
        self.0 * 3 + 1
    }
    */
    pub fn majority(&self) -> usize {
        self.0 * 2 + 1
    }

    /// excluding either current node or the point's author, depending on the context
    pub fn majority_of_others(&self) -> usize {
        // yes, genesis has the contradiction: reliable minority > majority of others
        self.0 * 2
    }

    pub fn reliable_minority(&self) -> usize {
        self.0 + 1
    }
    /*
    pub fn unreliable(&self) -> usize {
        self.0
    }
    */
}
