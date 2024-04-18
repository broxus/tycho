use std::fmt::Formatter;

#[derive(Clone)]
pub struct NodeCount(usize);

impl std::fmt::Debug for NodeCount {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("NodeCount(")?;
        f.write_str(self.full().to_string().as_str())?;
        f.write_str(")")
    }
}

impl TryFrom<usize> for NodeCount {
    type Error = &'static str;
    fn try_from(total_peers: usize) -> Result<Self, Self::Error> {
        // may occur if peer_schedule is empty
        if total_peers < 3 {
            return Err("not enough nodes to run consensus");
        } else {
            Ok(NodeCount::new(total_peers))
        }
    }
}

impl NodeCount {
    pub const GENESIS: Self = Self(0);

    pub fn new(total_peers: usize) -> Self {
        // 1 matches the genesis
        assert!(
            total_peers != 0 && total_peers != 2,
            "invalid node count: {total_peers}"
        );
        // ceil up to 3F+1; assume the least possible amount of nodes is offline
        let count = ((total_peers + 1) / 3) * 3 + 1;
        assert!(
            total_peers <= count,
            "node count {total_peers} overflows after rounding up to 3F+1"
        );
        NodeCount((count - 1) / 3) // 1F
    }

    fn full(&self) -> usize {
        self.0 * 3 + 1
    }

    pub fn majority(&self) -> usize {
        self.0 * 2 + 1
    }

    /// excluding either current node or the point's author, depending on the context
    pub fn majority_of_others(&self) -> usize {
        // yes, genesis has the contradiction: reliable minority > majority of others;
        // but no node may exist in genesis, thus cannot exclude itself from it
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
