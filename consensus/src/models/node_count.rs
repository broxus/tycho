#[derive(Clone)]
pub struct NodeCount(u8);

impl std::fmt::Debug for NodeCount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NodeCount({})", self.full())
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
        // ceil up to 3F+1 and scale down to 1F,
        // assuming the least possible amount of nodes is not in validator set
        let one_f = (total_peers + 1) / 3;
        assert!(
            u8::try_from(one_f * 3 + 1).is_ok(),
            "node count 3F+1={one_f} overflows u8 after ceiling {total_peers}"
        );
        NodeCount(one_f as u8)
    }

    pub fn full(&self) -> usize {
        self.0 as usize * 3 + 1
    }

    pub fn majority(&self) -> usize {
        self.0 as usize * 2 + 1
    }

    /// excluding either current node or the point's author, depending on the context
    pub fn majority_of_others(&self) -> usize {
        // at first glance, genesis has a contradiction: reliable minority > majority of others;
        // but a real node cannot exist in genesis, thus cannot exclude itself from it
        self.0 as usize * 2
    }

    /// at least one node is reliable
    pub fn reliable_minority(&self) -> usize {
        self.0 as usize + 1
    }
    /*
    pub fn unreliable(&self) -> usize {
        self.0
    }
    */
}
