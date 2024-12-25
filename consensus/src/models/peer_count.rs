use anyhow::anyhow;

#[derive(Clone, Copy, PartialEq)]
pub struct PeerCount(u8);

impl std::fmt::Debug for PeerCount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerCount")
            .field("3F+1", &self.full())
            .finish()
    }
}

impl TryFrom<usize> for PeerCount {
    type Error = anyhow::Error;
    fn try_from(total_peers: usize) -> Result<Self, Self::Error> {
        // may occur if peer_schedule is empty
        if total_peers < 3 {
            Err(anyhow!("{total_peers} peers not enough to run consensus"))
        } else {
            // ceil up to 3F+1 and scale down to 1F,
            // assuming the least possible amount of nodes is not in validator set
            let one_f = (total_peers + 1) / 3;
            let full = one_f * 3 + 1;
            assert!(
                u8::try_from(full).is_ok(),
                "node count 3F+1={full} overflows u8 after ceiling {total_peers}"
            );
            Ok(PeerCount(one_f as u8))
        }
    }
}

impl PeerCount {
    pub const GENESIS: Self = Self(0);
    pub const MAX: Self = Self((u8::MAX - 1) / 3);

    pub const fn full(&self) -> usize {
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
    // pub fn unreliable(&self) -> usize {
    // self.0
    // }
}
