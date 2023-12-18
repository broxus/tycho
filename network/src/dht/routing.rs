use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

use crate::types::PeerId;

pub struct Builder {
    local_id: PeerId,
    max_k: usize,
    node_timeout: Duration,
}

impl Builder {
    pub fn build(self) -> RoutingTable {
        RoutingTable {
            local_id: self.local_id,
            buckets: BTreeMap::default(),
            max_k: self.max_k,
            node_timeout: self.node_timeout,
        }
    }

    pub fn with_node_timeout(mut self, timeout: Duration) -> Self {
        self.node_timeout = timeout;
        self
    }
}

pub struct RoutingTable {
    local_id: PeerId,
    buckets: BTreeMap<usize, Bucket>,
    max_k: usize,
    node_timeout: Duration,
}

impl RoutingTable {
    pub fn builder(local_id: PeerId) -> Builder {
        Builder {
            local_id,
            max_k: 20,
            node_timeout: Duration::from_secs(15 * 60),
        }
    }

    pub fn add(&mut self, key: &PeerId) -> bool {
        let distance = distance(&self.local_id, key);
        if distance == 0 {
            return false;
        }

        self.buckets
            .entry(distance)
            .or_insert_with(|| Bucket::with_capacity(self.max_k))
            .insert(key, self.max_k, &self.node_timeout)
    }

    pub fn remove(&mut self, key: &PeerId) -> bool {
        let distance = distance(&self.local_id, key);
        if let Some(bucket) = self.buckets.get_mut(&distance) {
            bucket.remove(key)
        } else {
            false
        }
    }

    pub fn closest(&self, key: &PeerId, count: usize) -> Vec<PeerId> {
        let index = self.get_bucket_index(key);
        let mut result = Vec::<PeerId>::new();

        {
            let (first, second) = self.buckets[index].nodes.as_slices();
            result.extend_from_slice(first);
            result.extend_from_slice(second);
        }

        result
    }

    fn get_bucket_index(&mut self, key: &PeerId) -> usize {
        debug_assert!(!self.buckets.is_empty());
        std::cmp::min(self.local_id.common_prefix_len(key), self.buckets.len() - 1)
    }
}

struct Bucket {
    nodes: VecDeque<Node>,
}

impl Bucket {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            nodes: VecDeque::with_capacity(capacity),
        }
    }

    fn insert(&mut self, key: &PeerId, max_k: usize, timeout: &Duration) -> bool {
        if let Some(index) = self.nodes.iter_mut().position(|node| &node.id == key) {
            self.nodes.remove(index);
        } else if self.nodes.len() >= max_k {
            if matches!(self.nodes.front(), Some(node) if node.is_expired(timeout)) {
                self.nodes.pop_front();
            } else {
                return false;
            }
        }

        self.nodes.push_back(Node::new(key));
        true
    }

    fn remove(&mut self, key: &PeerId) -> bool {
        if let Some(index) = self.nodes.iter().position(|node| &node.id == key) {
            self.nodes.remove(index);
            true
        } else {
            false
        }
    }

    fn contains(&self, key: &PeerId) -> bool {
        self.nodes.iter().any(|node| &node.id == key)
    }

    fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

struct Node {
    id: PeerId,
    last_updated_at: Instant,
}

impl Node {
    fn new(peer_id: &PeerId) -> Self {
        Self {
            id: *peer_id,
            last_updated_at: Instant::now(),
        }
    }

    fn is_expired(&self, timeout: &Duration) -> bool {
        &self.last_updated_at.elapsed() >= timeout
    }
}

pub fn distance(left: &PeerId, right: &PeerId) -> usize {
    const MAX_DISTANCE: usize = 256;

    for (i, (left, right)) in std::iter::zip(left.0.chunks(8), right.0.chunks(8)).enumerate() {
        let left = u64::from_be_bytes(left.try_into().unwrap());
        let right = u64::from_be_bytes(right.try_into().unwrap());
        let diff = left ^ right;
        if diff != 0 {
            return MAX_DISTANCE - i * 64 + diff.leading_zeros() as usize;
        }
    }

    0
}

const MAX_BUCKET_SIZE_K: usize = 20;
