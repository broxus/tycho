use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tycho_util::time::now_sec;

use crate::dht::{MAX_XOR_DISTANCE, xor_distance};
use crate::network::KnownPeerHandle;
use crate::types::{PeerId, PeerInfo};

pub(crate) type SimpleRoutingTable = RoutingTable<Arc<PeerInfo>>;
pub(crate) type HandlesRoutingTable = RoutingTable<KnownPeerHandle>;

pub(crate) struct RoutingTable<T> {
    pub local_id: PeerId,
    pub buckets: BTreeMap<usize, Bucket<T>>,
}

impl<T> RoutingTable<T> {
    pub fn new(local_id: PeerId) -> Self {
        Self {
            local_id,
            buckets: Default::default(),
        }
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.buckets.values().all(Bucket::is_empty)
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.buckets.values().map(|bucket| bucket.nodes.len()).sum()
    }
}

impl<T: AsPeerInfo> RoutingTable<T> {
    pub fn contains(&self, peer_id: &PeerId) -> bool {
        let distance = xor_distance(&self.local_id, peer_id);
        self.buckets
            .get(&distance)
            .is_some_and(|bucket| bucket.contains(peer_id))
    }

    pub fn add<F>(&mut self, peer: Arc<PeerInfo>, max_k: usize, node_ttl: &Duration, f: F) -> bool
    where
        F: FnOnce(Arc<PeerInfo>) -> Option<T>,
    {
        let distance = xor_distance(&self.local_id, &peer.id);
        if distance == 0 {
            return false;
        }

        self.buckets
            .entry(distance)
            .or_insert_with(|| Bucket::with_capacity(max_k))
            .insert(peer, max_k, node_ttl, f)
    }

    pub fn closest(&self, key: &[u8; 32], count: usize) -> Vec<Arc<PeerInfo>> {
        if count == 0 {
            return Vec::new();
        }

        // TODO: fill secure and unsecure buckets in parallel
        let mut result = Vec::with_capacity(count);
        let distance = xor_distance(&self.local_id, PeerId::wrap(key));

        // Search for closest nodes first
        for i in (distance..=MAX_XOR_DISTANCE).chain((0..distance).rev()) {
            let remaining = match count.checked_sub(result.len()) {
                None | Some(0) => break,
                Some(n) => n,
            };

            if let Some(bucket) = self.buckets.get(&i) {
                for node in bucket.nodes.iter().take(remaining) {
                    result.push(node.data.load_peer_info());
                }
            }
        }

        result
    }

    pub fn visit_closest<F>(&self, key: &[u8; 32], count: usize, mut f: F)
    where
        F: FnMut(&T),
    {
        if count == 0 {
            return;
        }

        let distance = xor_distance(&self.local_id, PeerId::wrap(key));

        let mut processed = 0;

        // Search for closest nodes first
        for i in (distance..=MAX_XOR_DISTANCE).chain((0..distance).rev()) {
            let remaining = match count.checked_sub(processed) {
                None | Some(0) => break,
                Some(n) => n,
            };

            if let Some(bucket) = self.buckets.get(&i) {
                for node in bucket.nodes.iter().take(remaining) {
                    f(&node.data);
                    processed += 1;
                }
            }
        }
    }
}

impl<T: Clone> Clone for RoutingTable<T> {
    fn clone(&self) -> Self {
        Self {
            local_id: self.local_id,
            buckets: self.buckets.clone(),
        }
    }
}

impl<T> std::fmt::Debug for RoutingTable<T>
where
    T: AsPeerInfo,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoutingTable")
            .field("local_id", &self.local_id)
            .field("buckets", &self.buckets)
            .finish()
    }
}

pub(crate) struct Bucket<T> {
    nodes: VecDeque<Node<T>>,
}

impl<T> Bucket<T> {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            nodes: VecDeque::with_capacity(capacity),
        }
    }

    pub fn retain_nodes<F>(&mut self, f: F)
    where
        F: FnMut(&Node<T>) -> bool,
    {
        self.nodes.retain(f);
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

impl<T: AsPeerInfo> Bucket<T> {
    pub fn contains(&self, peer_id: &PeerId) -> bool {
        self.nodes
            .iter()
            .any(|node| node.data.as_peer_info().id == *peer_id)
    }

    fn insert<F>(
        &mut self,
        peer_info: Arc<PeerInfo>,
        max_k: usize,
        timeout: &Duration,
        f: F,
    ) -> bool
    where
        F: FnOnce(Arc<PeerInfo>) -> Option<T>,
    {
        let data = 'data: {
            if let Some(index) = self
                .nodes
                .iter_mut()
                .position(|item| item.data.as_peer_info().id == peer_info.id)
            {
                if let Some(data) = f(peer_info) {
                    // Found node info with the same id, update it
                    self.nodes.remove(index);
                    break 'data data;
                }
            } else if self.nodes.len() >= max_k {
                if matches!(self.nodes.front(), Some(node) if node.is_expired(now_sec(), timeout)) {
                    if let Some(data) = f(peer_info) {
                        // Found an expired node, replace it
                        self.nodes.pop_front();
                        break 'data data;
                    }
                }
            } else if let Some(data) = f(peer_info) {
                // Found an empty slot, insert the new node
                break 'data data;
            }

            // No action was taken
            return false;
        };

        self.nodes.push_back(Node::new(data));
        true
    }
}

impl<T: Clone> Clone for Bucket<T> {
    fn clone(&self) -> Self {
        Self {
            nodes: self.nodes.clone(),
        }
    }
}

impl<T> std::fmt::Debug for Bucket<T>
where
    T: AsPeerInfo,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bucket")
            .field("nodes", &self.nodes)
            .finish()
    }
}

pub(crate) struct Node<T> {
    pub data: T,
    pub last_updated_at: Instant,
}

impl<T> Node<T> {
    fn new(data: T) -> Self {
        Self {
            data,
            last_updated_at: Instant::now(),
        }
    }
}

impl<T: AsPeerInfo> Node<T> {
    pub fn is_expired(&self, at: u32, timeout: &Duration) -> bool {
        self.data.as_peer_info().is_expired(at) || &self.last_updated_at.elapsed() >= timeout
    }
}

impl<T: Clone> Clone for Node<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            last_updated_at: self.last_updated_at,
        }
    }
}

impl<T> std::fmt::Debug for Node<T>
where
    T: AsPeerInfo,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("peer_info", self.data.as_peer_info().as_ref())
            .field("last_updated_at", &self.last_updated_at)
            .finish()
    }
}

pub(crate) trait AsPeerInfo {
    type Guard<'a>: std::ops::Deref<Target = Arc<PeerInfo>>
    where
        Self: 'a;

    fn as_peer_info(&self) -> Self::Guard<'_>;

    fn load_peer_info(&self) -> Arc<PeerInfo>;
}

impl AsPeerInfo for Arc<PeerInfo> {
    type Guard<'a> = &'a Arc<PeerInfo>;

    #[inline]
    fn as_peer_info(&self) -> Self::Guard<'_> {
        self
    }

    #[inline]
    fn load_peer_info(&self) -> Arc<PeerInfo> {
        self.clone()
    }
}

impl AsPeerInfo for KnownPeerHandle {
    type Guard<'a>
        = arc_swap::Guard<Arc<PeerInfo>, arc_swap::DefaultStrategy>
    where
        Self: 'a;

    #[inline]
    fn as_peer_info(&self) -> Self::Guard<'_> {
        self.peer_info()
    }

    #[inline]
    fn load_peer_info(&self) -> Arc<PeerInfo> {
        KnownPeerHandle::load_peer_info(self)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::util::make_peer_info_stub;

    const MAX_K: usize = 20;

    #[derive(Debug, Default)]
    struct PeerState {
        knows_about: usize,
        known_by: usize,
    }

    impl PeerState {
        fn compute_states<'a, I>(tables: I) -> BTreeMap<PeerId, PeerState>
        where
            I: Iterator<Item = &'a SimpleRoutingTable> + Clone + 'a,
        {
            let mut states = BTreeMap::<_, PeerState>::new();
            for (i, left) in tables.clone().enumerate() {
                for (j, right) in tables.clone().enumerate() {
                    if i == j {
                        continue;
                    }

                    let left_id = left.local_id;
                    let right_id = right.local_id;

                    if left.contains(&right_id) {
                        states.entry(left_id).or_default().knows_about += 1;
                        states.entry(right_id).or_default().known_by += 1;
                    }
                }
            }
            states
        }
    }

    #[test]
    fn buckets_are_sets() {
        let mut table = RoutingTable::new(rand::random());

        let peer = rand::random();
        assert!(table.add(make_peer_info_stub(peer), MAX_K, &Duration::MAX, Some));
        assert!(table.add(make_peer_info_stub(peer), MAX_K, &Duration::MAX, Some)); // returns true because the node was updated
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn should_not_add_self() {
        let local_id = rand::random();
        let mut table = RoutingTable::new(local_id);

        assert!(!table.add(make_peer_info_stub(local_id), MAX_K, &Duration::MAX, Some));
        assert!(table.is_empty());
    }

    #[test]
    fn max_k_per_bucket() {
        let k = 20;
        let timeout = Duration::MAX;
        let mut bucket = Bucket::with_capacity(k);

        for _ in 0..k {
            assert!(bucket.insert(make_peer_info_stub(rand::random()), k, &timeout, Some));
        }
        assert!(!bucket.insert(make_peer_info_stub(rand::random()), k, &timeout, Some));
    }

    #[test]
    fn find_closest_nodes() {
        let ids = [
            "4a76f9bc07ca82a9a60a198de13721283649b0d1e3eada12e717e922a02e5bb3",
            "0f542142194b68e262a715791380574fe1ba59a440372bb48a6021cadcbe0c80",
            "95e594066d545fe55f3a7da54065f12bfade3205480f2f0c48ea4ab23af955c9",
            "ceec84c6726f140200dfe4b206d46eee82ee94f4539ad5579070ba59d4748065",
            "ef02b1fda8ca4082168a925f8e4f1382764fc8650f5945c64c57a54741fd45b1",
            "d2778cf6161b43fbd552902be3ab56d2059e8e4ab2563b7b54e2f3dc37735686",
            "bd1ab6dcb76bdef6da7f7fb3fcc1d187638e67acf19654157074c0052c267fe1",
            "2709f88a1cda61b92f3036e69a7bcee273721a89e1bcbfa5bf705efcfd66ea5e",
            "cb6eeb5680c581bfab2da1d9c2dbeae43ce27d8c59179f6a2e75c9c63a044db6",
            "75a8edc3ac6fd40dcb3ec28ef27886225dfe267745c5ca237c036f6d60a06c7f",
            "1e7de617e4fd4cd5a5c6c4258dbf24561099e8cb233577a1e2a875f77260a7ab",
            "138f06d98756b78695d115e4cacdb56069f3564ac48f49241d9d61b177948b37",
            "e0e608b434424cfbe6b7995f8b1dec9d8d08cf9e93aa59b8e36fd6f9f2239538",
            "236286b8f8c388ea0877201fd1301e701b474c46f7b5df329fbd3289602074e9",
            "6660dc422459c1e1003a8cdcbd5e3fd722df33e67e26072b582ee8c46c5ad5e9",
            "19b32fcbf5b45bd3679ce5c9e22b11b57a5fcf56a746ff5426857644ccbc272a",
            "fb8c40aaa92e4910a7a47d547c290c598b5aa357a0b96fc3815d7710f682b69c",
            "6cf33e51fa4e0cef076c79bd90e1b50eb4be2cb70a1d0a275bd7aa2760a73e4e",
            "1c72b8583ac3947718238a23863283a2fe8aedc2581d5538f9830441ad3bf84c",
            "c52600bc1018e595739d45433ea4c59ce36fea5242c3c46270c3e61af3f24c26",
            "1127d91d128f383f528e5e8b24bfc683368fd22d3e7e185ac50816f1d51726f4",
            "1d16bbaf7d432418ad0f799d71cdfea0a865f50a3b02543dc846e4797bdcf30d",
            "74ce456b7e956c407625c24522ef5851ae3e2d5630603ff60427fe682e3419ea",
            "12dcaae7276b99335b3b75ae8fd04ce378c5e5c7b0143efd04c3d800e4345061",
            "6f9bde29ef1eae90896e1fb50fe19df9f342af97018e67dae7c8f721280f4243",
            "a8caf1325b29fc49c465b6b8bd6cfc2cbb2d4809153e307956c336b0d4bbd816",
            "b4b5d8eb4c39345dd8bea701a541e8fb4df827964aa3796bad122652ddd5be1e",
            "9f812affedd280a6c13d8916e3f933a7d52d5814bc3c6218a31bfe37cce3befa",
            "beec74f32c5c9b83df70aa0df4e3f5abea71726b2edc123a8bb453ddf3d2de90",
            "d2f0f2e684c6578e60794fee51d3bcb484627bb384027bd07a442216f1a38893",
            "956b9e26da6a429e70828b777d6b843662f1172258129f20273bea63d4c8a467",
            "88361b564dc7d147e411087ac891e8565aadd5b27a2e92a7a7bd7107d02b5fdc",
            "52593e3c6739d16f22723840225a804e0b9a90a86b29cb470345cc5559b6ac49",
            "7289912a703a94fc0df53a80d6e23a8195f4dd04d67e45d31a0426dcc3d5d1b1",
            "ae7c0ca443cf94dd0ee54a0bb7678aa2a82712da2da87813363ff8232ca3a363",
            "db2328dc4fee7c9a888cf3c4099a9e1eb0d5c43d52702898c27ff442307c1934",
            "965d913e0de7251d12985467e9adc9fb4ba87988307cc9e2b940712a818caacd",
            "ba714e28cf5f87e84a6ff8e3187db1ffe0d5788c74067cb8d90bcea184600afa",
            "beb3c47ee72dc88438d694947a619200dfc000dccc5239f719096e42600524ab",
            "882a587dc9f47a0c40074f4232ff67a61865365c626999aff9526d34955757a2",
            "f2ad154d811d2e019d63f8e4f824fba7b72ff13a7e97da12cf717a76ea91273e",
            "45e5e550116a9f49bd25a86120ae1f40f85b2611bd9a45dd8a98a9f9dd647dc6",
            "d5813e9a7a9445b68839db5e7a95e7125090e4ac763cbe32812ae0c5002d1a58",
            "4214d98c9bf2166cc41ef9cf0c37fac68b685358f638e36f2297f426c04f91b4",
            "7cc1af0803f8fea2015577e1a5510310cad5b136d5924919b9e533e66c648e2d",
            "f62ca6a41fa5ed4be443d5b326166a3dafc2c0b1d7dbfcbc709ed22bfddf28a2",
            "c91581e33de7a3e1404c58f559e5d6b4438f27d8bedfbb357b8f064ed86df1f8",
            "d1ac225a8bfaba82776b6da70010d66b29a876385bacc4a4b365d6ffbeeacc86",
            "8c8b75aeeff02c3b88394fe18e7a65534da1b00b36f9446061f7d995484e6177",
            "f7d172ff80f4e451f04ba73e279286f2a4707e290ec4268bc16fe94277c7f733",
            "3205396db7242347cfa75c796839cf5afb7961a9acb01f650c163fb86b332097",
            "7ba5e8fee0239cc2c499161aaecf89d4fa3ebc76b7c8c2d1b305d3309121ca4d",
            "c3c05d9e1d51c2d87d6eb4144a726eec697999ba21552951e9c4eefc07f35df4",
            "771594e90ff55c810a697d901027ca73e286a8977ea19432e95e28761be19319",
            "efcf3927f3456a8eb5e87a2f1d5c582c2bb97336455edad53ce10dcfdbe79420",
            "e96dc8c885a3fc866597c8f3b243b011eb928b81bd4accd4fe08d9277a468b75",
            "b2797ca70e15d10f8079c527ad13da29af6e261b75f6ccbb5908b6e4e7c7dc87",
            "757fe465b20ac4614df7cca23fb3038848fd7fbd0d59afb8800f5e9f212acf40",
            "d2bf6ff26de798c1e8944c6c8a39c22b2299e3192fd3a83347a72d7ec4f80071",
            "30dbca20ebf6c7f4cdcd8ccf0ce78ae858fd3b296b033ff305559896cb22f54f",
            "0a99ceb98807d4f3d217e4b71a7b0cbeb3f79f954088c4f143e1cf046e2132f6",
            "227c54051f6872cae600a006eb0e6840ba3903e61a52a18f4a31e4248c7a68eb",
            "79799ee7e4e0c5d90d900d9e6a1a4b07ec9f0a565a64e109bca4768a26d095b3",
            "2f548b927815ada03b49befad9fc5019d1607f8e3219dd5fa1510b8ae493f064",
            "f146a459753a2fb80f3ff5278f9d1bd33734442fa5048e6e0c97d2ae060f1798",
            "272dc41968edb8784e34ad71a9b7b06a5a5a200b8df1d14c6b68e6451e27c922",
            "5db66920b3d006733c1eb10666b28d83929eede48a7b1fc8f690da2660464c62",
            "99019fa36fe000eeafca8efd5fa5c0e77a3a4ed77a4d7ae526cbc71e57026d06",
            "c2a0c8b2132ef0db36420eef9f5f0f87da43b01cd78a734bb82e55515f8ffd1d",
            "f0c4dac4e62b132b3c3f6086d691c2bf710f1b47e1914eed3fc0a3d4176338a3",
            "4f57644cf2f94cb9f547ad1043f8cc439bd7d47cb31748d68ca79b9bc411f99b",
            "4ed89565bcd28fa1637fd30b474184c289dc8d326dc4fa052be131b8900b338d",
            "b1eb827b1e0b7ca81df1590a5f29818e53a8156634653ae0c02cf3c2a4bb2bde",
            "4fa40df71e0237d39d8cc769c2e7252bf741abc755995bbadd6a7e8f95ab1694",
            "92398a19157e20036d1e9baffb360096524ae045316e988bf5365e0514183e9c",
            "7ca701bffa4a52902298fbe7a7cd383360049cf5fc8201efe17470fd8bbdc7ee",
            "e823a52f49062a18c7f2622ced876ca17985d84e20d278935c230847e5560ed1",
            "712a228b32fb45b91c9691e73daa96fa0136c85796d0cd802905de7b36da5c99",
            "9475a23f0eb50d1573bc6032db822dfda0885bea1eb096cd65eee3bb292c7567",
            "6da8d09bc9115d799efdc7e77b7e488dfd87e566e440fb9f591a1257b7914c9d",
            "f1ca9e1623356604a00f1982837fe10d634b3f758c5b72d5f543548e616d95e5",
            "4e97df7376a778ef083de064d09d9ddd60c42d382bd7d53a721fecdb1e6fba2a",
            "dd429467062dcb9e51832f6c6ab55a361615f56e8be7aed600292241684a8133",
            "0fc4aed5ebbd23755b4e250bcbc44a5accd3a64b3cf9078da1c02cb53dc8c196",
            "8d70a1319a085c4d1c22eac63335085d2c0ddf1a4ffb5b7d93c8a796679e2463",
            "f873f50e465c834e2819d104d9dc904f8a32b3f09eb6a880b8669cf08247913e",
            "69870545d1b886222d4b968aa14c70c0bfa436893e5a6894749e964cd760069c",
            "d5b590ec2b93d9b78e225b254121630ccfaec13be57a1dbf7c915cd922e08d75",
            "2abfc539a31361ee6e830b82149c33c898d4bcd3dea6127b930c05ce354dd474",
            "1a34e99b9561406f55c9eb5c28965ae1458a6573abb6143f2ca793ccd3bcb7c6",
            "5bfe3ac277824dd2d093eeb7241fa8011bbd4dc90ebbad7cce3d055b15524c0b",
            "304884f6ea7d01bfa294edc27562c2ebe660e810087f7b962c168b1b967a8d74",
            "272b32b839b80f4e7c980577ebc41d8d729d8bed66db9522d69f3851a186fbeb",
            "77f06ed2f83251c47af7afef00e9d96729a7d30388fdbe067987a333ea085ede",
            "a942f1858af47d7347696427504b9eafa94af58452fa4a26fcc1a99ed70e78b6",
            "500de9b4be309b5fa9074e856a473090419c2a131e3823636abe0d538e18c712",
            "c30e59f93b5c3a801a31016d2e17e5c7fb5bd525165827466925e8a8cc4dbcd9",
            "ffce42b385ed2abdc6eef6b88fd963522b57bfea2f9c7f6b230eb1c518912edf",
            "750b037a6a8b833ee976ce27120e31807b644626e526a5e4fff3bfcfeed374dd",
            "93a756cd44f530a9a072b6573421ba1ade3a7fe35494a2fc308da2ed58c1a7f7",
        ];
        let ids = ids
            .into_iter()
            .map(PeerId::from_str)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        let local_id =
            PeerId::from_str("bdbc554024c65b463b0f0a01037b55985190f4fc01c47dc81c19aab4b4b2d9ab")
                .unwrap();

        let mut table = RoutingTable::new(local_id);
        for id in ids {
            table.add(make_peer_info_stub(id), MAX_K, &Duration::MAX, Some);
        }

        {
            let expected_closest_ids = [
                "882a587dc9f47a0c40074f4232ff67a61865365c626999aff9526d34955757a2",
                "88361b564dc7d147e411087ac891e8565aadd5b27a2e92a7a7bd7107d02b5fdc",
                "8c8b75aeeff02c3b88394fe18e7a65534da1b00b36f9446061f7d995484e6177",
                "92398a19157e20036d1e9baffb360096524ae045316e988bf5365e0514183e9c",
                "9475a23f0eb50d1573bc6032db822dfda0885bea1eb096cd65eee3bb292c7567",
                "956b9e26da6a429e70828b777d6b843662f1172258129f20273bea63d4c8a467",
                "95e594066d545fe55f3a7da54065f12bfade3205480f2f0c48ea4ab23af955c9",
                "965d913e0de7251d12985467e9adc9fb4ba87988307cc9e2b940712a818caacd",
                "99019fa36fe000eeafca8efd5fa5c0e77a3a4ed77a4d7ae526cbc71e57026d06",
                "9f812affedd280a6c13d8916e3f933a7d52d5814bc3c6218a31bfe37cce3befa",
                "a8caf1325b29fc49c465b6b8bd6cfc2cbb2d4809153e307956c336b0d4bbd816",
                "a942f1858af47d7347696427504b9eafa94af58452fa4a26fcc1a99ed70e78b6",
                "ae7c0ca443cf94dd0ee54a0bb7678aa2a82712da2da87813363ff8232ca3a363",
                "b1eb827b1e0b7ca81df1590a5f29818e53a8156634653ae0c02cf3c2a4bb2bde",
                "b2797ca70e15d10f8079c527ad13da29af6e261b75f6ccbb5908b6e4e7c7dc87",
                "b4b5d8eb4c39345dd8bea701a541e8fb4df827964aa3796bad122652ddd5be1e",
                "ba714e28cf5f87e84a6ff8e3187db1ffe0d5788c74067cb8d90bcea184600afa",
                "bd1ab6dcb76bdef6da7f7fb3fcc1d187638e67acf19654157074c0052c267fe1",
                "beb3c47ee72dc88438d694947a619200dfc000dccc5239f719096e42600524ab",
                "beec74f32c5c9b83df70aa0df4e3f5abea71726b2edc123a8bb453ddf3d2de90",
            ];
            let expected_closest_ids = expected_closest_ids
                .into_iter()
                .map(PeerId::from_str)
                .collect::<Result<Vec<_>, _>>()
                .unwrap();

            let mut closest = table
                .closest(local_id.as_bytes(), 20)
                .into_iter()
                .map(|item| item.id)
                .collect::<Vec<_>>();
            closest.sort();
            assert_eq!(closest, expected_closest_ids);
        }

        {
            let expected_closest_ids = [
                "c3c05d9e1d51c2d87d6eb4144a726eec697999ba21552951e9c4eefc07f35df4",
                "c52600bc1018e595739d45433ea4c59ce36fea5242c3c46270c3e61af3f24c26",
                "c91581e33de7a3e1404c58f559e5d6b4438f27d8bedfbb357b8f064ed86df1f8",
                "cb6eeb5680c581bfab2da1d9c2dbeae43ce27d8c59179f6a2e75c9c63a044db6",
                "ceec84c6726f140200dfe4b206d46eee82ee94f4539ad5579070ba59d4748065",
                "d1ac225a8bfaba82776b6da70010d66b29a876385bacc4a4b365d6ffbeeacc86",
                "d2778cf6161b43fbd552902be3ab56d2059e8e4ab2563b7b54e2f3dc37735686",
                "d2bf6ff26de798c1e8944c6c8a39c22b2299e3192fd3a83347a72d7ec4f80071",
                "d2f0f2e684c6578e60794fee51d3bcb484627bb384027bd07a442216f1a38893",
                "d5813e9a7a9445b68839db5e7a95e7125090e4ac763cbe32812ae0c5002d1a58",
                "db2328dc4fee7c9a888cf3c4099a9e1eb0d5c43d52702898c27ff442307c1934",
                "e0e608b434424cfbe6b7995f8b1dec9d8d08cf9e93aa59b8e36fd6f9f2239538",
                "e96dc8c885a3fc866597c8f3b243b011eb928b81bd4accd4fe08d9277a468b75",
                "ef02b1fda8ca4082168a925f8e4f1382764fc8650f5945c64c57a54741fd45b1",
                "efcf3927f3456a8eb5e87a2f1d5c582c2bb97336455edad53ce10dcfdbe79420",
                "f146a459753a2fb80f3ff5278f9d1bd33734442fa5048e6e0c97d2ae060f1798",
                "f2ad154d811d2e019d63f8e4f824fba7b72ff13a7e97da12cf717a76ea91273e",
                "f62ca6a41fa5ed4be443d5b326166a3dafc2c0b1d7dbfcbc709ed22bfddf28a2",
                "f7d172ff80f4e451f04ba73e279286f2a4707e290ec4268bc16fe94277c7f733",
                "fb8c40aaa92e4910a7a47d547c290c598b5aa357a0b96fc3815d7710f682b69c",
            ];
            let expected_closest_ids = expected_closest_ids
                .into_iter()
                .map(PeerId::from_str)
                .collect::<Result<Vec<_>, _>>()
                .unwrap();

            let target = PeerId::from_str(
                "d41f603e6bd24f1c3e2eb4d97d81fd155dd307f5b5c9be443a1a229bd1392b72",
            )
            .unwrap();

            let mut closest = table
                .closest(target.as_bytes(), 20)
                .into_iter()
                .map(|item| item.id)
                .collect::<Vec<_>>();
            closest.sort();
            assert_eq!(closest, expected_closest_ids);
        }
    }

    #[test]
    fn everyone_can_know_about_everyone() {
        let peer_info =
            std::array::from_fn::<_, { MAX_K }, _>(|_| make_peer_info_stub(rand::random()));
        let mut tables = peer_info
            .iter()
            .map(|peer_info| SimpleRoutingTable::new(peer_info.id))
            .collect::<Vec<_>>();

        for (i, left) in tables.iter_mut().enumerate() {
            for (j, right) in peer_info.iter().enumerate() {
                if i == j {
                    continue;
                }

                left.add(right.clone(), MAX_K, &Duration::MAX, Some);
            }
        }

        let states = PeerState::compute_states(tables.iter());

        assert_eq!(states.len(), MAX_K);
        for state in states.values() {
            assert_eq!(state.knows_about, MAX_K - 1);
            assert_eq!(state.known_by, MAX_K - 1);
        }
    }

    #[test]
    fn announces_are_distributed_properly() {
        let peer_info =
            std::array::from_fn::<_, { MAX_K }, _>(|_| make_peer_info_stub(rand::random()));
        let mut tables = peer_info
            .iter()
            .map(|peer_info| (peer_info.id, SimpleRoutingTable::new(peer_info.id)))
            .collect::<BTreeMap<_, _>>();

        // Make sure everyone knows about the first node
        for table in tables.values_mut() {
            table.add(peer_info[0].clone(), MAX_K, &Duration::MAX, Some);
        }

        // Announce peer info (aka push)
        for peer_info in &peer_info {
            let key_hash = tl_proto::hash(crate::proto::dht::PeerValueKeyRef {
                name: crate::proto::dht::PeerValueKeyName::NodeInfo,
                peer_id: &peer_info.id,
            });

            let mut target_ids = Vec::new();
            tables[&peer_info.id].visit_closest(&key_hash, 3, |node| {
                target_ids.push(node.id);
            });

            for peer_id in &target_ids {
                tables.get_mut(peer_id).unwrap().add(
                    peer_info.clone(),
                    MAX_K,
                    &Duration::MAX,
                    Some,
                );
            }
        }

        // Refresh routing tables (aka pull)
        for peer_id in peer_info.iter().map(|info| &info.id) {
            let mut random_ids = Vec::new();

            for (&distance, bucket) in &tables[peer_id].buckets {
                if bucket.is_empty() || distance == 0 {
                    continue;
                }

                random_ids.push(crate::dht::random_key_at_distance(
                    peer_id,
                    distance,
                    &mut rand::rng(),
                ));
            }

            for random_id in random_ids {
                let key_hash = tl_proto::hash(crate::proto::dht::PeerValueKeyRef {
                    name: crate::proto::dht::PeerValueKeyName::NodeInfo,
                    peer_id: &random_id,
                });

                let mut target_ids = Vec::new();
                tables[peer_id].visit_closest(&key_hash, 3, |node| {
                    target_ids.push(node.id);
                });

                for target_peer_id in &target_ids {
                    let response = tables[target_peer_id].closest(&key_hash, MAX_K);

                    let table = tables.get_mut(peer_id).unwrap();
                    for peer_info in response {
                        table.add(peer_info.clone(), MAX_K, &Duration::MAX, Some);
                    }
                }
            }
        }

        let states = PeerState::compute_states(tables.values());

        assert_eq!(states.len(), MAX_K);
        for state in states.values() {
            assert_eq!(state.knows_about, MAX_K - 1);
            assert_eq!(state.known_by, MAX_K - 1);
        }
    }
}
