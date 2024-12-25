use std::collections::hash_map::Entry;

use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use tokio::sync::watch;
use tycho_network::PeerId;
use tycho_util::FastHashMap;

use crate::effects::AltFormat;
use crate::models::{PeerCount, PointInfo, Round, ValidPoint};

pub struct Threshold {
    round: Round,
    peer_count: PeerCount,
    includes: watch::Sender<FastHashMap<PeerId, PointInfo>>,
}

impl Threshold {
    pub fn new(round: Round, peer_count: PeerCount) -> Self {
        Self {
            round,
            peer_count,
            includes: watch::Sender::new(FastHashMap::default()),
        }
    }
    pub fn add(&self, valid: &ValidPoint) {
        assert_eq!(valid.info.round(), self.round, "point round mismatch");
        self.includes.send_modify(
            |collected| match collected.entry(valid.info.data().author) {
                Entry::Vacant(vacant) => {
                    vacant.insert(valid.info.clone());
                }
                Entry::Occupied(other) => {
                    panic!(
                        "cannot add to threshold same author twice: exists {:?} new digest {}",
                        other.get().id().alt(),
                        valid.info.digest().alt()
                    );
                }
            },
        );
    }
    /// used in between calls to [`Self::reached`]
    pub fn count(&self) -> usize {
        self.includes.borrow().len()
    }
    pub fn get(&self) -> Vec<PointInfo> {
        let snapshot = self.includes.borrow();

        assert!(
            snapshot.len() >= self.peer_count.majority(),
            "{} is not reached at round {}: currently {}",
            self.peer_count.majority(),
            self.round.0,
            snapshot.len()
        );

        snapshot.values().cloned().collect()
    }
    pub fn reached(&self) -> BoxFuture<'static, ()> {
        let mut rx = self.includes.subscribe();
        let expected = self.peer_count.majority();
        let round = self.round;
        async move {
            let task = rx
                .wait_for(|includes| includes.len() >= expected)
                .map(|result| result.map(|_count| ()));
            match task.await {
                Ok(()) => (),
                Err(e) => {
                    tracing::warn!("threshold for round {} will hang: {e}", round.0);
                    futures_util::future::pending::<()>().await;
                }
            }
        }
        .boxed()
    }
}

#[cfg(all(test, feature = "test"))]
mod test {
    use std::sync::Arc;
    use std::time::Duration;

    use everscale_crypto::ed25519::KeyPair;
    use rand::thread_rng;

    use super::*;
    use crate::dag::threshold::Threshold;
    use crate::engine::CachedConfig;
    use crate::models::{DagPoint, Link, PeerCount, Point, PointData, UnixTime};
    use crate::test_utils::default_test_config;

    #[tokio::test]
    async fn test() -> anyhow::Result<()> {
        let total_peers = 10;
        let round = Round(5);

        let thresh = Arc::new(Threshold::new(round, PeerCount::try_from(total_peers)?));
        let thresh2 = thresh.clone();

        let handle = tokio::spawn(async move {
            thresh2.reached().await;
            println!("reached");
        });

        CachedConfig::init(&default_test_config());

        for i in 1..=total_peers {
            let keypair = KeyPair::generate(&mut thread_rng());

            let info = PointInfo::from(&Point::new(
                &keypair,
                round,
                Default::default(),
                Default::default(),
                PointData {
                    author: PeerId::from(keypair.public_key),
                    includes: Default::default(),
                    witness: Default::default(),
                    anchor_trigger: Link::ToSelf,
                    anchor_proof: Link::ToSelf,
                    time: UnixTime::now(),
                    anchor_time: UnixTime::now(),
                },
            ));

            tokio::time::sleep(Duration::from_millis(10)).await;
            let dag_point = DagPoint::new_valid(info, false);
            let valid = dag_point.valid().expect("created as valid");
            thresh.add(valid);
            println!("count {}", thresh.count());
            assert_eq!(i, thresh.count(), "must return count");
        }

        handle.await?;
        Ok(())
    }
}
