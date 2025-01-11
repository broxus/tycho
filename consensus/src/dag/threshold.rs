use std::sync::atomic;
use std::sync::atomic::AtomicU32;
use std::time::Duration;

use ahash::HashMapExt;
use futures_util::StreamExt;
use tokio::sync::{mpsc, Mutex};
use tokio_util::time::DelayQueue;
use tycho_network::PeerId;
use tycho_util::FastHashMap;

use crate::effects::AltFormat;
use crate::engine::CachedConfig;
use crate::models::{PeerCount, PointInfo, Round, UnixTime, ValidPoint};

/// NOTE see [`Threshold::reached()`] for comments on limited usability
pub struct Threshold {
    round: Round,
    target_count: usize,
    clock_skew: UnixTime,
    count: AtomicU32,
    sender: mpsc::Sender<PointInfo>,
    work: Mutex<ThresholdWork>,
}

struct ThresholdWork {
    is_reached: bool,
    ready: FastHashMap<PeerId, PointInfo>,
    delayed: DelayQueue<PointInfo>,
    receiver: mpsc::Receiver<PointInfo>,
}

impl Threshold {
    pub fn new(round: Round, peer_count: PeerCount) -> Self {
        let (sender, receiver) = mpsc::channel(peer_count.full());
        let target_count = peer_count.majority();
        let count = ThresholdCount {
            target: target_count as u8,
            ready: 0,
            delayed: 0,
            in_channel: 0,
        };
        Self {
            round,
            target_count,
            count: AtomicU32::new(count.pack()),
            clock_skew: UnixTime::from_millis(CachedConfig::get().consensus.clock_skew_millis as _),
            sender,
            work: Mutex::new(ThresholdWork {
                is_reached: false,
                ready: FastHashMap::with_capacity(peer_count.full()),
                delayed: DelayQueue::with_capacity(peer_count.full()),
                receiver,
            }),
        }
    }

    /// used both in between and during calls to [`Self::reached`]
    pub fn count(&self) -> ThresholdCount {
        ThresholdCount::unpack(self.count.load(atomic::Ordering::Relaxed))
    }

    pub fn add(&self, valid: &ValidPoint) {
        assert_eq!(valid.info.round(), self.round, "point round mismatch");
        // count no matter if threshold is already reached;
        // increase counter before send to reduce it upon receive;
        ThresholdCount::add_one_in_channel(&self.count);
        match self.sender.try_send(valid.info.clone()) {
            Ok(()) => {}
            Err(e) => {
                // consider impossible
                panic!("cannot add {:?} to threshold: {e}", valid.info.id().alt())
            }
        };
    }

    /// **WARNING** lock design is simple and is suited **only** for current use cases
    ///     that do not overlap during each Engine round task:
    /// * `.reached()` is used for current dag round in [`Collector`](crate::intercom::Collector)
    /// * `.reached()` is used for previous dag round before point is produced
    /// * [`Self::get_reached()`] is used for previous dag round to produce point
    ///     after `.reached()` was awaited
    pub async fn reached(&self) {
        let mut work = self.work.lock().await;

        if work.is_reached {
            return;
        }

        let ThresholdWork {
            ready,
            delayed,
            receiver,
            ..
        } = &mut *work;

        // use last value and update when some point doesn't fit
        let mut max_time = UnixTime::now() + self.clock_skew;

        while ready.len() < self.target_count {
            let (info, is_from_channel) = tokio::select! {
                Some(info) = receiver.recv() => {
                    let mut to_delay = info.data().time - max_time;
                    if to_delay.millis() > 0 {
                        max_time = UnixTime::now() + self.clock_skew;
                        to_delay = info.data().time - max_time;
                    }

                    if to_delay.millis() > 0 {
                        delayed.insert(info, Duration::from_millis(to_delay.millis()));
                        ThresholdCount::set_decrease(&self.count, ready, delayed, true as u8);
                        continue;
                    } else {
                        (info, true)
                    }
                },
                Some(expired) = delayed.next() => (expired.into_inner(), false)
            };

            Self::push_ready(ready, info);
            ThresholdCount::set_decrease(&self.count, ready, delayed, is_from_channel as u8);
        }

        work.is_reached = true;
    }

    /// use only after [`Self::reached()`] was awaited to completion
    pub fn get_reached(&self) -> Vec<PointInfo> {
        let mut work = match self.work.try_lock() {
            Ok(guard) => guard,
            Err(e) => panic!("threshold lock must be released: {e}"),
        };

        assert!(
            work.is_reached,
            "threshold was not reached, cannot get its contents"
        );

        let max_time = UnixTime::now() + self.clock_skew;

        loop {
            let Some(next_key) = work.delayed.peek() else {
                break;
            };
            // manually re-check expiration
            let info = work.delayed.remove(&next_key).into_inner();
            let to_delay = info.data().time - max_time;
            if to_delay.millis() > 0 {
                work.delayed
                    .insert(info, Duration::from_millis(to_delay.millis()));
                break; // peek is ordered by duration, so others are left delayed anyway
            } else {
                Self::push_ready(&mut work.ready, info);
            }
        }
        let mut removed_from_channel: u8 = 0;
        while let Ok(info) = work.receiver.try_recv() {
            removed_from_channel = removed_from_channel
                .checked_add(1)
                .expect("cannot overflow");
            let to_delay = info.data().time - max_time;
            if to_delay.millis() > 0 {
                work.delayed
                    .insert(info, Duration::from_millis(to_delay.millis()));
            } else {
                Self::push_ready(&mut work.ready, info);
            }
        }

        ThresholdCount::set_decrease(
            &self.count,
            &work.ready,
            &work.delayed,
            removed_from_channel,
        );

        work.ready.values().cloned().collect()
    }

    fn push_ready(ready: &mut FastHashMap<PeerId, PointInfo>, info: PointInfo) {
        ready
            .entry(info.data().author)
            .and_modify(|old| {
                panic!(
                    "cannot add to threshold same author twice: exists {:?} new digest {}",
                    old.id().alt(),
                    info.digest().alt()
                )
            })
            .or_insert(info);
    }
}

// u8 is safe because: all items are unique by peer, all peers are in v_set, and v_set fits u8;
// every item is either ready or delayed or in channel
pub struct ThresholdCount {
    pub target: u8,
    pub ready: u8,
    pub delayed: u8,
    pub in_channel: u8,
}

impl ThresholdCount {
    fn add_one_in_channel(count: &AtomicU32) {
        count
            .fetch_update(
                atomic::Ordering::Release,
                atomic::Ordering::Relaxed,
                |packed| {
                    let mut count = ThresholdCount::unpack(packed);
                    count.in_channel = count.in_channel.checked_add(1)?;
                    Some(count.pack())
                },
            )
            .expect("threshold in-channel counter overflow");
    }

    fn set_decrease(
        count: &AtomicU32,
        ready: &FastHashMap<PeerId, PointInfo>,
        delayed: &DelayQueue<PointInfo>,
        removed_from_channel: u8,
    ) {
        let ready_len = u8::try_from(ready.len()).expect("too many ready includes");
        let delayed_len = u8::try_from(delayed.len()).expect("too many delayed");
        count
            .fetch_update(
                atomic::Ordering::Acquire,
                atomic::Ordering::Relaxed,
                |packed| {
                    let mut count = ThresholdCount::unpack(packed);
                    count.ready = ready_len;
                    count.delayed = delayed_len;
                    count.in_channel = count.in_channel.checked_sub(removed_from_channel)?;
                    Some(count.pack())
                },
            )
            .expect("threshold in-channel underflow");
    }

    fn pack(&self) -> u32 {
        u32::from_be_bytes([self.target, self.ready, self.delayed, self.in_channel])
    }

    fn unpack(packed: u32) -> Self {
        let [target, ready, delayed, in_channel] = packed.to_be_bytes();
        Self {
            target,
            ready,
            delayed,
            in_channel,
        }
    }

    pub fn total(&self) -> u8 {
        match self
            .ready
            .checked_add(self.delayed)
            .and_then(|all| all.checked_add(self.in_channel))
        {
            Some(total) => total,
            None => panic!("threshold total count overflow: {self}"), // cannot happen
        }
    }
}

impl std::fmt::Display for ThresholdCount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}+{}+{}/{}",
            self.ready, self.delayed, self.in_channel, self.target
        )
    }
}

#[cfg(all(test, feature = "test"))]
mod test {
    use std::sync::Arc;
    use std::time::Duration;

    use everscale_crypto::ed25519::KeyPair;
    use rand::{thread_rng, Rng};

    use super::*;
    use crate::dag::threshold::Threshold;
    use crate::engine::CachedConfig;
    use crate::models::{DagPoint, Link, PeerCount, Point, PointData, PointStatusValid, UnixTime};
    use crate::test_utils::default_test_config;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test() {
        let total_peers = 10;
        let round = Round(5);
        CachedConfig::init(&default_test_config());

        let peer_count = PeerCount::try_from(total_peers).expect("cannot fail");

        let thresh = Arc::new(Threshold::new(round, peer_count));

        let handle = tokio::spawn({
            let thresh = thresh.clone();
            async move {
                println!("start count {}", thresh.count());
                thresh.reached().await;
                let count = thresh.count();
                println!("reached {count}");
                assert!(count.ready as usize >= peer_count.majority());
            }
        });

        CachedConfig::init(&default_test_config());

        let now = UnixTime::now();

        for i in 1..=total_peers {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let dag_point = new_valid_point(round, now);
            let valid = dag_point.valid().expect("created as valid");
            thresh.add(valid);
            println!("added #{i} total count {}", thresh.count());
        }

        handle.await.expect("join handle");

        // threshold state changes only when someone calls `.reached()` or `.get_reached()`

        {
            let mut work = thresh.work.try_lock().expect("no one holds the lock now");
            let count = thresh.count();

            assert_eq!(count.ready as usize, work.ready.len(), "ready count");

            assert_eq!(count.delayed as usize, work.delayed.len(), "delayed count");

            let mut channel_contents = Vec::new();
            while let Ok(info) = work.receiver.try_recv() {
                channel_contents.push(info);
            }

            assert_eq!(
                count.in_channel as usize,
                channel_contents.len(),
                "in-channel count"
            );

            // return items directly to leave counter untouched
            for info in channel_contents {
                (thresh.sender.try_send(info)).expect("failed to return item to channel");
            }
        }

        let result = thresh.get_reached();

        let count = thresh.count();

        println!("result items: {}", result.len());
        println!("final count: {count}");

        assert!(
            result.len() >= peer_count.majority(),
            "must be reached, {count}"
        );

        assert_eq!(
            count.ready as usize,
            result.len(),
            "threshold can change its state only when called"
        );

        assert_eq!(
            count.total() as usize,
            total_peers,
            "all sent items must be counted"
        );

        let thresh = Arc::into_inner(thresh).expect("must be single reference");
        let work = thresh.work.into_inner();

        assert_eq!(count.ready as usize, work.ready.len(), "ready count");

        assert_eq!(count.delayed as usize, work.delayed.len(), "delayed count");

        assert_eq!(
            count.in_channel, 0,
            "all items must be drained from channel"
        );
    }

    fn new_valid_point(round: Round, now: UnixTime) -> DagPoint {
        let status = PointStatusValid::default();
        let keypair = KeyPair::generate(&mut thread_rng());

        let delay = UnixTime::from_millis(thread_rng().gen_range(1000..8000));

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
                time: now + delay,
                anchor_time: UnixTime::now(),
            },
        ));

        DagPoint::new_valid(info, &status)
    }
}
