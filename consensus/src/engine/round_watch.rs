use std::marker::PhantomData;
use std::sync::atomic::AtomicU32;
use std::sync::{atomic, Arc};

use tokio::sync::Notify;

use crate::models::Round;

/// Marker trait to distinguish between data sources despite variable names
// `Clone` derived for `OuterRound<T>` brakes with weird messages
// at usages of `.clone()` if `T` is not `Clone`, thus needed only for `PhantomData<T>`
pub trait Source: Clone {}

/// Round is defined in the local collator by a top known block,
/// i.e. a block with the greatest `seq_no`
/// that obtained 2/3+1 signatures and is kept in local storage,
/// but which state update is not necessarily applied.
///
/// Does not have any effect on storage cleaning:
/// if collator is left too far behind, it has to catchup by itself.
///
/// ### Atomic-style usage
/// Allows the collator to put local mempool into pause mode, so its round will not advance:
/// dag growth, putting new broadcasts into dag, validating and signing them are put on hold.
///
/// ### Channel-style usage
/// Collator signals mempool to exit pause mode immediately and keep producing new points.
#[derive(Clone)]
pub struct TopKnownAnchor;
impl Source for TopKnownAnchor {}

/// Allows a node to drive consensus by collected dependencies with
/// [`Collector`](crate::intercom::Collector)
/// or follow it from broadcasts received by
/// [`BroadcastFilter`](crate::intercom::BroadcastFilter)
///
/// ### Atomic-style usage
/// `BroadcastFilter` sends reliably determined rounds (and their points) to `Collector` via channel,
/// but `Collector` doesn't know about the latest round, until it consumes the channel to the end.
///
/// Also, `BroadcastFilter` continues its work when [`Engine`](crate::Engine)
/// changes current dag round and takes some (little) time to respawn `Collector` and other tasks.
///
/// ### Channel-style usage
/// To clean storage.
#[derive(Clone)]
pub struct Consensus;
impl Source for Consensus {}

/// Commit procedure is separated into info part in dag and storage part later in adapter.
/// Commit is not finished, until payload data is read from storage, so it may be cleaned.
/// Mempool Adapter may decide to skip reading some out of interest data,
/// but it will mark stored data with committed status anyway.
///
/// ### Channel-style usage
/// Allows to clean storage with its own pace, repeating it as soon as both:
/// previous task completed and a new anchor was committed.
#[derive(Clone)]
pub struct Commit;
impl Source for Commit {}

#[derive(Clone)]
pub struct RoundWatch<T: Source>(Arc<RoundWatchInner<T>>);

pub struct RoundWatchInner<T: Source> {
    notify: Notify,
    round: AtomicU32,
    _phantom_data: PhantomData<T>,
}

impl<T: Source> Default for RoundWatch<T> {
    fn default() -> Self {
        Self(Arc::new(RoundWatchInner {
            notify: Notify::new(),
            round: AtomicU32::new(Round::BOTTOM.0),
            _phantom_data: Default::default(),
        }))
    }
}

impl<T: Source> RoundWatch<T> {
    /// **warning** do not use prior [`Self::receiver`], as the latter may skip updates;
    ///
    /// either use only on sender side, or prefer [`RoundWatcher::get`]
    pub fn get(&self) -> Round {
        Round(self.0.round.load(atomic::Ordering::Acquire))
    }

    pub fn set_max_raw(&self, value: u32) {
        if self.0.round.fetch_max(value, atomic::Ordering::AcqRel) < value {
            self.0.notify.notify_waiters();
        }
    }

    pub fn set_max(&self, value: Round) {
        self.set_max_raw(value.0);
    }

    // not available to collator or adapter
    pub fn receiver(&self) -> RoundWatcher<T> {
        RoundWatcher {
            watch: self.clone(),
            // SAFETY: parent is kept in struct so `Notified` preserves parent lifetime
            last_value: self.get(),
        }
    }
}

// no `Clone` and not available to collator or adapter
pub struct RoundWatcher<T: Source> {
    watch: RoundWatch<T>,
    last_value: Round,
}

impl<T: Source> RoundWatcher<T> {
    /// the only way to inspect the current value; [`Self::next`] may return it or a greater one
    pub fn get(&self) -> Round {
        self.watch.get()
    }

    /// does not return (hardly viable) default value, as any other prior [`Self`] creation
    pub async fn next(&mut self) -> Round {
        let notified = self.watch.0.notify.notified();

        let last_value = self.watch.get();
        if self.last_value < last_value {
            self.last_value = last_value;
            return last_value;
        }

        notified.await;

        let last_value = self.watch.get();
        assert!(
            self.last_value < last_value,
            "notified value is not greater than previous"
        );
        self.last_value = last_value;

        last_value
    }
}
