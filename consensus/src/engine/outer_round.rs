use std::marker::PhantomData;

use tokio::sync::watch;

use crate::models::Round;

/// Marker trait to distinguish between data sources despite variable names
// `Clone` derived for `OuterRound<T>` brakes with weird messages
// at usages of `.clone()` if `T` is not `Clone`, thus needed only for `PhantomData<T>`
pub trait Source: Clone {}

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
/// Allows to clean storage with its own pace, repeating it as soon as both:
/// previous task completed and consensus round advanced.
#[derive(Clone)]
pub struct Consensus;
impl Source for Consensus {}

#[derive(Clone)]
pub struct OuterRound<T: Source> {
    tx: watch::Sender<Round>,
    _phantom_data: PhantomData<T>,
}
impl<T: Source> Default for OuterRound<T> {
    fn default() -> Self {
        Self {
            tx: watch::Sender::new(Round::BOTTOM),
            _phantom_data: Default::default(),
        }
    }
}

impl<T: Source> OuterRound<T> {
    pub fn get(&self) -> Round {
        *self.tx.borrow()
    }

    pub fn set_max_raw(&self, value: u32) {
        self.set_max(Round(value));
    }

    pub fn set_max(&self, value: Round) {
        self.tx.send_if_modified(|old| {
            let old_is_lesser = *old < value;
            if old_is_lesser {
                *old = value;
            }
            old_is_lesser
        });
    }

    // not available to collator or adapter
    pub(crate) fn receiver(&self) -> OuterRoundRecv<T> {
        OuterRoundRecv {
            rx: self.tx.subscribe(),
            _phantom_data: Default::default(),
        }
    }
}

// no `Clone` and not available to collator or adapter
pub(crate) struct OuterRoundRecv<T: Source> {
    rx: watch::Receiver<Round>,
    _phantom_data: PhantomData<T>,
}

impl<T: Source> OuterRoundRecv<T> {
    pub fn get(&self) -> Round {
        *self.rx.borrow()
    }

    /// does not return (hardly viable) default value
    pub async fn next(&mut self) -> Round {
        self.rx.changed().await.expect("sender is dropped");
        *self.rx.borrow_and_update()
    }
}

// Keep outdated drop-in replacement for some time
#[allow(dead_code)]
#[deprecated]
mod unused {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    use crate::models::Round;

    #[derive(Clone)]
    pub struct OuterRoundAtomic(Arc<AtomicU32>);
    impl Default for OuterRoundAtomic {
        fn default() -> Self {
            Self(Arc::new(AtomicU32::new(Round::BOTTOM.0)))
        }
    }

    impl OuterRoundAtomic {
        pub fn get(&self) -> Round {
            Round(self.0.load(Ordering::Relaxed))
        }

        pub fn set_max_raw(&self, value: u32) {
            self.0.fetch_max(value, Ordering::Relaxed);
        }

        pub fn set_max(&self, round: Round) {
            self.0.fetch_max(round.0, Ordering::Relaxed);
        }
    }
}
