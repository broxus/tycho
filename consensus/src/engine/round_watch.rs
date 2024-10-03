use std::marker::PhantomData;

use tokio::sync::watch;

use crate::engine::MempoolConfig;
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
/// Allows the collator to put local mempool into silent mode:
/// with the start of a new round it will keep collecting and signing broadcasts,
/// downloading and uploading dependencies, validating and committing points,
/// but creation and broadcast of new points is forbidden.
///
/// ### Channel-style usage
/// Collator signals mempool to exit silent mode immediately and keep producing new points.
#[derive(Clone)]
pub struct TopKnownAnchor;
impl Source for TopKnownAnchor {}
impl TopKnownAnchor {
    pub fn adapter_history_bottom(top_known_anchor: Round) -> Round {
        // oldest unique data to collate (including latest collated round)
        let round = (top_known_anchor.0)
            .saturating_sub(MempoolConfig::commit_depth() as u32)
            .saturating_sub(MempoolConfig::deduplicate_rounds() as u32);
        Round(round).max(MempoolConfig::genesis_round())
    }
    pub fn silence_upper_bound(top_known_anchor: Round) -> Round {
        // oldest unique data to collate (including latest collated round)
        let round =
            (top_known_anchor.0).saturating_add(MempoolConfig::max_anchor_distance() as u32);
        Round(round).max(MempoolConfig::genesis_round())
    }
}

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
impl Consensus {
    pub fn history_bottom(consensus: Round) -> Round {
        // enough to handle acceptable collator lag
        let round = (consensus.0)
            // before silent mode
            .saturating_sub(MempoolConfig::max_anchor_distance() as u32)
            // before top known block in silent mode
            .saturating_sub(MempoolConfig::acceptable_collator_lag() as u32)
            // oldest data to collate as unique
            .saturating_sub(MempoolConfig::commit_depth() as u32) // data to collate
            .saturating_sub(MempoolConfig::deduplicate_rounds() as u32); // as unique
        Round(round).max(MempoolConfig::genesis_round())
    }
}

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
impl Commit {
    pub fn stored_history_bottom(commit: Round) -> Round {
        // oldest data to collate that is validatable and unique
        let round = (commit.0)
            .saturating_sub(MempoolConfig::commit_depth() as u32) // data to collate
            .saturating_sub(
                (MempoolConfig::max_anchor_distance() as u32) // validatable by other peers
                    .max(MempoolConfig::deduplicate_rounds() as u32), // unique
            );
        Round(round).max(MempoolConfig::genesis_round())
    }
}

#[derive(Clone)]
pub struct RoundWatch<T: Source> {
    tx: watch::Sender<Round>,
    _phantom_data: PhantomData<T>,
}
impl<T: Source> Default for RoundWatch<T> {
    fn default() -> Self {
        Self {
            tx: watch::Sender::new(Round::BOTTOM),
            _phantom_data: Default::default(),
        }
    }
}

impl<T: Source> RoundWatch<T> {
    /// **warning** do not use prior [`Self::receiver`], as the latter may skip updates;
    ///
    /// either use only on sender side, or prefer [`RoundWatcher::get`]
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
                // let mut type_name = std::any::type_name::<T>();
                // type_name = type_name.split(":").last().unwrap_or(type_name);
                // tracing::warn!("{type_name} {} -> {}", old.0, value.0);
                *old = value;
            }
            old_is_lesser
        });
    }

    // not available to collator or adapter
    pub fn receiver(&self) -> RoundWatcher<T> {
        RoundWatcher {
            rx: self.tx.subscribe(),
            _phantom_data: Default::default(),
        }
    }
}

// no `Clone` and not available to collator or adapter
pub struct RoundWatcher<T: Source> {
    rx: watch::Receiver<Round>,
    _phantom_data: PhantomData<T>,
}

impl<T: Source> RoundWatcher<T> {
    /// the only way to inspect the value upon creation, as [`Self::next`] will not return it
    pub fn get(&self) -> Round {
        *self.rx.borrow()
    }

    /// does not return (hardly viable) default value, as any other prior [`Self`] creation
    pub async fn next(&mut self) -> Round {
        match self.rx.changed().await {
            Ok(()) => *self.rx.borrow_and_update(),
            Err(e) => {
                let mut type_name = std::any::type_name::<T>();
                type_name = type_name.split(":").last().unwrap_or(type_name);
                tracing::error!("{type_name} watch sender is dropped, {e}");
                futures_util::future::pending().await
            }
        }
    }
}
