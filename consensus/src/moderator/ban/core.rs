use std::collections::hash_map;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio_util::time::delay_queue::Expired;
use tokio_util::time::{DelayQueue, delay_queue};
use tycho_network::PeerId;
use tycho_util::FastHashMap;
use tycho_util::futures::JoinTask;

use crate::intercom::PeerSchedule;
use crate::models::UnixTime;
use crate::moderator::ban::state::BanCoreState;
use crate::moderator::ban::{CurrentBan, UpdaterQueueItem};
use crate::moderator::journal::item::{
    BanItem, BanOrigin, JournalItem, JournalItemFull, UnbanItem, UnbanOrigin,
};
use crate::moderator::journal::record_key::RecordKeyFactory;
use crate::moderator::{BanConfig, DelayedDbTask, JournalEvent, RecordKey, RecordValueShort};

#[derive(Clone)]
pub struct BanCore(Arc<BanCoreInner>);

struct BanCoreInner {
    config: BanConfig,
    state: Mutex<BanCoreState>,
    updates_tx: mpsc::UnboundedSender<UpdaterQueueItem>,
    delayed_db_tasks_tx: mpsc::UnboundedSender<DelayedDbTask>,
    _updater: JoinTask<()>, // outlives mempool session(s) as a moderator part
}

impl BanCore {
    pub fn new(
        config: BanConfig,
        record_key_factory: RecordKeyFactory,
        delayed_db_tasks_tx: mpsc::UnboundedSender<DelayedDbTask>,
    ) -> Self {
        let (updates_tx, updates_rx) = mpsc::unbounded_channel();
        Self(Arc::new_cyclic(|weak| BanCoreInner {
            config,
            state: Mutex::new(BanCoreState::new(record_key_factory, updates_tx.clone())),
            updates_tx,
            delayed_db_tasks_tx,
            _updater: JoinTask::new({
                let weak = weak.clone();
                UnbanScheduler::default().run(weak, updates_rx)
            }),
        }))
    }

    pub fn set_peer_schedule(&self, peer_schedule: &PeerSchedule) {
        let mut state = self.0.state.lock().unwrap();
        state.set_peer_schedule(peer_schedule);
    }

    pub fn restore(
        &self,
        now: UnixTime,
        shorts: impl Iterator<Item = Result<(RecordKey, RecordValueShort)>>,
    ) -> Result<()> {
        let mut state = self.0.state.lock().unwrap();
        let items = state.restore(now, shorts, &self.0.config)?;
        (self.0.delayed_db_tasks_tx)
            .send(DelayedDbTask::Items {
                items,
                user_callback: None,
            })
            .context("channel new items on restore")?;
        Ok(())
    }

    pub fn send_report(&self, event: JournalEvent) -> Result<()> {
        (self.0.updates_tx)
            .send(UpdaterQueueItem::Event(event))
            .context("channel new mempool event")
    }
}

#[derive(Default)]
struct UnbanScheduler {
    last_bans: FastHashMap<PeerId, (delay_queue::Key, CurrentBan)>,
    auto_unbans: DelayQueue<PeerId>,
}

impl UnbanScheduler {
    async fn run(
        mut self,
        weak: Weak<BanCoreInner>,
        mut updates_rx: mpsc::UnboundedReceiver<UpdaterQueueItem>,
    ) {
        scopeguard::defer!(tracing::warn!("Mempool ban core updater shut down"));
        loop {
            tokio::select! {
                Some(queue_item) = updates_rx.recv() => {
                    let Some(inner) = weak.upgrade() else {
                        return;
                    };
                    match queue_item {
                        UpdaterQueueItem::Event(event) => {
                            self.on_event(&inner, event);
                        },
                        UpdaterQueueItem::Banned{peer_id, q_ban} => {
                            self.on_banned(&inner, &peer_id, q_ban);
                        },
                    }
                },
                Some(expired) = self.auto_unbans.next() => {
                    let Some(inner) = weak.upgrade() else {
                        return;
                    };
                    self.on_auto_unban(&inner, expired);
                },
                else => panic!("unexpected branch in BanCore update task"),
            }
        }
    }

    #[allow(clippy::unused_self, reason = "style + may use later")]
    fn on_event(&self, inner: &BanCoreInner, event: JournalEvent) {
        let mut state = inner.state.lock().unwrap();

        let mut event_item = None;
        let mut maybe_ban = None;

        if event.action().store() {
            let key = state.kf.new_key();

            maybe_ban = (event.action().is_ban_related())
                .then(|| state.maybe_ban(event.peer_id(), &key, event.tag(), &inner.config))
                .flatten()
                .map(|q_ban| JournalItemFull {
                    key: q_ban.key,
                    item: JournalItem::Banned(BanItem {
                        peer_id: *event.peer_id(),
                        until: q_ban.until,
                        origin: BanOrigin::Parent {
                            key,
                            tag: event.tag(),
                        },
                    }),
                });

            let item = JournalItem::Event(event);
            event_item = Some(JournalItemFull { key, item });
        }

        let items = event_item.into_iter().chain(maybe_ban).collect::<Vec<_>>();

        if !items.is_empty() {
            (inner.delayed_db_tasks_tx)
                .send(DelayedDbTask::Items {
                    items,
                    user_callback: None,
                })
                .expect("channel delayed DB write of event");
        }
    }

    fn on_banned(&mut self, inner: &BanCoreInner, peer_id: &PeerId, q_ban: CurrentBan) {
        let _state = inner.state.lock().unwrap(); // state race guard
        let unban_wait = Duration::from_millis((q_ban.until - UnixTime::now()).millis());
        match self.last_bans.entry(*peer_id) {
            hash_map::Entry::Occupied(mut exists) => {
                let (dq_key, old) = *exists.get();
                assert!(
                    q_ban.until > old.until,
                    "only greater bans be enqueued: new {q_ban:?} <= old {old:?}",
                );
                self.auto_unbans.reset(&dq_key, unban_wait);
                exists.insert((dq_key, q_ban));
            }
            hash_map::Entry::Vacant(empty) => {
                let dq_key = self.auto_unbans.insert(*peer_id, unban_wait);
                empty.insert((dq_key, q_ban));
            }
        };
    }

    fn on_auto_unban(&mut self, inner: &BanCoreInner, expired: Expired<PeerId>) {
        let (dq_key, q_ban) = (self.last_bans)
            .remove(expired.get_ref())
            .expect("peer unban was scheduled without last_bans entry");
        assert_eq!(
            expired.key(),
            dq_key,
            "last_bans entry has wrong delay queue key"
        );
        let peer_id = expired.into_inner();

        let mut state = inner.state.lock().unwrap();

        if !state.unban(&peer_id, q_ban.until) {
            // there is a new auto-unban waiting in channel, skip this
            return;
        }

        let item_full = JournalItemFull {
            key: state.kf.new_key(),
            item: JournalItem::Unbanned(UnbanItem {
                peer_id,
                origin: UnbanOrigin::Parent(q_ban.key),
            }),
        };
        (inner.delayed_db_tasks_tx)
            .send(DelayedDbTask::Items {
                items: vec![item_full],
                user_callback: None,
            })
            .expect("channel delayed db write on auto unban");
    }
}
