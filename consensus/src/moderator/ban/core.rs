use std::collections::hash_map;
use std::ops::Range;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use futures_util::StreamExt;
use tokio::sync::{mpsc, oneshot};
use tokio_util::time::delay_queue::Expired;
use tokio_util::time::{DelayQueue, delay_queue};
use tycho_network::PeerId;
use tycho_util::FastHashMap;
use tycho_util::futures::JoinTask;

use crate::effects::{Cancelled, TaskResult};
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
                let task = UnbanScheduler::default().run(weak, updates_rx);
                async move { task.await.expect("moderator unban scheduler") }
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

    pub fn manual_ban(
        &self,
        peer_id: &PeerId,
        until: UnixTime,
        callback: oneshot::Sender<Result<()>>,
    ) -> Result<()> {
        let mut state = self.0.state.lock().unwrap();
        let key = state.kf.new_key();
        if !state.manual_ban(peer_id, CurrentBan { until, key }) {
            anyhow::bail!("already banned for a longer period")
        }

        let item = JournalItem::Banned(BanItem {
            until,
            peer_id: *peer_id,
            origin: BanOrigin::Manual,
        });
        (self.0.delayed_db_tasks_tx)
            .send(DelayedDbTask::Items {
                items: vec![JournalItemFull { key, item }],
                user_callback: Some(callback),
            })
            .expect("send to db writer at the end");
        Ok(())
    }

    pub fn manual_unban(
        &self,
        peer_id: &PeerId,
        callback: oneshot::Sender<Result<()>>,
    ) -> Result<()> {
        (self.0.updates_tx)
            .send(UpdaterQueueItem::ManualUnban {
                peer_id: *peer_id,
                callback,
            })
            .context("unban task stopped")
    }

    pub fn delete(
        &self,
        range: Range<UnixTime>,
        user_callback: oneshot::Sender<Result<()>>,
    ) -> Result<()> {
        (self.0.delayed_db_tasks_tx)
            .send(DelayedDbTask::Delete {
                range,
                user_callback,
            })
            .context("delete events task stopped")
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
    ) -> TaskResult<()> {
        scopeguard::defer!(tracing::warn!("Mempool ban core updater shut down"));
        loop {
            tokio::select! {
                Some(queue_item) = updates_rx.recv() => {
                    let Some(inner) = weak.upgrade() else {
                        return Ok(());
                    };
                    match queue_item {
                        UpdaterQueueItem::Event(event) => {
                            self.on_event(&inner, event)?;
                        },
                        UpdaterQueueItem::Banned{peer_id, q_ban} => {
                            self.on_banned(&inner, &peer_id, q_ban);
                        },
                        UpdaterQueueItem::ManualUnban{peer_id, callback} => {
                            self.on_manual_unban(&inner, &peer_id, callback)?;
                        }
                    }
                },
                Some(expired) = self.auto_unbans.next() => {
                    let Some(inner) = weak.upgrade() else {
                        return Ok(());
                    };
                    self.on_auto_unban(&inner, expired)?;
                },
                else => panic!("unexpected branch in BanCore update task"),
            }
        }
    }

    #[allow(clippy::unused_self, reason = "style + may use later")]
    fn on_event(&self, inner: &BanCoreInner, event: JournalEvent) -> TaskResult<()> {
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
            let items = DelayedDbTask::Items {
                items,
                user_callback: None,
            };
            if (inner.delayed_db_tasks_tx).send(items).is_err() {
                tracing::error!("failed to channel to delayed db writer");
                return Err(Cancelled());
            }
        }
        Ok(())
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

    fn on_manual_unban(
        &mut self,
        inner: &BanCoreInner,
        peer_id: &PeerId,
        user_callback: oneshot::Sender<Result<()>>,
    ) -> TaskResult<()> {
        let mut state = inner.state.lock().unwrap();

        let Some((dq_key, q_ban)) = self.last_bans.remove(peer_id) else {
            user_callback.send(Err(anyhow!("peer is not banned"))).ok();
            return Ok(());
        };

        self.auto_unbans.remove(&dq_key); // panics if the key is not present

        if !state.unban(peer_id, q_ban.until) {
            user_callback.send(Err(anyhow!("no ban to remove"))).ok();
            return Ok(());
        }

        let item_full = JournalItemFull {
            key: state.kf.new_key(),
            item: JournalItem::Unbanned(UnbanItem {
                peer_id: *peer_id,
                origin: UnbanOrigin::Manual,
            }),
        };

        let items = DelayedDbTask::Items {
            items: vec![item_full],
            user_callback: Some(user_callback),
        };

        match inner.delayed_db_tasks_tx.send(items) {
            Ok(()) => Ok(()),
            Err(mpsc::error::SendError(DelayedDbTask::Items {
                user_callback: Some(user_callback),
                ..
            })) => {
                (user_callback.send(Err(anyhow!("db writer task is dead")))).ok();
                tracing::error!("failed to channel to delayed db writer");
                Err(Cancelled())
            }
            Err(mpsc::error::SendError(_)) => unreachable!(),
        }
    }

    fn on_auto_unban(&mut self, inner: &BanCoreInner, expired: Expired<PeerId>) -> TaskResult<()> {
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
            return Ok(());
        }

        let item_full = JournalItemFull {
            key: state.kf.new_key(),
            item: JournalItem::Unbanned(UnbanItem {
                peer_id,
                origin: UnbanOrigin::Parent(q_ban.key),
            }),
        };
        let items = DelayedDbTask::Items {
            items: vec![item_full],
            user_callback: None,
        };
        if (inner.delayed_db_tasks_tx).send(items).is_err() {
            tracing::error!("failed to channel to delayed db writer");
            return Err(Cancelled());
        }
        Ok(())
    }
}
