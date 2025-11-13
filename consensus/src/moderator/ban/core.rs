use std::collections::hash_map;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use anyhow::{Context, anyhow};
use futures_util::StreamExt;
use tokio::sync::{mpsc, oneshot};
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
use crate::moderator::{JournalEvent, JournalTtl, ModeratorConfig, RecordKey, RecordValueShort};

pub struct BanCore(Arc<BanCoreInner>);

struct BanCoreInner {
    config: ModeratorConfig,
    state: Mutex<BanCoreState>,
    updates_tx: mpsc::UnboundedSender<UpdaterQueueItem>,
    delayed_db_writes_tx: mpsc::UnboundedSender<Vec<JournalItemFull>>,
    _updater: JoinTask<()>, // outlives mempool session(s) as a moderator part
}

impl BanCore {
    pub fn new(
        config: ModeratorConfig,
        record_key_factory: RecordKeyFactory,
        delayed_db_writes_tx: mpsc::UnboundedSender<Vec<JournalItemFull>>,
    ) -> Self {
        let (updates_tx, updates_rx) = mpsc::unbounded_channel();
        Self(Arc::new_cyclic(|weak| BanCoreInner {
            config,
            state: Mutex::new(BanCoreState::new(record_key_factory, updates_tx.clone())),
            updates_tx,
            delayed_db_writes_tx,
            _updater: JoinTask::new(run_updater(weak.clone(), updates_rx)),
        }))
    }

    pub fn event_journal_ttl(&self) -> JournalTtl {
        self.0.config.event_journal_ttl
    }

    pub fn set_peer_schedule(&self, peer_schedule: &PeerSchedule) {
        let mut state = self.0.state.lock().unwrap();
        state.set_peer_schedule(peer_schedule);
    }

    pub fn restore(&self, now: UnixTime, shorts: &[(RecordKey, RecordValueShort)]) {
        let mut state = self.0.state.lock().unwrap();
        let new_items = state.restore(now, shorts, &self.0.config.bans);
        self.0.delayed_db_writes_tx.send(new_items).ok();
    }

    pub fn send_report(&self, event: JournalEvent) {
        self.0.updates_tx.send(UpdaterQueueItem::Event(event)).ok();
    }

    pub fn manual_ban(&self, peer_id: &PeerId, until: UnixTime) -> anyhow::Result<()> {
        let mut state = self.0.state.lock().unwrap();
        let key = state.kf.new_key();
        state
            .manual_ban(peer_id, CurrentBan { until, key })
            .map_err(|()| anyhow!("already banned for a longer period"))?;

        let item = JournalItem::Banned(BanItem {
            until,
            peer_id: *peer_id,
            origin: BanOrigin::Manual,
        });
        self.0
            .delayed_db_writes_tx
            .send(vec![JournalItemFull { key, item }])
            .context("send to db writer at the end")?;
        Ok(())
    }

    pub fn manual_unban(
        &self,
        peer_id: &PeerId,
        tx: oneshot::Sender<anyhow::Result<()>>,
    ) -> anyhow::Result<()> {
        self.0
            .updates_tx
            .send(UpdaterQueueItem::ManualUnban {
                peer_id: *peer_id,
                callback: tx,
            })
            .context("unban task stopped")
    }
}

async fn run_updater(
    weak: Weak<BanCoreInner>,
    mut updates_rx: mpsc::UnboundedReceiver<UpdaterQueueItem>,
) {
    scopeguard::defer!(tracing::warn!("Mempool ban core updater shut down"));
    let mut last_bans = FastHashMap::<PeerId, (delay_queue::Key, CurrentBan)>::default();
    let mut auto_unbans = DelayQueue::<PeerId>::new();
    loop {
        tokio::select! {
            Some(queue_item) = updates_rx.recv() => match queue_item {
                UpdaterQueueItem::Event(event) => {
                    let Some(inner) = weak.upgrade() else {
                        break;
                    };
                    inner.on_event(event);
                }
                UpdaterQueueItem::AutoUnban{peer_id, q_ban} => {
                    let unban_wait = Duration::from_millis((q_ban.until - UnixTime::now()).millis());
                    match last_bans.entry(peer_id) {
                        hash_map::Entry::Occupied(mut occupied) => {
                            let (dq_key, old) = *occupied.get();
                            assert!(
                                q_ban.until > old.until,
                                "only greater bans be enqueued: new {q_ban:?} <= old {old:?}",
                            );
                            auto_unbans.reset(&dq_key, unban_wait);
                            occupied.insert((dq_key, q_ban));
                        }
                        hash_map::Entry::Vacant(vacant) => {
                            let dq_key = auto_unbans.insert(peer_id, unban_wait);
                            vacant.insert((dq_key, q_ban));
                        }
                    };
                }
                UpdaterQueueItem::ManualUnban{peer_id, callback} => {
                    match last_bans.remove(&peer_id) {
                        Some((dq_key, _q_ban)) => {
                            auto_unbans.remove(&dq_key); // panics if the key is not present
                            let Some(inner) = weak.upgrade() else {
                                break;
                            };
                            inner.on_manual_unban(&peer_id, callback);
                        }
                        None => {
                            callback.send(Err(anyhow!("peer is not banned"))).ok();
                        }
                    };
                }
            },
            Some(expired) = auto_unbans.next() => {
                let (dq_key , q_ban) = last_bans
                    .remove(expired.get_ref())
                    .expect("peer unban was scheduled without last_bans entry");
                assert_eq!(expired.key(), dq_key, "last_bans entry has wrong delay queue key");
                let peer_id = expired.into_inner();

                let Some(inner) = weak.upgrade() else {
                    break;
                };
                inner.on_auto_unban(&peer_id, q_ban);
            },
            else => break,
        }
    }
}

impl BanCoreInner {
    fn on_event(&self, event: JournalEvent) {
        let mut state = self.state.lock().unwrap();

        let mut event_item = None;
        let mut maybe_ban = None;

        if event.action().store() {
            let key = state.kf.new_key();

            maybe_ban = (event.action().is_ban_related())
                .then(|| state.maybe_ban(event.peer_id(), &key, event.tag(), &self.config.bans))
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
            self.delayed_db_writes_tx.send(items).ok();
        }
    }

    fn on_manual_unban(&self, peer_id: &PeerId, callback: oneshot::Sender<anyhow::Result<()>>) {
        let mut state = self.state.lock().unwrap();

        if state.manual_unban(peer_id).is_err() {
            callback.send(Err(anyhow!("no ban to remove"))).ok();
            return;
        }

        let item_full = JournalItemFull {
            key: state.kf.new_key(),
            item: JournalItem::Unbanned(UnbanItem {
                peer_id: *peer_id,
                origin: UnbanOrigin::Manual,
            }),
        };

        let result = self
            .delayed_db_writes_tx
            .send(vec![item_full])
            .map_err(|_e| anyhow!("ban server db writer is shut down"));
        callback.send(result).ok();
    }

    fn on_auto_unban(&self, peer_id: &PeerId, q_ban: CurrentBan) {
        let mut state = self.state.lock().unwrap();

        if state.auto_unban(peer_id, q_ban.until).is_err() {
            // there is a new auto-unban waiting in channel, skip this
            return;
        }

        let item_full = JournalItemFull {
            key: state.kf.new_key(),
            item: JournalItem::Unbanned(UnbanItem {
                peer_id: *peer_id,
                origin: UnbanOrigin::Parent(q_ban.key),
            }),
        };
        self.delayed_db_writes_tx.send(vec![item_full]).ok();
    }
}
