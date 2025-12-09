use std::cell::RefCell;
use std::mem;

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tycho_rpc_subscriptions::{
    ClientId, MAX_ADDRS_PER_CLIENT, SubscribeError, SubscriberManager, SubscriberManagerConfig,
    UnsubscribeError,
};
use tycho_types::models::StdAddr;
use tycho_util::FastDashMap;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountUpdate {
    pub address: StdAddr,
    pub max_lt: u64,
    pub gen_utime: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientStatus {
    pub client_id: ClientId,
    pub subscription_count: usize,
    pub max_per_client: u8,
    pub max_clients: u32,
    pub max_addrs: u32,
}

struct ClientQueue {
    uuid: Uuid,
    sender: mpsc::Sender<AccountUpdate>,
}

pub struct RpcSubscriptions {
    manager: SubscriberManager,
    queues: FastDashMap<ClientId, ClientQueue>,
    queue_capacity: usize,
}

thread_local! {
    static CLIENT_IDS: RefCell<Vec<ClientId>> =const { RefCell::new(Vec::new())};
}

impl RpcSubscriptions {
    pub fn new(config: SubscriberManagerConfig, queue_capacity: usize) -> Self {
        let queue_capacity = queue_capacity.max(1);
        Self {
            manager: SubscriberManager::new(config),
            queues: FastDashMap::default(),
            queue_capacity,
        }
    }

    pub fn register(
        &self,
    ) -> Result<(Uuid, ClientId, mpsc::Receiver<AccountUpdate>), SubscribeError> {
        let (uuid, client_id) = loop {
            let uuid = Uuid::new_v4();
            match self.manager.register(uuid) {
                Ok(a) => break (uuid, a),
                Err(SubscribeError::Collision { .. }) => {
                    tracing::warn!("YAY, uuid collision, retrying");
                }
                Err(err) => return Err(RegisterError::from(err)),
            };
        };
        tracing::debug!(?client_id, "Registered new client");
        let (tx, rx) = mpsc::channel(self.queue_capacity);
        self.queues
            .insert(client_id, ClientQueue { uuid, sender: tx });
        Ok((uuid, client_id, rx))
    }

    pub fn unregister(&self, uuid: Uuid) {
        if let Some(client_id) = self.manager.client_id(uuid)
            && let Some((_, removed)) = self.queues.remove(&client_id)
        {
            self.manager.unregister(removed.uuid);
            return;
        }
        self.manager.unregister(uuid);
    }

    pub fn subscribe<I>(&self, uuid: Uuid, addrs: I) -> Result<(), SubscribeError>
    where
        I: IntoIterator<Item = StdAddr>,
    {
        self.manager.subscribe_many(uuid, addrs)
    }

    pub fn unsubscribe<I>(&self, uuid: Uuid, addrs: I) -> Result<(), UnsubscribeError>
    where
        I: IntoIterator<Item = StdAddr>,
    {
        self.manager.unsubscribe_many(uuid, addrs)
    }

    pub fn unsubscribe_all(&self, uuid: Uuid) -> Result<(), UnsubscribeError> {
        self.manager.unsubscribe_all(uuid)
    }

    pub fn status(&self, uuid: Uuid) -> Result<ClientStatus, UnsubscribeError> {
        let stats = self
            .manager
            .client_stats(uuid)
            .ok_or(UnsubscribeError::UnknownClient)?;
        let config = self.manager.config();

        Ok(ClientStatus {
            client_id: stats.client_id,
            subscription_count: stats.subscription_count,
            max_per_client: MAX_ADDRS_PER_CLIENT,
            max_clients: config.max_clients,
            max_addrs: config.max_addrs,
        })
    }

    pub fn list_subscriptions(&self, uuid: Uuid) -> Result<Vec<StdAddr>, UnsubscribeError> {
        self.manager.list_subscriptions(uuid)
    }

    pub fn client_id(&self, uuid: Uuid) -> Option<ClientId> {
        self.manager.client_id(uuid)
    }

    pub async fn fanout_updates<I>(&self, updates: I)
    where
        I: IntoIterator<Item = AccountUpdate>,
    {
        let mut client_ids = CLIENT_IDS.with(|cell| mem::take(&mut *cell.borrow_mut()));

        for update in updates {
            self.manager
                .clients_to_notify(update.address.clone(), &mut client_ids);

            if client_ids.is_empty() {
                continue;
            }

            tracing::debug!(len = client_ids.len(), "Fanout updates to clients");

            for client_id in &client_ids {
                let Some(queue) = self.queues.get(client_id) else {
                    continue;
                };

                // WARN: should be pretty fast, so I assume that holding the lock for a short time is fine
                let send_res = queue.sender.try_send(update.clone());
                drop(queue);

                if let Err(err) = send_res {
                    match err {
                        TrySendError::Full(_) => {
                            tracing::debug!(?client_id, "Queue full, dropping update");
                        }
                        TrySendError::Closed(_) => {
                            if let Some((_, removed)) = self.queues.remove(client_id) {
                                self.manager.unregister(removed.uuid);
                            }
                        }
                    }
                }
            }

            tokio::task::consume_budget().await;
        }

        CLIENT_IDS.with(|cell| {
            *cell.borrow_mut() = client_ids;
        });
    }
}
