use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::sync::oneshot;
use tycho_util::FastDashMap;

pub struct SlotSubscriptions<K, V> {
    subscriptions: FastDashMap<K, Subscriptions<V>>,
}

impl<K: Eq + Hash, V> Default for SlotSubscriptions<K, V> {
    fn default() -> Self {
        Self {
            subscriptions: FastDashMap::default(),
        }
    }
}

impl<K: Eq + Hash + Clone, V> SlotSubscriptions<K, V> {
    pub fn subscribe<'a>(&'a self, key: &'a K) -> SlotSubscription<'a, K, V> {
        use dashmap::mapref::entry::Entry;

        let (tx, rx) = oneshot::channel();

        let index = match self.subscriptions.entry(key.clone()) {
            Entry::Occupied(mut entry) => entry.get_mut().insert(tx),
            Entry::Vacant(entry) => {
                entry.insert(Subscriptions::new(tx));
                0
            }
        };

        SlotSubscription {
            subscriptions: &self.subscriptions,
            block_id: key,
            index: Some(index as u32),
            rx,
        }
    }
}

impl<K: Eq + Hash, V: Clone> SlotSubscriptions<K, V> {
    pub fn notify(&self, key: &K, block: &V) {
        if let Some((_, subscriptions)) = self.subscriptions.remove(key) {
            subscriptions.notify(block);
        }
    }
}

struct Subscriptions<T> {
    active: usize,
    slots: Vec<Option<oneshot::Sender<T>>>,
}

impl<T> Subscriptions<T> {
    fn new(tx: oneshot::Sender<T>) -> Self {
        Self {
            active: 1,
            slots: vec![Some(tx)],
        }
    }

    fn insert(&mut self, tx: oneshot::Sender<T>) -> usize {
        self.active += 1;

        // Reuse existing slot
        for (i, item) in self.slots.iter_mut().enumerate() {
            if item.is_some() {
                continue;
            }
            *item = Some(tx);
            return i;
        }

        // Add new slot
        let idx = self.slots.len();
        self.slots.push(Some(tx));
        idx
    }

    fn remove(&mut self, index: usize) {
        // NOTE: Slot count never decreases
        self.active -= 1;
        self.slots[index] = None;
    }
}

impl<T: Clone> Subscriptions<T> {
    fn notify(self, block: &T) {
        for tx in self.slots.into_iter().flatten() {
            tx.send(block.clone()).ok();
        }
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct SlotSubscription<'a, K: Eq + Hash, V> {
    subscriptions: &'a FastDashMap<K, Subscriptions<V>>,
    block_id: &'a K,
    index: Option<u32>,
    rx: oneshot::Receiver<V>,
}

impl<'a, K: Eq + Hash, V> Future for SlotSubscription<'a, K, V> {
    type Output = V;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(self.index.is_some(), "called after complete");

        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(block)) => {
                self.index = None;
                Poll::Ready(block)
            }
            Poll::Ready(Err(_)) => panic!("SlotSubscription dropped"),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<'a, K: Eq + Hash, V> Drop for SlotSubscription<'a, K, V> {
    fn drop(&mut self) {
        let Some(index) = self.index else {
            return;
        };

        self.subscriptions.remove_if_mut(self.block_id, |_, slots| {
            slots.remove(index as usize);
            slots.active == 0
        });
    }
}
