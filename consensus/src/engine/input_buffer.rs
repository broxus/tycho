use std::collections::VecDeque;
use std::mem;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use rand::{thread_rng, RngCore};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;

use crate::engine::MempoolConfig;

#[async_trait]
pub trait InputBuffer {
    /// `only_fresh = false` to repeat the same elements if they are still buffered,
    /// use in case last round failed
    async fn fetch(&mut self, only_fresh: bool) -> Vec<Bytes>;
}

pub struct InputBufferImpl {
    abort: Arc<Notify>,
    consumer: Option<JoinHandle<InputBufferImplInner>>,
}

struct InputBufferImplInner {
    abort: Arc<Notify>,
    externals: mpsc::UnboundedReceiver<Bytes>,
    data: VecDeque<Bytes>,
    data_bytes: usize,
    offset_elements: usize,
}

#[async_trait]
impl InputBuffer for InputBufferImpl {
    async fn fetch(&mut self, only_fresh: bool) -> Vec<Bytes> {
        self.abort.notify_one();
        let handle = mem::take(&mut self.consumer).expect("consumer must be set");
        let mut inner = handle.await.expect("consumer failed");
        if only_fresh {
            inner.commit_offset();
        }
        let result = inner.fetch();
        self.consumer = Some(tokio::spawn(inner.consume()));
        result
    }
}

impl InputBufferImpl {
    pub fn new(externals: mpsc::UnboundedReceiver<Bytes>) -> Self {
        let abort = Arc::new(Notify::new());
        let inner = InputBufferImplInner {
            externals,
            abort: abort.clone(),
            data: VecDeque::new(),
            data_bytes: 0,
            offset_elements: 0,
        };
        Self {
            abort,
            consumer: Some(tokio::spawn(inner.consume())),
        }
    }
}

impl InputBufferImplInner {
    async fn consume(mut self) -> Self {
        loop {
            tokio::select! {
                () = self.abort.notified() => break self,
                recieved = self.externals.recv() => match recieved {
                    Some(payload) => self.add(payload),
                    None => panic!("externals input channel to mempool is closed")
                },
            }
        }
    }
    fn fetch(&mut self) -> Vec<Bytes> {
        let mut taken_bytes = 0;
        let result = self
            .data
            .iter()
            .take_while(|elem| {
                taken_bytes += elem.len();
                taken_bytes <= MempoolConfig::PAYLOAD_BATCH_BYTES
            })
            .cloned()
            .collect::<Vec<_>>();
        self.offset_elements = result.len(); // overwrite
        result
    }
    fn commit_offset(&mut self) {
        let initial_capacity = self.data.capacity();
        let committed_bytes: usize = self
            .data
            .drain(..self.offset_elements)
            .map(|comitted_bytes| comitted_bytes.len())
            .sum();
        self.update_capacity(initial_capacity);
        self.data_bytes = self
            .data_bytes
            .checked_sub(committed_bytes)
            .expect("decrease buffered data size on commit offset");
        self.offset_elements = 0;
    }
    fn add(&mut self, payload: Bytes) {
        let payload_bytes = payload.len();
        assert!(
            payload_bytes <= MempoolConfig::PAYLOAD_BUFFER_BYTES,
            "cannot buffer too large message of {payload_bytes} bytes: \
            increase config value of PAYLOAD_BUFFER_BYTES={} \
            or filter out insanely large messages prior sending them to mempool",
            MempoolConfig::PAYLOAD_BUFFER_BYTES
        );
        let max_used_bytes = MempoolConfig::PAYLOAD_BUFFER_BYTES - payload_bytes;
        if self.data_bytes > max_used_bytes {
            let initial_capacity = self.data.capacity();
            let to_drop = self
                .data
                .iter()
                .take_while(|evicted| {
                    self.data_bytes = self
                        .data_bytes
                        .checked_sub(evicted.len())
                        .expect("decrease buffered data size on eviction");
                    self.data_bytes > max_used_bytes
                })
                .count();
            self.offset_elements = self.offset_elements.saturating_sub(to_drop);
            _ = self.data.drain(..to_drop);
            self.update_capacity(initial_capacity);
        }
        self.data_bytes += payload_bytes;
        self.data.push_back(payload);
    }
    fn update_capacity(&mut self, initial_capacity: usize) {
        // because reallocation on adding elements doubles the capacity
        if self.data.capacity() < initial_capacity / 4 {
            self.data.shrink_to(initial_capacity / 2);
        }
    }
}

pub struct InputBufferStub {
    fetch_count: usize,
    steps_until_full: usize,
    fetches_in_step: usize,
}

impl InputBufferStub {
    /// external message is limited by 64 KiB
    const EXTERNAL_MSG_MAX_BYTES: usize = 64 * 1024;
    pub fn new(fetches_in_step: usize, steps_until_full: usize) -> Self {
        Self {
            fetch_count: 0,
            steps_until_full,
            fetches_in_step,
        }
    }
}

#[async_trait]
impl InputBuffer for InputBufferStub {
    async fn fetch(&mut self, _: bool) -> Vec<Bytes> {
        self.fetch_count += 1;
        let step = (self.fetch_count / self.fetches_in_step).min(self.steps_until_full);
        let msg_count = (MempoolConfig::PAYLOAD_BATCH_BYTES * step)
            / self.steps_until_full
            / Self::EXTERNAL_MSG_MAX_BYTES;
        let mut result = Vec::with_capacity(msg_count);
        for _ in 0..msg_count {
            let mut data = vec![0; Self::EXTERNAL_MSG_MAX_BYTES];
            thread_rng().fill_bytes(data.as_mut_slice());
            result.push(Bytes::from(data));
        }
        result
    }
}
