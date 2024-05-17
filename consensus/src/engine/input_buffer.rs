use std::collections::VecDeque;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use rand::{thread_rng, RngCore};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;

use crate::engine::MempoolConfig;

#[async_trait]
pub trait InputBuffer: Send + 'static {
    /// `only_fresh = false` to repeat the same elements if they are still buffered,
    /// use in case last round failed
    async fn fetch(&mut self, only_fresh: bool) -> Vec<Bytes>;
}

pub struct InputBufferImpl {
    abort: Arc<Notify>,
    consumer: Option<JoinHandle<InputBufferImplInner>>,
}

impl InputBufferImpl {
    pub fn new(externals: mpsc::UnboundedReceiver<Bytes>) -> Self {
        let abort = Arc::new(Notify::new());
        let inner = InputBufferImplInner {
            externals,
            data: Default::default(),
        };

        Self {
            consumer: Some(tokio::spawn(inner.consume(abort.clone()))),
            abort,
        }
    }
}

impl Drop for InputBufferImpl {
    fn drop(&mut self) {
        if let Some(handle) = self.consumer.take() {
            handle.abort();
        }
    }
}

#[async_trait]
impl InputBuffer for InputBufferImpl {
    async fn fetch(&mut self, only_fresh: bool) -> Vec<Bytes> {
        self.abort.notify_waiters();
        let handle = self.consumer.take().expect("consumer must be set");
        let mut inner = handle.await.expect("consumer failed");

        if only_fresh {
            inner.data.commit_offset();
        }
        let result = inner.data.fetch();

        self.consumer = Some(tokio::spawn(inner.consume(self.abort.clone())));
        result
    }
}

struct InputBufferImplInner {
    externals: mpsc::UnboundedReceiver<Bytes>,
    data: InputBufferData,
}

impl InputBufferImplInner {
    async fn consume(mut self, abort: Arc<Notify>) -> Self {
        let mut notified = std::pin::pin!(abort.notified());
        loop {
            tokio::select! {
                _ = &mut notified => break self,
                payload = self.externals.recv() => {
                    self.data.add(payload.expect("externals input channel to mempool is closed"));
                },
            }
        }
    }
}

#[derive(Default)]
struct InputBufferData {
    data: VecDeque<Bytes>,
    data_bytes: usize,
    offset_elements: usize,
}

impl InputBufferData {
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

    fn add(&mut self, payload: Bytes) {
        let payload_bytes = payload.len();
        assert!(
            payload_bytes <= MempoolConfig::PAYLOAD_BUFFER_BYTES,
            "cannot buffer too large message of {payload_bytes} bytes: \
            increase config value of PAYLOAD_BUFFER_BYTES={} \
            or filter out insanely large messages prior sending them to mempool",
            MempoolConfig::PAYLOAD_BUFFER_BYTES
        );

        let max_data_bytes = MempoolConfig::PAYLOAD_BUFFER_BYTES - payload_bytes;
        if self.data_bytes > max_data_bytes {
            let to_drop = self
                .data
                .iter()
                .take_while(|evicted| {
                    self.data_bytes = self
                        .data_bytes
                        .checked_sub(evicted.len())
                        .expect("decrease buffered data size on eviction");
                    self.data_bytes > max_data_bytes
                })
                .count();

            self.offset_elements = self.offset_elements.saturating_sub(to_drop);
            _ = self.data.drain(..to_drop);
        }

        self.data_bytes += payload_bytes;
        self.data.push_back(payload);
    }

    fn commit_offset(&mut self) {
        let committed_bytes: usize = self
            .data
            .drain(..self.offset_elements)
            .map(|comitted_bytes| comitted_bytes.len())
            .sum();

        self.update_capacity();

        self.data_bytes = self
            .data_bytes
            .checked_sub(committed_bytes)
            .expect("decrease buffered data size on commit offset");

        self.offset_elements = 0;
    }

    /// Ensures that the capacity is not too large.
    fn update_capacity(&mut self) {
        let len = self.data.len();

        // because reallocation on adding elements doubles the capacity
        if self.data.capacity() >= len * 4 {
            self.data.shrink_to(len / 2);
        }
    }
}

pub struct InputBufferStub {
    fetch_count: usize,
    steps_until_full: usize,
    fetches_in_step: usize,
}

impl InputBufferStub {
    /// External message is limited by 64 KiB
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
