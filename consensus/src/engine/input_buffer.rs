use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard};
use rand::{thread_rng, RngCore};
use tokio::sync::mpsc;

use crate::engine::MempoolConfig;

trait InputBufferInner: Send {
    fn fetch_inner(&mut self, only_fresh: bool) -> Vec<Bytes>;
}

#[derive(Clone)]
pub struct InputBuffer(Arc<Mutex<dyn InputBufferInner>>);

impl InputBuffer {
    /// `only_fresh = false` to repeat the same elements if they are still buffered,
    /// use in case last round failed
    pub fn fetch(&self, only_fresh: bool) -> Vec<Bytes> {
        let mut inner = self.0.lock();
        inner.fetch_inner(only_fresh)
    }
}

pub struct InputBufferImpl;

impl InputBufferImpl {
    pub fn new(externals: mpsc::UnboundedReceiver<Bytes>) -> InputBuffer {
        let inner = Arc::new(Mutex::new(InputBufferData::default()));
        tokio::spawn(Self::consume(inner.clone(), externals));
        InputBuffer(inner)
    }
    async fn consume(
        inner: Arc<Mutex<InputBufferData>>,
        mut externals: mpsc::UnboundedReceiver<Bytes>,
    ) -> ! {
        while let Some(payload) = externals.recv().await {
            let mut data = inner.lock();
            data.add(payload);
            // `fetch()` is topmost priority
            MutexGuard::unlock_fair(data);
        }
        panic!("externals input channel to mempool is closed");
    }
}

impl InputBufferInner for InputBufferData {
    fn fetch_inner(&mut self, only_fresh: bool) -> Vec<Bytes> {
        if only_fresh {
            self.commit_offset();
        }
        self.fetch()
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
        let data_bytes_pre = self.data_bytes;
        if self.data_bytes > max_data_bytes {
            let to_drop = self
                .data
                .iter()
                .take_while(|front| {
                    // last call must not change `self`
                    let take_more = self.data_bytes > max_data_bytes;
                    if take_more {
                        self.data_bytes = self
                            .data_bytes
                            .checked_sub(front.len())
                            .expect("decrease buffered data size on eviction");
                    }
                    take_more
                })
                .count();

            self.offset_elements = self.offset_elements.saturating_sub(to_drop);
            _ = self.data.drain(..to_drop);

            metrics::counter!("tycho_mempool_evicted_externals_count").increment(to_drop as _);

            tracing::warn!(
                count = to_drop,
                size = data_bytes_pre - self.data_bytes,
                "evicted externals",
            );
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
    fetch_count: NonZeroUsize,
    steps_until_full: NonZeroUsize,
    points_in_step: NonZeroUsize,
}

impl InputBufferStub {
    /// External message is limited by 64 KiB
    const EXTERNAL_MSG_MAX_BYTES: usize = 64 * 1024;

    pub fn new(points_in_step: NonZeroUsize, steps_until_full: NonZeroUsize) -> InputBuffer {
        InputBuffer(Arc::new(Mutex::new(Self {
            fetch_count: NonZeroUsize::MIN,
            steps_until_full,
            points_in_step,
        })))
    }
}

impl InputBufferInner for InputBufferStub {
    fn fetch_inner(&mut self, _: bool) -> Vec<Bytes> {
        let step =
            (self.fetch_count.get() / self.points_in_step.get()).min(self.steps_until_full.get());
        let msg_count = (MempoolConfig::PAYLOAD_BATCH_BYTES * step)
            / self.steps_until_full
            / Self::EXTERNAL_MSG_MAX_BYTES;
        let mut result = Vec::with_capacity(msg_count);
        for _ in 0..msg_count {
            let mut data = vec![0; Self::EXTERNAL_MSG_MAX_BYTES];
            thread_rng().fill_bytes(data.as_mut_slice());
            result.push(Bytes::from(data));
        }
        self.fetch_count = self.fetch_count.saturating_add(1);
        result
    }
}
