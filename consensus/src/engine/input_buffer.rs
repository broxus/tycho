use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use everscale_types::models::ConsensusConfig;
use parking_lot::{Mutex, MutexGuard};

trait InputBufferInner: Send {
    fn push(&mut self, ext_in_msg: Bytes);
    fn fetch_inner(&mut self, only_fresh: bool) -> Vec<Bytes>;
    fn apply_config(&mut self, config: &ConsensusConfig);
}

#[derive(Clone)]
pub struct InputBuffer(Arc<Mutex<dyn InputBufferInner>>);

impl Default for InputBuffer {
    fn default() -> Self {
        InputBuffer(Arc::new(Mutex::new(InputBufferData::default())))
    }
}

impl InputBuffer {
    pub fn push(&self, ext_in_msg: Bytes) {
        let mut data = self.0.lock();
        data.push(ext_in_msg);
        // `fetch()` is topmost priority
        MutexGuard::unlock_fair(data);
    }

    /// `only_fresh = false` to repeat the same elements if they are still buffered,
    /// use in case last round failed
    pub fn fetch(&self, only_fresh: bool) -> Vec<Bytes> {
        let mut inner = self.0.lock();
        inner.fetch_inner(only_fresh)
    }

    pub fn apply_config(&self, consensus_config: &ConsensusConfig) {
        let mut inner = self.0.lock();
        inner.apply_config(consensus_config);
    }
}

impl InputBufferInner for InputBufferData {
    fn push(&mut self, ext_in_msg: Bytes) {
        if self.payload_buffer_bytes == 0 || self.payload_batch_bytes == 0 {
            // TODO log debounce https://github.com/broxus/tycho/issues/406
            tracing::trace!("cannot enqueue msg until config is init");
            metrics::counter!("tycho_mempool_evicted_externals_count").increment(1);
            metrics::counter!("tycho_mempool_evicted_externals_size")
                .increment(ext_in_msg.len() as _);
            return; // ignore until config applied
        }
        self.add(ext_in_msg);
    }

    fn fetch_inner(&mut self, only_fresh: bool) -> Vec<Bytes> {
        if only_fresh {
            self.commit_offset();
        }
        self.fetch()
    }

    fn apply_config(&mut self, consensus_config: &ConsensusConfig) {
        self.payload_buffer_bytes = consensus_config.payload_buffer_bytes as usize;
        self.payload_batch_bytes = consensus_config.payload_batch_bytes as usize;
        tracing::info!(
            payload_batch_bytes = consensus_config.payload_batch_bytes,
            payload_buffer_bytes = consensus_config.payload_buffer_bytes,
            "input buffer config applied"
        );
    }
}

#[derive(Default)]
struct InputBufferData {
    data: VecDeque<Bytes>,
    data_bytes: usize,
    offset_elements: usize,
    payload_buffer_bytes: usize,
    payload_batch_bytes: usize,
}

impl InputBufferData {
    fn fetch(&mut self) -> Vec<Bytes> {
        let mut taken_bytes = 0;
        let result = self
            .data
            .iter()
            .take_while(|elem| {
                taken_bytes += elem.len();
                taken_bytes <= self.payload_batch_bytes
            })
            .cloned()
            .collect::<Vec<_>>();
        self.offset_elements = result.len(); // overwrite
        result
    }

    fn add(&mut self, payload: Bytes) {
        let payload_bytes = payload.len();
        assert!(
            payload_bytes <= self.payload_buffer_bytes,
            "cannot buffer too large message of {payload_bytes} bytes: \
            increase config value of PAYLOAD_BUFFER_BYTES={} \
            or filter out insanely large messages prior sending them to mempool",
            self.payload_buffer_bytes
        );

        let max_data_bytes = self.payload_buffer_bytes - payload_bytes;
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
            metrics::counter!("tycho_mempool_evicted_externals_size")
                .increment((data_bytes_pre - self.data_bytes) as _);

            tracing::trace!(
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
        if self.data.capacity() >= len.saturating_mul(4) {
            self.data.shrink_to(len.saturating_mul(2));
        }
    }
}

#[cfg(feature = "test")]
mod stub {
    use std::num::NonZeroUsize;

    use rand::{thread_rng, RngCore};

    use super::*;
    use crate::engine::CachedConfig;

    /// External message is limited by 64 KiB
    const EXTERNAL_MSG_MAX_BYTES: usize = 64 * 1024;
    struct InputBufferStub {
        fetch_count: NonZeroUsize,
        payload_step: usize,
        steps_until_full: NonZeroUsize,
    }

    impl InputBuffer {
        pub fn new_stub(payload_step: usize, steps_until_full: NonZeroUsize) -> InputBuffer {
            InputBuffer(Arc::new(Mutex::new(InputBufferStub {
                fetch_count: NonZeroUsize::MIN,
                steps_until_full,
                payload_step,
            })))
        }
    }

    impl InputBufferInner for InputBufferStub {
        fn fetch_inner(&mut self, _: bool) -> Vec<Bytes> {
            if self.payload_step == 0 {
                return Vec::new();
            }
            let step =
                (self.fetch_count.get() / self.payload_step).min(self.steps_until_full.get());
            let msg_count = (CachedConfig::get().consensus.payload_batch_bytes as usize * step)
                / self.steps_until_full
                / EXTERNAL_MSG_MAX_BYTES;
            let mut result = Vec::with_capacity(msg_count);
            for _ in 0..msg_count {
                let mut data = vec![0; EXTERNAL_MSG_MAX_BYTES];
                thread_rng().fill_bytes(data.as_mut_slice());
                result.push(Bytes::from(data));
            }
            self.fetch_count = self.fetch_count.saturating_add(1);
            result
        }

        fn push(&mut self, _: Bytes) {
            panic!("not available for tests");
        }

        fn apply_config(&mut self, _: &ConsensusConfig) {
            panic!("not available for tests");
        }
    }
}
