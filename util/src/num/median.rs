use std::cmp::Reverse;
use std::collections::BinaryHeap;

#[derive(Default)]
pub struct StreamingUnsignedMedian {
    lo: BinaryHeap<u128>,          // max-heap
    hi: BinaryHeap<Reverse<u128>>, // min-heap
}

impl StreamingUnsignedMedian {
    pub fn insert<V: Into<u128>>(&mut self, v: V) {
        let v = v.into();
        // 1) put to hi, move the minimal to lo
        self.hi.push(Reverse(v));
        if let Some(Reverse(v)) = self.hi.pop() {
            self.lo.push(v);
        }
        // 2) rebalance
        if self.lo.len() > self.hi.len() + 1 {
            let v = self.lo.pop().unwrap();
            self.hi.push(Reverse(v));
        }
    }

    pub fn get_median(&self) -> u128 {
        self.get_median_checked()
            .expect("should check first with `median_checked`")
    }

    pub fn get_median_checked(&self) -> Option<u128> {
        if self.lo.is_empty() {
            return None;
        }
        if self.lo.len() > self.hi.len() {
            Some(*self.lo.peek().unwrap())
        } else {
            let a = *self.lo.peek().unwrap();
            let b = self.hi.peek().unwrap().0;
            Some(a.saturating_add(b).saturating_div(2))
        }
    }
}

pub struct VecOfStreamingUnsignedMedian {
    idx: usize,
    inner: Vec<StreamingUnsignedMedian>,
}

impl VecOfStreamingUnsignedMedian {
    pub fn new(capacity: usize) -> Self {
        let mut vec = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            vec.push(StreamingUnsignedMedian::default());
        }
        Self { idx: 0, inner: vec }
    }

    pub fn accum<V: Into<u128>>(&mut self, idx: usize, v: V) {
        self.insert(idx, v);
    }

    pub fn accum_next<V: Into<u128>>(&mut self, v: V) {
        self.insert_next(v);
    }

    pub fn insert<V: Into<u128>>(&mut self, idx: usize, v: V) {
        self.idx = idx;
        self.insert_curr(v);
    }

    pub fn insert_next<V: Into<u128>>(&mut self, v: V) {
        self.idx += 1;
        self.insert_curr(v);
    }

    fn insert_curr<V: Into<u128>>(&mut self, v: V) {
        self.inner[self.idx].insert(v);
    }

    pub fn get_avg(&mut self, idx: usize) -> u128 {
        self.get_median(idx)
    }

    pub fn get_avg_checked(&mut self, idx: usize) -> Option<u128> {
        self.get_median_checked(idx)
    }

    pub fn get_avg_next(&mut self) -> u128 {
        self.get_median_next()
    }

    pub fn get_avg_next_checked(&mut self) -> Option<u128> {
        self.get_median_next_checked()
    }

    pub fn get_median(&mut self, idx: usize) -> u128 {
        self.get_median_checked(idx)
            .expect("should check first with `get_median_checked`")
    }

    pub fn get_median_checked(&mut self, idx: usize) -> Option<u128> {
        self.idx = idx;
        self.get_median_curr()
    }

    pub fn get_median_next(&mut self) -> u128 {
        self.get_median_next_checked()
            .expect("should check first with `get_median_checked`")
    }

    pub fn get_median_next_checked(&mut self) -> Option<u128> {
        self.idx += 1;
        self.get_median_curr()
    }

    fn get_median_curr(&self) -> Option<u128> {
        self.inner[self.idx].get_median_checked()
    }
}
