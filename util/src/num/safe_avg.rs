#[derive(Default)]
pub struct SafeUnsignedAvg {
    sum: u128,
    counter: u128,
}

impl SafeUnsignedAvg {
    pub fn with_initial<V: Into<u128>>(v: V) -> Self {
        Self {
            sum: v.into(),
            counter: 1,
        }
    }

    pub fn accum<V: Into<u128>>(&mut self, v: V) {
        let v = v.into();
        self.sum = self.sum.checked_add(v).unwrap_or_else(|| {
            let partial = self.sum / self.counter;
            self.counter = 0;
            partial.saturating_add(v)
        });
        self.counter += 1;
    }

    pub fn get_avg(&self) -> u128 {
        self.get_avg_checked()
            .expect("should check first with `get_avg_checked`")
    }

    pub fn get_avg_checked(&self) -> Option<u128> {
        if self.counter == 0 {
            None
        } else if self.counter == 1 {
            Some(self.sum)
        } else {
            Some(self.sum.saturating_div(self.counter))
        }
    }
}

#[derive(Default)]
pub struct SafeSignedAvg {
    sum: i128,
    counter: i128,
}

impl SafeSignedAvg {
    pub fn with_initial<V: Into<i128>>(v: V) -> Self {
        Self {
            sum: v.into(),
            counter: 1,
        }
    }

    pub fn accum<V: Into<i128>>(&mut self, v: V) {
        let v = v.into();
        self.sum = self.sum.checked_add(v).unwrap_or_else(|| {
            let partial = self.sum / self.counter;
            self.counter = 0;
            partial.saturating_add(v)
        });
        self.counter += 1;
    }

    pub fn get_avg(&self) -> i128 {
        self.get_avg_checked()
            .expect("should check first with `get_avg_checked`")
    }

    pub fn get_avg_checked(&self) -> Option<i128> {
        if self.counter == 0 {
            None
        } else if self.counter == 1 {
            Some(self.sum)
        } else {
            Some(self.sum.saturating_div(self.counter))
        }
    }
}

pub struct SafeUnsignedVecAvg {
    idx: usize,
    counters: Vec<u128>,
    sums: Vec<u128>,
}

impl SafeUnsignedVecAvg {
    pub fn new(capacity: usize) -> Self {
        Self {
            idx: 0,
            counters: vec![0; capacity],
            sums: vec![0; capacity],
        }
    }

    pub fn accum<V: Into<u128>>(&mut self, idx: usize, v: V) {
        self.idx = idx;
        self.accum_curr(v);
    }

    pub fn accum_next<V: Into<u128>>(&mut self, v: V) {
        self.idx += 1;
        self.accum_curr(v);
    }

    fn accum_curr<V: Into<u128>>(&mut self, v: V) {
        let v = v.into();
        self.sums[self.idx] = self.sums[self.idx].checked_add(v).unwrap_or_else(|| {
            let partial = self.sums[self.idx] / self.counters[self.idx];
            self.counters[self.idx] = 0;
            partial.saturating_add(v)
        });
        self.counters[self.idx] += 1;
    }

    pub fn get_avg(&mut self, idx: usize) -> u128 {
        self.get_avg_checked(idx)
            .expect("should check first with `get_avg_checked`")
    }

    pub fn get_avg_checked(&mut self, idx: usize) -> Option<u128> {
        self.idx = idx;
        self.get_avg_curr()
    }

    pub fn get_avg_next(&mut self) -> u128 {
        self.get_avg_next_checked().unwrap()
    }

    pub fn get_avg_next_checked(&mut self) -> Option<u128> {
        self.idx += 1;
        self.get_avg_curr()
    }

    fn get_avg_curr(&self) -> Option<u128> {
        if self.counters[self.idx] == 0 {
            None
        } else {
            Some(self.sums[self.idx].saturating_div(self.counters[self.idx]))
        }
    }
}
