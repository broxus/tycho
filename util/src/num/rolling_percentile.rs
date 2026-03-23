use std::collections::VecDeque;

use tycho_util::FastHashMap;

#[derive(Debug, Clone, Default)]
pub struct RollingPercentiles<T> {
    window: usize,
    buf: VecDeque<T>,
    vals: FastHashMap<u8, T>,
}

impl<T: Copy + Ord> RollingPercentiles<T> {
    pub fn new(window: usize) -> Self {
        let window = window.max(100);
        Self {
            window,
            buf: VecDeque::with_capacity(window),
            vals: Default::default(),
        }
    }

    pub fn window(&self) -> usize {
        self.window
    }

    pub fn push(&mut self, v: T) {
        if self.buf.len() == self.window {
            self.buf.pop_front();
        }
        self.buf.push_back(v);
        self.vals.clear();
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn window_is_filled(&self) -> bool {
        self.buf.len() >= self.window
    }

    pub fn percentile(&mut self, p: u8) -> Option<T> {
        if self.buf.is_empty() {
            return None;
        }

        if let Some(val) = self.vals.get(&p) {
            return Some(*val);
        }

        let mut v: Vec<_> = self.buf.iter().copied().collect();
        v.sort_unstable();

        if v.len() == 1 {
            let val = v[0];
            self.vals.insert(p, val);
            return Some(val);
        }

        let perc = p as f64;
        let perc = perc.clamp(0.0, 100.0);
        let pos = (perc / 100.0) * ((v.len() - 1) as f64);
        let pos = (pos.round() as usize).min(v.len() - 1);

        let val = v[pos];
        self.vals.insert(p, val);
        Some(val)
    }

    pub fn bounds(&mut self, p_low: u8, p_high: u8) -> Option<(T, T)> {
        let lo = self.percentile(p_low)?;
        let hi = self.percentile(p_high)?;
        Some((lo.min(hi), lo.max(hi)))
    }

    pub fn clip(&mut self, x: T, p_low: u8, p_high: u8) -> Option<T> {
        let (lo, hi) = self.bounds(p_low, p_high)?;
        Some(x.max(lo).min(hi))
    }

    pub fn push_and_clip(&mut self, x: T, p_low: u8, p_high: u8) -> T {
        if !self.window_is_filled() {
            self.push(x);
            return x;
        }
        let clipped = self
            .clip(x, p_low, p_high)
            .expect("percentiles buf should contain at least one value here");
        self.push(x);
        clipped
    }
}

#[cfg(test)]
mod tests {
    use super::RollingPercentiles;

    #[test]
    fn push_and_clip_returns_input_before_window_is_filled() {
        let mut pct = RollingPercentiles::new(100);
        for _ in 0..99 {
            assert_eq!(pct.push_and_clip(10u128, 1, 99), 10);
        }
        assert_eq!(pct.len(), 99);
        assert_eq!(pct.push_and_clip(100u128, 1, 99), 100);
        assert_eq!(pct.len(), 100);
    }

    #[test]
    fn push_and_clip_clips_against_history_only_when_window_is_filled() {
        let mut pct = RollingPercentiles::new(100);
        for _ in 0..99 {
            assert_eq!(pct.push_and_clip(10u128, 1, 99), 10);
        }
        assert_eq!(pct.push_and_clip(100u128, 1, 99), 100);
        assert_eq!(pct.percentile(99), Some(10));
        assert_eq!(pct.push_and_clip(1000u128, 1, 99), 10);
        assert_eq!(pct.len(), 100);
        assert_eq!(pct.percentile(100), Some(1000));
    }
}
