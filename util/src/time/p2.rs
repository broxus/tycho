use std::fmt::{Debug, Formatter};

#[derive(Default)]
pub struct P2 {
    quantile: f64,
    heights: [i64; 5],
    pos: [i64; 5],
    n_pos: [f64; 5],
    dn: [f64; 5],
    count: usize,
    filled: bool,
}

impl P2 {
    pub fn new(quantile: f64) -> Result<Self, Error> {
        if !(0.0..=1.0).contains(&quantile) {
            return Err(Error::InvalidQuantile(quantile));
        }
        let p2 = Self {
            quantile,
            heights: [0; 5],
            n_pos: [
                0.0,
                2.0 * quantile,
                4.0 * quantile,
                2.0 + 2.0 * quantile,
                4.0,
            ],
            dn: [0.0, quantile / 2.0, quantile, (1.0 + quantile) / 2.0, 1.0],
            pos: [0, 1, 2, 3, 4],
            count: 0,
            filled: false,
        };

        Ok(p2)
    }

    pub fn append(&mut self, data: i64) {
        if self.count < 5 {
            self.heights[self.count] = data;
            self.count += 1;
            return;
        }
        if !self.filled {
            self.filled = true;
            self.heights.sort_unstable();
        }
        self.append_data(data);
    }

    fn append_data(&mut self, data: i64) {
        let mut k: isize = -1;
        if data < self.heights[0] {
            k = 0;
            self.heights[0] = data;
        } else if self.heights[4] <= data {
            k = 3;
            self.heights[4] = data;
        } else {
            for i in 1..5 {
                if self.heights[i - 1] <= data && data < self.heights[i] {
                    k = i as isize - 1;
                    break;
                }
            }
        }

        for i in 0..5 {
            if i > k as usize {
                self.pos[i] += 1;
            }
            self.n_pos[i] += self.dn[i];
        }

        self.adjust_heights();
    }

    fn adjust_heights(&mut self) {
        for i in 1..4 {
            let n = self.pos[i];
            let np1 = self.pos[i + 1];
            let nm1 = self.pos[i - 1];
            let d = self.n_pos[i] - n as f64;

            if (d >= 1.0 && np1 - n > 1) || (d <= -1.0 && nm1 - n < -1) {
                let d = if d >= 0.0 { 1 } else { -1 };

                // Only adjust if values are different
                if d > 0 && self.heights[i + 1] != self.heights[i] {
                    let delta = self.heights[i + 1] - self.heights[i];
                    let np1 = self.pos[i + 1];
                    self.heights[i] += delta / (np1 - n).max(1);
                } else if d < 0 && self.heights[i] != self.heights[i - 1] {
                    let delta = self.heights[i] - self.heights[i - 1];
                    let nm1 = self.pos[i - 1];
                    self.heights[i] -= delta / (n - nm1).max(1);
                }

                self.pos[i] += d;
            }
        }
    }
    pub fn value(&self) -> i64 {
        if !self.filled {
            return match self.count {
                0 => 0,
                1 => self.heights[0],
                _ => {
                    let mut sorted = self.heights[..self.count].to_vec();
                    sorted.sort_unstable();
                    let rank = (self.quantile * self.count as f64) as usize;
                    sorted[rank]
                }
            };
        }
        self.heights[2]
    }
}

impl Debug for P2 {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("P2")
            .field("quantile", &self.quantile)
            .field("heights", &self.heights)
            .field("pos", &self.pos)
            .field("n_pos", &self.n_pos)
            .field("dn", &self.dn)
            .field("count", &self.count)
            .field("filled", &self.filled)
            .field("value", &self.value())
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Invalid quantile: {0}. Quantile must be in [0, 1]")]
    InvalidQuantile(f64),
}

#[cfg(test)]
mod test {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::P2;

    #[test]
    fn test_p2() {
        let mut data = (0..=100).collect::<Vec<_>>();
        let epsilon = 1;

        assert_percentile(&data, 0.25, epsilon);
        assert_percentile(&data, 0.5, epsilon);
        assert_percentile(&data, 0.75, epsilon);

        data.reverse();

        assert_percentile(&data, 0.25, epsilon);
        assert_percentile(&data, 0.5, epsilon);
        assert_percentile(&data, 0.75, epsilon);

        let repeated_data = vec![42; 1000];
        assert_percentile(&repeated_data, 0.25, 0);
        assert_percentile(&repeated_data, 0.5, 0);
        assert_percentile(&repeated_data, 0.75, 0);

        let mut rand_data = StdRng::seed_from_u64(42)
            .sample_iter(rand::distributions::Uniform::new(0, 100))
            .take(1000)
            .collect::<Vec<_>>();

        assert_percentile(&rand_data, 0.25, 5);
        assert_percentile(&rand_data, 0.5, 5);
        assert_percentile(&rand_data, 0.75, 5);

        rand_data.reverse();

        assert_percentile(&rand_data, 0.25, 5);
        assert_percentile(&rand_data, 0.5, 5);
        assert_percentile(&rand_data, 0.75, 5);

        rand_data.retain(|&x| x % 2 == 0);

        assert_percentile(&rand_data, 0.25, 5);
        assert_percentile(&rand_data, 0.5, 5);
        assert_percentile(&rand_data, 0.75, 5);

        rand_data.reverse();

        assert_percentile(&rand_data, 0.25, 5);
        assert_percentile(&rand_data, 0.5, 5);
        assert_percentile(&rand_data, 0.75, 5);
    }

    fn assert_percentile(data: &[i64], percentile: f64, epsilon: i64) {
        let expected = get_percentile(data, percentile);
        let mut p2 = P2::new(percentile).unwrap();
        for &d in data {
            p2.append(d);
        }
        let res = p2.value();
        assert!(
            (res - expected).abs() <= epsilon,
            "Expected {}th percentile to be close to {}, got {} (diff > {})",
            percentile * 100.0,
            expected,
            res,
            epsilon
        );
    }

    fn get_percentile(numbers: &[i64], percentile: f64) -> i64 {
        let mut sorted = numbers.to_vec();
        sorted.sort_unstable();

        let index = (percentile * (sorted.len() - 1) as f64).round() as usize;
        sorted[index]
    }
}
