use std::collections::VecDeque;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

pub use p2::*;

mod p2;

pub fn now_sec() -> u32 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as u32
}

pub fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

pub fn shifted_interval(period: Duration, max_shift: Duration) -> tokio::time::Interval {
    let shift = rand::random_range(Duration::ZERO..max_shift);
    tokio::time::interval_at(tokio::time::Instant::now() + shift, period + shift)
}

pub fn shifted_interval_immediate(period: Duration, max_shift: Duration) -> tokio::time::Interval {
    let shift = rand::random_range(Duration::ZERO..max_shift);
    tokio::time::interval(period + shift)
}

pub trait Clock {
    fn now(&self) -> Instant;
}

pub struct RealClock;
impl Clock for RealClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

pub struct RollingP2Estimator<C: Clock = RealClock> {
    estimators: VecDeque<P2>,
    timings: VecDeque<Instant>,
    quantile: f64,
    window_length: Duration,
    max_windows: usize,
    clock: C,
}

impl<C: Clock> RollingP2Estimator<C> {
    pub fn new_with_config(
        quantile: f64,
        window_length: Duration,
        max_windows: usize,
        clock: C,
    ) -> Result<Self, RollingP2EstimatorError> {
        if !(0.0..=1.0).contains(&quantile) {
            return Err(p2::Error::InvalidQuantile(quantile).into());
        }

        if window_length.is_zero() {
            return Err(RollingP2EstimatorError::ZeroWindowLength);
        }

        if max_windows == 0 {
            return Err(RollingP2EstimatorError::ZeroMaxWindows);
        }

        Ok(RollingP2Estimator {
            estimators: VecDeque::with_capacity(max_windows),
            timings: VecDeque::with_capacity(max_windows),
            quantile,
            window_length,
            max_windows,
            clock,
        })
    }

    pub fn append(&mut self, value: i64) {
        self.get_estimator().append(value);
    }

    fn get_estimator(&mut self) -> &mut P2 {
        let now = self.clock.now();

        let needs_new_window = self.estimators.is_empty()
            || now.duration_since(*self.timings.back().unwrap()) > self.window_length;

        if needs_new_window {
            if self.estimators.len() >= self.max_windows {
                self.estimators.pop_front();
                self.timings.pop_front();
            }

            self.estimators.push_back(P2::new(self.quantile).unwrap());
            self.timings.push_back(now);
        }

        self.estimators.back_mut().unwrap()
    }

    pub fn exponentially_weighted_average(&self) -> Option<i64> {
        if self.estimators.is_empty() {
            return None;
        }

        let now = self.clock.now();

        let mut total_weight = 0.0;
        let mut weighted_sum = 0.0;

        // Calculate exponentially weighted average
        for (estimator, &timing) in self.estimators.iter().zip(self.timings.iter()) {
            let age = now.duration_since(timing).as_secs_f64();

            // decay factor: e^(-age/window)
            // new values (age=0) have weight 1.0
            // values at window_length have weight 1/e
            // older values decay towards 0
            let weight = (-age / self.window_length.as_secs_f64()).exp();

            let estimate = estimator.value() as f64;
            weighted_sum += estimate * weight;
            total_weight += weight;
        }

        if total_weight > 0.0 {
            Some((weighted_sum / total_weight) as i64)
        } else {
            None
        }
    }

    pub fn max_over_window(&self) -> Option<i64> {
        self.estimators
            .iter()
            .map(|estimator| estimator.value())
            .max()
    }
}

impl RollingP2Estimator<RealClock> {
    pub fn new(quantile: f64) -> Result<Self, RollingP2EstimatorError> {
        Self::new_with_config(quantile, Duration::from_secs(60), 5, RealClock)
    }
}

/// Clock that will not decrease in runtime.
/// **WARNING** Changing server time between node runs should be avoided or handled with care.
pub struct MonotonicClock {
    init_instant: Instant,
    init_system_time: std::time::SystemTime,
}
static MONOTONIC_CLOCK: LazyLock<MonotonicClock> = LazyLock::new(|| MonotonicClock {
    init_instant: Instant::now(),
    init_system_time: std::time::SystemTime::now(),
});
impl MonotonicClock {
    pub fn now_millis() -> u64 {
        // initialize lazy lock
        let Self {
            init_instant,
            init_system_time,
        } = *MONOTONIC_CLOCK;

        let since_init = {
            let now = Instant::now();
            now.checked_duration_since(init_instant)
                .unwrap_or_else(|| panic!("current {now:?} < initial {init_instant:?}"))
        };

        let system_time = init_system_time.checked_add(since_init).unwrap_or_else(|| {
            panic!(
                "overflow at init system time {} + duration {} since {init_instant:?}",
                humantime::format_rfc3339_nanos(init_system_time),
                humantime::format_duration(since_init),
            )
        });

        let since_epoch = system_time
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or_else(|err| {
                panic!(
                    "calculated current {system_time:?} < UNIX_EPOCH {:?} for {}",
                    std::time::SystemTime::UNIX_EPOCH,
                    humantime::format_duration(err.duration())
                )
            });

        u64::try_from(since_epoch.as_millis()).unwrap_or_else(|_| {
            panic!(
                "current time millis exceed u64: {}",
                since_epoch.as_millis()
            )
        })
    }
}

#[derive(thiserror::Error, Debug)]
pub enum RollingP2EstimatorError {
    #[error(transparent)]
    P2Error(#[from] p2::Error),
    #[error("Window length must be greater than zero")]
    ZeroWindowLength,
    #[error("Max windows must be greater than zero")]
    ZeroMaxWindows,
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;

    use super::*;

    #[derive(Clone)]
    struct MockClock {
        current_time: Rc<Cell<Instant>>,
    }

    impl MockClock {
        fn new() -> Self {
            Self {
                current_time: Rc::new(Cell::new(Instant::now())),
            }
        }

        fn advance(&self, duration: Duration) {
            let new_time = self.current_time.get() + duration;
            self.current_time.set(new_time);
        }
    }

    impl Clock for MockClock {
        fn now(&self) -> Instant {
            self.current_time.get()
        }
    }

    #[test]
    fn test_invalid_quantile() {
        let clock = MockClock::new();
        assert!(
            RollingP2Estimator::new_with_config(1.5, Duration::from_secs(60), 5, clock).is_err()
        );
    }

    #[test]
    fn test_single_window() {
        let clock = MockClock::new();
        let mut estimator =
            RollingP2Estimator::new_with_config(0.95, Duration::from_secs(60), 5, clock.clone())
                .unwrap();

        estimator.append(1);
        estimator.append(2);
        estimator.append(3);

        assert_eq!(estimator.exponentially_weighted_average(), Some(3));
    }

    #[test]
    fn test_multiple_windows_with_cleanup() {
        let clock = MockClock::new();
        let mut estimator =
            RollingP2Estimator::new_with_config(0.95, Duration::from_secs(60), 5, clock.clone())
                .unwrap();

        // First window
        estimator.append(1);
        assert_eq!(estimator.exponentially_weighted_average().unwrap(), 1);

        // Second window
        clock.advance(Duration::from_secs(30));
        estimator.append(2);

        // Third window
        clock.advance(Duration::from_secs(31)); // Total 61s
        estimator.append(10);

        let value = estimator.exponentially_weighted_average().unwrap();
        println!("Value: {value}");
        assert_eq!(value, 7); // sma of 1, 2, 10 with time decay

        let max_value = estimator.max_over_window().unwrap();
        assert_eq!(max_value, 10);
    }

    #[test]
    fn test_max_windows_with_cleanup() {
        let clock = MockClock::new();
        let mut estimator = RollingP2Estimator::new_with_config(
            0.95,
            Duration::from_secs(120), // 2 minute window
            2,                        // Only allow 2 windows
            clock.clone(),
        )
        .unwrap();

        println!("\n=== First window ===");
        estimator.append(1);
        println!(
            "After first append: {} estimators",
            estimator.estimators.len()
        );
        assert_eq!(estimator.estimators.len(), 1);

        println!("\n=== Same window (61s < 120s) ===");
        clock.advance(Duration::from_secs(61));
        estimator.append(5);
        println!(
            "After second append: {} estimators",
            estimator.estimators.len()
        );
        assert_eq!(estimator.estimators.len(), 1); // Changed expectation

        println!("\n=== Second window (122s > 120s) ===");
        clock.advance(Duration::from_secs(61)); // Total 122s
        estimator.append(10);
        println!(
            "After third append: {} estimators",
            estimator.estimators.len()
        );
        assert_eq!(estimator.estimators.len(), 2);

        let value = estimator.exponentially_weighted_average().unwrap();
        assert!(value > 5, "value = {value}");
        let max_value = estimator.max_over_window().unwrap();
        assert_eq!(max_value, 10);
    }

    #[test]
    fn test_window_cleanup() {
        let clock = MockClock::new();
        let window_length = Duration::from_secs(60);
        let mut estimator =
            RollingP2Estimator::new_with_config(0.95, window_length, 2, clock.clone()).unwrap();

        // Step 1: Add value to the first window
        estimator.append(1);
        assert_eq!(estimator.estimators.len(), 1);
        assert_eq!(estimator.timings.len(), 1);

        // Step 2: Create the second window
        clock.advance(window_length + Duration::from_secs(1));
        estimator.append(2);
        assert_eq!(estimator.estimators.len(), 2);
        assert_eq!(estimator.timings.len(), 2);

        // Step 3: Create third window - should trigger cleanup
        clock.advance(window_length + Duration::from_secs(1));
        estimator.append(3);

        // Verify cleanup mechanics
        assert_eq!(
            estimator.estimators.len(),
            2,
            "Should maintain max 2 windows"
        );
        assert_eq!(estimator.timings.len(), 2, "Should maintain max 2 timings");

        // The value will be between 2 and 3 since a window with value 1 was removed
        let value = estimator.exponentially_weighted_average().unwrap();
        assert!(value >= 2, "Expected value >= 2, got {value}");
        assert!(value <= 3, "Expected value <= 3, got {value}");

        // Add more values to the latest window to verify it's active
        estimator.append(4);
        estimator.append(5);

        let final_value = estimator.exponentially_weighted_average().unwrap();
        assert!(
            final_value > value,
            "Value should increase after adding larger numbers"
        );
    }
}
