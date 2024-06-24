use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rand::Rng;

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
    let shift = rand::thread_rng().gen_range(Duration::ZERO..max_shift);
    tokio::time::interval_at(tokio::time::Instant::now() + shift, period + shift)
}

pub fn shifted_interval_immediate(period: Duration, max_shift: Duration) -> tokio::time::Interval {
    let shift = rand::thread_rng().gen_range(Duration::ZERO..max_shift);
    tokio::time::interval(period + shift)
}

pub fn duration_between_unix_and_instant(unix_time: u64, instant: Instant) -> Duration {
    // Convert Unix timestamp to SystemTime
    let system_time = UNIX_EPOCH + Duration::from_secs(unix_time);

    // Convert SystemTime to Instant
    let instant_from_unix = Instant::now()
        - SystemTime::now()
            .duration_since(system_time)
            .unwrap_or(Duration::ZERO);

    // Calculate the duration
    if instant_from_unix <= instant {
        instant - instant_from_unix
    } else {
        instant_from_unix - instant
    }
}
