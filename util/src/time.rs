use std::time::Duration;

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
