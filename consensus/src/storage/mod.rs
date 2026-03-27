pub use adapter_store::*;
pub use db::*;
pub use db_cleaner::*;
pub use store::*;

mod adapter_store;
mod db;
mod db_cleaner;
mod store;
mod tables;

pub fn meter_db_clean_error(kind: &'static str) {
    metrics::gauge!("tycho_mempool_db_clean_error_count", "kind" => kind).increment(1);
}

pub fn meter_db_wait_for_compact_time() -> tycho_util::metrics::HistogramGuard {
    tycho_util::metrics::HistogramGuard::begin("tycho_mempool_db_wait_for_compact_time")
}

fn meter_db_clean_points_time() -> tycho_util::metrics::HistogramGuard {
    tycho_util::metrics::HistogramGuard::begin("tycho_mempool_store_clean_points_time")
}
