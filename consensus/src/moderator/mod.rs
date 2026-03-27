pub use config::*;
pub use impl_::*;
pub use journal::batch::RecordBatch;
pub use journal::event::*;
pub use journal::point_ref_count::*;
pub use journal::record::*;
pub use journal::record_key::RecordKey;
pub use storage::store::*;

mod ban;
mod config;
mod impl_;
mod journal;
mod storage;

enum DelayedDbTask {
    Items {
        items: Vec<journal::item::JournalItemFull>,
        user_callback: Option<tokio::sync::oneshot::Sender<anyhow::Result<()>>>,
    },
}
