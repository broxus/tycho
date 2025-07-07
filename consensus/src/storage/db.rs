use std::sync::Arc;

use tycho_storage::StorageContext;
use tycho_storage::kv::NamedTables;
use tycho_util::metrics::HistogramGuard;
use weedb::WeeDb;
use weedb::rocksdb::{IteratorMode, ReadOptions, WaitForCompactOptions, WriteBatch};

use crate::engine::round_watch::{Commit, RoundWatch};
use crate::storage::tables::MempoolTables;

pub struct MempoolDb {
    // NOTE: Context should have at least the lifetime of the created DB.
    #[allow(unused)]
    pub(super) ctx: StorageContext,
    pub(super) db: WeeDb<MempoolTables>,
    pub(super) commit_finished: RoundWatch<Commit>,
}

impl MempoolDb {
    /// Opens an existing or creates a new mempool `RocksDB` instance.
    pub fn open(
        ctx: StorageContext,
        commit_finished: RoundWatch<Commit>,
    ) -> anyhow::Result<Arc<Self>> {
        let db = ctx.open_preconfigured(MempoolTables::NAME)?;

        // TODO: Add migrations here if needed. However, it might require making this method `async`.

        Ok(Arc::new(Self {
            db,
            ctx,
            commit_finished,
        }))
    }

    /// delete all stored data up to provided value (exclusive);
    /// returns range of logically deleted keys
    pub(super) fn clean(
        &self,
        up_to_exclusive: &[u8; super::KEY_LEN],
    ) -> anyhow::Result<Option<(u32, u32)>> {
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_clean_time");
        let zero = [0_u8; super::KEY_LEN];
        let none = None::<[u8; super::KEY_LEN]>;

        let status_cf = self.db.tables().points_status.cf();
        let info_cf = self.db.tables().points_info.cf();
        let points_cf = self.db.tables().points.cf();
        let rocksdb = self.db.rocksdb();

        let mut opt = ReadOptions::default();
        opt.set_iterate_upper_bound(up_to_exclusive);
        let mut iter = rocksdb.raw_iterator_cf_opt(&status_cf, opt);
        iter.status()?;
        iter.seek_to_first();
        iter.status()?;
        let first = iter.key().map(|first_key| {
            super::parse_round(first_key).unwrap_or_else(|| {
                tracing::error!(
                    "mempool lower clean bound will be shown as 0: {}",
                    super::format_key(first_key)
                );
                0
            })
        });
        iter.status()?;
        iter.seek_to_last();
        iter.status()?;
        let last = iter
            .key()
            .map(|last_key| {
                super::parse_round(last_key).unwrap_or_else(|| {
                    tracing::error!(
                        "mempool upper clean bound will be shown as 0: {}",
                        super::format_key(last_key)
                    );
                    0
                })
            })
            .or(first);
        iter.status()?;
        drop(iter);

        // in case we'll return to `db.delete_file_in_range_cf()`:
        // * at first delete status, as their absense prevents incorrect access to points data
        // * then delete info, as it leaves points usable only for upload
        // * at last delete points safely

        let mut batch = WriteBatch::default();
        batch.delete_range_cf(&status_cf, &zero, up_to_exclusive);
        batch.delete_range_cf(&info_cf, &zero, up_to_exclusive);
        batch.delete_range_cf(&points_cf, &zero, up_to_exclusive);
        rocksdb.write(batch)?;

        rocksdb.compact_range_cf(&status_cf, none, Some(up_to_exclusive));
        rocksdb.compact_range_cf(&info_cf, none, Some(up_to_exclusive));
        rocksdb.compact_range_cf(&points_cf, none, Some(up_to_exclusive));

        Ok(first.zip(last))
    }

    /// Use when no reads/writes are possible, and this should finish prior other ops
    pub(super) fn wait_for_compact(&self) -> anyhow::Result<()> {
        let mut opt = WaitForCompactOptions::default();
        opt.set_flush(true);
        self.db.rocksdb().wait_for_compact(&opt)?;
        Ok(())
    }

    /// returns `true` if only one value existed and was equal to provided arg
    ///
    /// if returns `false` - should drop all data and wait for compactions to finish
    pub(super) fn has_compatible_data(&self, data_version: &[u8]) -> anyhow::Result<bool> {
        let default_cf = self.db.rocksdb();

        let mut has_same = false;
        let mut other_values = Vec::<Box<[u8]>>::new();

        for result in default_cf.iterator(IteratorMode::Start) {
            let (key, _) = result?;
            if &*key == data_version {
                has_same = true;
            } else {
                other_values.push(key);
            }
        }

        if has_same & other_values.is_empty() {
            return Ok(true);
        }

        let mut batch = WriteBatch::default();

        for other in other_values {
            batch.delete(other);
        }
        if !has_same {
            batch.put(data_version, []);
        }
        default_cf.write(batch)?;

        default_cf.compact_range(None::<&[u8]>, None::<&[u8]>);

        Ok(false)
    }
}
