pub mod point_status;

use std::fmt::{Display, Formatter};

use tycho_util::metrics::HistogramGuard;
use weedb::rocksdb::{IteratorMode, ReadOptions, WaitForCompactOptions, WriteBatch};

use crate::MempoolDb;

#[derive(Clone)]
pub struct MempoolStorage {
    pub db: MempoolDb,
}
impl MempoolStorage {
    pub const KEY_LEN: usize = 4 + 32;

    pub fn fill_key(round: u32, digest: &[u8; 32], key: &mut [u8; Self::KEY_LEN]) {
        Self::fill_prefix(round, key);
        key[4..].copy_from_slice(&digest[..]);
    }
    pub fn fill_prefix(round: u32, key: &mut [u8; Self::KEY_LEN]) {
        key[..4].copy_from_slice(&round.to_be_bytes()[..]);
    }
    /// function of limited usage: zero round does not exist by application logic
    /// and 4 zero bytes usually represents empty value in storage;
    /// here None represents value less than 4 bytes
    pub(crate) fn parse_round(bytes: &[u8]) -> Option<u32> {
        if bytes.len() < 4 {
            None
        } else {
            let mut round_bytes = [0_u8; 4];
            round_bytes.copy_from_slice(&bytes[..4]);
            Some(u32::from_be_bytes(round_bytes))
        }
    }
    pub fn format_key(bytes: &[u8]) -> String {
        if let Some(round) = Self::parse_round(bytes) {
            if bytes.len() == Self::KEY_LEN {
                format!("round {round} digest {}", BytesFmt(&bytes[4..]))
            } else {
                format!("unknown {} bytes: {:.12}", bytes.len(), BytesFmt(bytes))
            }
        } else {
            format!("unknown short {} bytes {}", bytes.len(), BytesFmt(bytes))
        }
    }

    /// delete all stored data up to provided value (exclusive);
    /// returns range of logically deleted keys
    pub fn clean(
        &self,
        up_to_exclusive: &[u8; Self::KEY_LEN],
    ) -> anyhow::Result<Option<(u32, u32)>> {
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_clean_time");
        let zero = [0_u8; Self::KEY_LEN];
        let none = None::<[u8; Self::KEY_LEN]>;

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
            MempoolStorage::parse_round(first_key).unwrap_or_else(|| {
                tracing::error!(
                    "mempool lower clean bound will be shown as 0: {}",
                    Self::format_key(first_key)
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
                MempoolStorage::parse_round(last_key).unwrap_or_else(|| {
                    tracing::error!(
                        "mempool upper clean bound will be shown as 0: {}",
                        Self::format_key(last_key)
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
    pub fn wait_for_compact(&self) -> anyhow::Result<()> {
        let mut opt = WaitForCompactOptions::default();
        opt.set_flush(true);
        self.db.rocksdb().wait_for_compact(&opt)?;
        Ok(())
    }

    /// returns `true` if only one value existed and was equal to provided arg
    ///
    /// if returns `false` - should drop all data and wait for compactions to finish
    pub fn has_compatible_data(&self, data_version: &[u8]) -> anyhow::Result<bool> {
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

pub struct BytesFmt<'a>(pub &'a [u8]);
impl Display for BytesFmt<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let len = f.precision().unwrap_or(self.0.len());
        for byte in &self.0[..len] {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}
