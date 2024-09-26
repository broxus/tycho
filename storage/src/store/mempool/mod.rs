pub mod point_status;

use tycho_util::metrics::HistogramGuard;
use weedb::rocksdb::WriteBatch;

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
            let mut digest = String::with_capacity((Self::KEY_LEN - 4) * 2);
            for byte in &bytes[4..Self::KEY_LEN] {
                digest.push_str(&format!("{byte:02x?}"));
            }
            format!("round {round} digest {digest} total bytes: {}", bytes.len())
        } else {
            let mut smth = String::with_capacity(bytes.len() * 2);
            for byte in &bytes[4..Self::KEY_LEN] {
                smth.push_str(&format!("{byte:02x?}"));
            }
            format!("unknown short bytes {smth}")
        }
    }

    /// delete all stored data up to provided value (exclusive)
    pub fn clean(&self, up_to_exclusive: &[u8; Self::KEY_LEN]) -> anyhow::Result<()> {
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_clean_time");
        let zero = [0_u8; Self::KEY_LEN];
        let none = None::<[u8; Self::KEY_LEN]>;

        let status_cf = self.db.tables().points_status.cf();
        let info_cf = self.db.tables().points_info.cf();
        let points_cf = self.db.tables().points.cf();
        let rocksdb = self.db.rocksdb();

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

        Ok(())
    }
}
