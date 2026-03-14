use tl_proto::{RawBytes, TlRead, TlWrite};

use crate::engine::MempoolConfig;
use crate::models::PointKey;

#[derive(TlRead, TlWrite)]
#[tl(boxed, id = "journal.point", scheme = "proto.tl")]
pub struct JournalPoint<'tl> {
    pub ref_count: u32,
    pub data: JournalPointData<'tl>,
}

#[derive(TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum JournalPointData<'tl> {
    #[tl(id = "journal.pointStoredData.sub")]
    Sub,
    #[tl(id = "journal.pointStoredData.data")]
    Data(RawBytes<'tl, tl_proto::Boxed>),
}

impl JournalPoint<'_> {
    pub const VARIANT_WITHOUT_DATA: usize = 4 + 4 + 4;

    pub fn max_tl_bytes(conf: &MempoolConfig) -> usize {
        4 + 4 + 4 + conf.point_max_bytes
    }

    pub fn fill_sub(ref_count: u32, dest: &mut [u8; 4 + 4 + 4]) {
        dest[..4].copy_from_slice(&Self::TL_ID.to_le_bytes());
        dest[4..4 + 4].copy_from_slice(&ref_count.to_le_bytes());
        let sub_tl_id: u32 = tl_proto::id!("journal.pointStoredData.sub", scheme = "proto.tl");
        dest[4 + 4..4 + 4 + 4].copy_from_slice(&sub_tl_id.to_le_bytes());
    }

    pub fn merge_bytes<'a>(key: &[u8], iter: impl Iterator<Item = &'a [u8]>) -> Option<Vec<u8>> {
        fn none_if_err(ref_count: i64, key: &[u8]) -> Option<u32> {
            match u32::try_from(ref_count) {
                Ok(ref_count) => Some(ref_count),
                Err(_) => {
                    tracing::error!(
                        "journal point merge failed: ref counter {ref_count} too big at {}",
                        PointKey::format_loose(key)
                    );
                    None
                }
            }
        }

        let mut ref_count: i64 = 0;
        let mut data_bytes: Option<&[u8]> = None;

        for item in iter {
            let value = match JournalPoint::read_from(&mut &*item) {
                Ok(value) => value,
                Err(tl_error) => {
                    tracing::error!(
                        "DB polluted: cannot merge journal points {tl_error} at {}",
                        PointKey::format_loose(key)
                    );
                    return None;
                }
            };
            match value.data {
                JournalPointData::Sub => ref_count -= i64::from(value.ref_count),
                JournalPointData::Data(bytes) => {
                    let bytes = bytes.into_inner();
                    ref_count += i64::from(value.ref_count);
                    let data_mismatch = if cfg!(debug_assertions) {
                        data_bytes.is_some_and(|stored| stored != bytes)
                    } else {
                        data_bytes.is_some_and(|stored| stored.len() != bytes.len())
                    };
                    if data_mismatch {
                        tracing::error!(
                            "journal point merge failed: point bytes differs at {}",
                            PointKey::format_loose(key)
                        );
                        return None;
                    } else {
                        data_bytes = Some(bytes);
                    }
                }
            }
        }

        if ref_count < 0 || data_bytes.is_none() {
            // we processed only subs, counter was not increased
            if ref_count > 0 {
                tracing::error!(
                    "journal point merge failed: ref count without data is {ref_count}>0 at {}",
                    PointKey::format_loose(key)
                );
                return None;
            }
            let ref_count = none_if_err(-ref_count, key)?;
            let mut vec = Vec::with_capacity(JournalPoint::VARIANT_WITHOUT_DATA);
            let data = JournalPointData::Sub;
            JournalPoint { ref_count, data }.write_to(&mut vec);
            Some(vec)
        } else {
            let ref_count = none_if_err(ref_count, key)?;
            let Some(data) = data_bytes else {
                unreachable!("data is checked above");
            };
            let mut vec = Vec::with_capacity(JournalPoint::VARIANT_WITHOUT_DATA + data.len());
            let data = JournalPointData::Data(RawBytes::new(data));
            JournalPoint { ref_count, data }.write_to(&mut vec);
            Some(vec)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn fill_sub_follows_tl() {
        let ref_count = rand::random();
        let mut j_point_buff = [0; _];

        let mut vec = Vec::new();
        let data = JournalPointData::Sub;
        JournalPoint { ref_count, data }.write_to(&mut vec);

        JournalPoint::fill_sub(ref_count, &mut j_point_buff);

        assert_eq!(&j_point_buff[..], &vec[..]);
    }
}
