//! Shared WAL/checkpoint record format.
//!
//! Each record is `[payload_hash][HashedRecord]`.
//! ```text
//! [payload_hash: BLAKE3([payload_len][payload])]
//! HashedRecord:
//! [payload_len: u64 LE]
//! Payload:
//! [id: u64 LE]
//! [insert_count: u32 LE]
//! [remove_count: u32 LE]
//! N * [hash][idx: u64 LE][born_round: u32 LE][data_len: u16 LE][data]
//! M * [hash]
//! ```
//!
//! Checkpoints use the same layout as WAL records and must have no removes.
//!
//! We intentionally trust the BLAKE3 hash over `[payload_len][payload]`. Only
//! the hash itself is unprotected; after the payload hash matches, we assume the
//! record was produced by our own node. So allocating buffers with exact decoded
//! sizes is fine, even if they are multi-gib.
use anyhow::{Context, Result, ensure};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tycho_types::cell::HashBytes;
use tycho_util::io::ByteOrderRead;

use crate::storage::shard_state::cell_nursery::{NurseryDelta, NurseryEntryRecord};
use crate::storage::shard_state::counters::Idx;

pub const HASH_LEN: usize = blake3::OUT_LEN;

pub struct EncodedRecord {
    id: u64,
    bytes: Bytes,
    stats: RecordStats,
}

impl EncodedRecord {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    pub fn hash(&self) -> &[u8; HASH_LEN] {
        self.bytes[0..HASH_LEN].as_array().unwrap()
    }

    pub fn stats(&self) -> &RecordStats {
        &self.stats
    }
}

pub fn encode_record(id: u64, delta: &NurseryDelta) -> EncodedRecord {
    let insert_count = delta.inserts.len() as u64;
    let remove_count = delta.removes.len() as u64;
    let cell_bytes = delta
        .inserts
        .iter()
        .map(|i| i.data.len() as u64)
        .sum::<u64>();
    let metadata_bytes = estimate_total_metadata_len(insert_count, remove_count);
    let payload_bytes = metadata_bytes + cell_bytes;

    let mut bytes = BytesMut::with_capacity(PREFIX_LEN as usize + payload_bytes as usize);

    bytes.put_slice(&[0; HASH_LEN]);
    bytes.put_u64_le(payload_bytes);

    bytes.put_u64_le(id);
    bytes.put_u32_le(
        insert_count
            .try_into()
            .expect("nursery insert count must fit u32"),
    );
    bytes.put_u32_le(
        remove_count
            .try_into()
            .expect("nursery remove count must fit u32"),
    );

    for insert in &delta.inserts {
        bytes.put_slice(insert.hash.as_array());
        bytes.put_u64_le(insert.idx.get());
        bytes.put_u32_le(insert.born_round);
        bytes.put_u16_le(
            insert
                .data
                .len()
                .try_into()
                .expect("cell bytes must fit u16"),
        );
        bytes.put_slice(insert.data.as_ref());
    }

    for hash in &delta.removes {
        bytes.put_slice(hash.as_array());
    }

    {
        let hash = blake3::hash(&bytes[HASH_LEN..]);
        bytes[0..HASH_LEN].copy_from_slice(hash.as_slice());
    }

    let bytes = bytes.freeze();
    assert_eq!(bytes.len() as u64, PREFIX_LEN + payload_bytes);

    EncodedRecord {
        id,
        bytes,
        stats: RecordStats {
            payload_bytes,
            metadata_bytes,
            cell_bytes,
            insert_ops: insert_count,
            remove_ops: remove_count,
        },
    }
}

pub struct DecodedRecord {
    pub id: u64,
    pub hash: [u8; HASH_LEN],
    pub delta: NurseryDelta,
    pub stats: RecordStats,
}

pub fn decode_record(io: &mut impl std::io::Read, buffer: &mut Vec<u8>) -> Result<DecodedRecord> {
    buffer.clear();

    let mut payload_hash = [0u8; HASH_LEN];
    io.read_exact(&mut payload_hash)?;

    let payload_bytes = io.read_le_u64()?;
    buffer.extend_from_slice(&payload_bytes.to_le_bytes());

    let buffer_len = payload_bytes
        .checked_add(8)
        .and_then(|l| usize::try_from(l).ok())
        .context("payload len is too big")?;
    buffer.resize(buffer_len, 0);

    io.read_exact(&mut buffer[8..])?;
    ensure!(
        blake3::hash(buffer).as_bytes() == &payload_hash,
        "nursery payload hash mismatch"
    );

    let data = &mut &buffer[8..];
    let id = data.get_u64_le();
    let insert_count = data.get_u32_le() as usize;
    let remove_count = data.get_u32_le() as usize;

    let mut inserts = Vec::with_capacity(insert_count);
    let mut cell_bytes = 0;

    for _ in 0..insert_count {
        let mut hash = [0; HASH_LEN];
        data.copy_to_slice(&mut hash);
        let idx = Idx::new(data.get_u64_le());
        let born_round = data.get_u32_le();
        let data_len = data.get_u16_le() as usize;

        cell_bytes += data_len as u64;
        inserts.push(NurseryEntryRecord {
            hash: HashBytes(hash),
            idx,
            born_round,
            data: data.copy_to_bytes(data_len),
        });
    }

    let mut removes = Vec::with_capacity(remove_count);
    for _ in 0..remove_count {
        let mut hash = [0; HASH_LEN];
        data.copy_to_slice(&mut hash);
        removes.push(HashBytes(hash));
    }

    let metadata_bytes = estimate_total_metadata_len(insert_count as _, remove_count as _);
    let stats = RecordStats {
        payload_bytes,
        metadata_bytes,
        cell_bytes,
        insert_ops: insert_count as u64,
        remove_ops: remove_count as u64,
    };

    Ok(DecodedRecord {
        id,
        hash: payload_hash,
        delta: NurseryDelta { inserts, removes },
        stats,
    })
}

fn estimate_total_metadata_len(insert_count: u64, remove_count: u64) -> u64 {
    PAYLOAD_COUNTS_LEN + insert_count * INSERT_META_LEN + remove_count * REMOVE_META_LEN
}

const PREFIX_LEN: u64 = HASH_LEN as u64 + 8;
const PAYLOAD_COUNTS_LEN: u64 = 8 + 4 + 4;
const INSERT_META_LEN: u64 = HASH_LEN as u64 + 8 + 4 + 2;
const REMOVE_META_LEN: u64 = HASH_LEN as u64;

// === Stats ===

#[derive(Debug, Clone, Copy, Default)]
pub struct RecordStats {
    pub payload_bytes: u64,
    pub metadata_bytes: u64,
    pub cell_bytes: u64,
    pub insert_ops: u64,
    pub remove_ops: u64,
}

impl RecordStats {
    pub fn record_wal_write_metrics(self) {
        metrics::counter!("tycho_storage_cell_nursery_wal_cell_bytes").increment(self.cell_bytes);
        metrics::counter!("tycho_storage_cell_nursery_wal_insert_ops").increment(self.insert_ops);
        metrics::counter!("tycho_storage_cell_nursery_wal_remove_ops").increment(self.remove_ops);
    }
}
