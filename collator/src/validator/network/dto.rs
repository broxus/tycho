use std::collections::HashMap;

use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockIdShort, Signature};
use tl_proto::{TlRead, TlWrite};

#[derive(Debug, Clone, TlRead, TlWrite)]
#[tl(boxed, id = 0x11112222)]
pub struct SignaturesQuery {
    pub session_seqno: u32,
    #[tl(with = "tl_block_id_short")]
    pub block_id_short: BlockIdShort,
    pub signatures: Vec<([u8; 32], [u8; 64])>,
}

impl SignaturesQuery {
    pub(crate) fn create(
        session_seqno: u32,
        block_header: BlockIdShort,
        current_signatures: &HashMap<HashBytes, Signature>,
    ) -> Self {
        let signatures = current_signatures.iter().map(|(k, v)| (k.0, v.0)).collect();
        Self {
            session_seqno,
            block_id_short: block_header,
            signatures,
        }
    }
}

mod tl_block_id_short {
    use everscale_types::models::{BlockIdShort, ShardIdent};
    use tl_proto::{TlPacket, TlRead, TlResult, TlWrite};

    pub const fn size_hint(_: &BlockIdShort) -> usize {
        16
    }

    pub fn write<P: TlPacket>(block_id: &BlockIdShort, packet: &mut P) {
        block_id.shard.workchain().write_to(packet);
        block_id.shard.prefix().write_to(packet);
        block_id.seqno.write_to(packet);
    }

    pub fn read(packet: &[u8], offset: &mut usize) -> TlResult<BlockIdShort> {
        let workchain = i32::read_from(packet, offset)?;
        let prefix = u64::read_from(packet, offset)?;
        let seqno = u32::read_from(packet, offset)?;

        let shard = ShardIdent::new(workchain, prefix);

        let shard = match shard {
            None => return Err(tl_proto::TlError::InvalidData),
            Some(shard) => shard,
        };

        Ok(BlockIdShort { shard, seqno })
    }
}
