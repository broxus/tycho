use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockIdShort, Signature};
use tl_proto::{TlRead, TlWrite};
use tycho_network::util::tl;

#[derive(Debug, Clone, TlRead, TlWrite)]
#[tl(boxed, id = "queries.signaturesQueryRes", scheme = "proto.tl")]
pub struct SignaturesQueryResponse {
    pub session_seqno: u32,
    #[tl(with = "tl_block_id_short")]
    pub block_id_short: BlockIdShort,
    #[tl(with = "tl::VecWithMaxLen::<150>")]
    pub signatures: Vec<([u8; 32], [u8; 64])>,
}

impl SignaturesQueryResponse {
    pub fn new(
        session_seqno: u32,
        block_id_short: BlockIdShort,
        signatures: Vec<(HashBytes, Signature)>,
    ) -> Self {
        Self {
            session_seqno,
            block_id_short,
            signatures: signatures
                .into_iter()
                .map(|(hash, signature)| (hash.0, signature.0))
                .collect(),
        }
    }

    pub fn wrapped_signatures(&self) -> Vec<(HashBytes, Signature)> {
        self.signatures
            .iter()
            .map(|(hash, signature)| (HashBytes(*hash), Signature(*signature)))
            .collect()
    }
}

#[derive(Debug, Clone, TlRead, TlWrite)]
#[tl(boxed, id = "queries.signaturesQueryReq", scheme = "proto.tl")]
pub struct SignaturesQueryRequest {
    pub session_seqno: u32,
    #[tl(with = "tl_block_id_short")]
    pub block_id_short: BlockIdShort,
    pub signature: [u8; 64],
}

impl SignaturesQueryRequest {
    pub fn new(session_seqno: u32, block_id_short: BlockIdShort, signature: Signature) -> Self {
        Self {
            session_seqno,
            block_id_short,
            signature: signature.0,
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
