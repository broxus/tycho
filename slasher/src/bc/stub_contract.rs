use std::num::{NonZeroU8, NonZeroU32};

use anyhow::{Context, Result, anyhow};
use tycho_slasher_traits::{AnchorPeerStats, ValidationSessionId};
use tycho_types::cell::Lazy;
use tycho_types::dict;
use tycho_types::models::{
    BlockchainConfigParams, ComputePhase, ExtInMsgInfo, Message, MsgInfo, OwnedMessage, TxInfo,
};
use tycho_types::prelude::*;

use super::{
    AnchorStatsHistory, BlocksBatch, SignatureHistory, SignedMessage, SlasherContract,
    SlasherContractEvent, SlasherParams, SubmitBlocksBatch,
};
use crate::util::BitSet;

/// ```tlb
/// slasher_params#01
///     address:bits256
///     blocks_batch_size:uint8
///     { blocks_batch_size > 0 }
///     = ConfigParam 666;
/// ```
#[derive(Debug, Store, Load)]
#[tlb(tag = "#01")]
pub struct StubSlasherParams {
    pub address: HashBytes,
    pub blocks_batch_size: NonZeroU8,
}

impl StubSlasherParams {
    pub const IDX: u32 = 666;
}

pub struct StubSlasherContract;

impl SlasherContract for StubSlasherContract {
    fn default_batch_size(&self) -> NonZeroU32 {
        NonZeroU32::new(10).unwrap()
    }

    fn find_params(&self, config: &BlockchainConfigParams) -> Result<Option<SlasherParams>> {
        let Some(raw) = config.get_raw_cell_ref(StubSlasherParams::IDX)? else {
            return Ok(None);
        };
        let params = raw.parse::<StubSlasherParams>()?;
        Ok(Some(SlasherParams {
            address: params.address,
            blocks_batch_size: params.blocks_batch_size.into(),
        }))
    }

    fn encode_blocks_batch_message(
        &self,
        params: &super::EncodeBlocksBatchMessage<'_>,
    ) -> Result<SignedMessage> {
        let cell = CellBuilder::build_from(BlocksBatchBc::wrap(params.batch))
            .context("failed to serialize blocks batch")?;

        let now = tycho_util::time::now_millis();
        let expire_at = (now / 1000).saturating_add(params.ttl.as_secs()) as u32;
        let body_to_sign = {
            let mut b = CellBuilder::new();
            b.store_u64(now)?;
            b.store_u32(expire_at)?;
            b.store_u32(params.session_id.catchain_seqno)?;
            b.store_u32(params.session_id.vset_switch_round)?;
            b.store_u16(params.validator_idx)?;
            b.store_reference(cell)?;
            b.build()?
        };

        let signature = params.keypair.sign_raw(
            &params
                .signature_context
                .apply(body_to_sign.repr_hash().as_array()),
        );
        let body = {
            let mut b = CellBuilder::new();
            b.store_raw(&signature, 512)?;
            b.store_slice(body_to_sign.as_slice()?)?;
            b.build()?
        };

        let message = Lazy::new(&OwnedMessage {
            info: MsgInfo::ExtIn(ExtInMsgInfo {
                dst: params.address.clone().into(),
                ..Default::default()
            }),
            init: None,
            body: body.into(),
            layout: None,
        })?;

        Ok(SignedMessage { message, expire_at })
    }

    fn decode_event(
        &self,
        tx: &tycho_types::models::Transaction,
    ) -> Result<Option<SlasherContractEvent>> {
        'check: {
            if let TxInfo::Ordinary(info) = tx.load_info()?
                && let ComputePhase::Executed(ph) = info.compute_phase
                && ph.exit_code == 0
            {
                break 'check;
            }
            return Ok(None);
        };

        let Some(in_msg) = &tx.in_msg else {
            return Ok(None);
        };
        let msg = in_msg.parse::<Message<'_>>()?;
        if !msg.info.is_external_in() {
            return Ok(None);
        }

        // TODO: Add message op
        let mut body = msg.body;
        body.skip_first(512 + 64 + 32, 0)?;
        let catchain_seqno = body.load_u32()?;
        let vset_switch_round = body.load_u32()?;
        let session_id = ValidationSessionId {
            vset_switch_round,
            catchain_seqno,
        };
        let validator_idx = body.load_u16()?;
        let mut batch_cs = body.load_reference_as_slice()?;
        let BlocksBatchBc(blocks_batch) = <_>::load_from(&mut batch_cs)?;
        if !body.is_empty() || !batch_cs.is_empty() {
            return Err(tycho_types::error::Error::CellOverflow.into());
        }

        Ok(Some(SlasherContractEvent::SubmitBlocksBatch(
            SubmitBlocksBatch {
                session_id,
                validator_idx,
                blocks_batch,
            },
        )))
    }
}

#[repr(transparent)]
struct BlocksBatchBc(BlocksBatch);

impl BlocksBatchBc {
    fn wrap(inner: &BlocksBatch) -> &Self {
        // SAFETY: `BlocksBatchBc` has the same layout as `BlocksBatch`.
        unsafe { &*(inner as *const BlocksBatch).cast::<Self>() }
    }
}

impl<'a> Load<'a> for BlocksBatchBc {
    fn load_from(slice: &mut CellSlice<'a>) -> Result<Self, tycho_types::error::Error> {
        let start_seqno = slice.load_u32()?;
        let anchor_range =
            Some(slice.load_u32()?..=slice.load_u32()?).filter(|range| *range.end() > 0);

        let block_count = slice.size_bits() as usize;
        let committed_blocks = BitSet::load_from_cs(block_count, slice)?;

        let mut zipped = Vec::new();

        let dict = Dict::<u16, CellSlice<'_>>::from_raw(Some(slice.load_reference_cloned()?));
        for entry in dict.iter() {
            let (validator_idx, mut cs) = entry?;
            let bits = BitSet::load_from_cs(block_count * 2, &mut cs)?;
            let points_proven = cs.load_u16()?;
            if !cs.is_empty() {
                return Err(tycho_types::error::Error::CellOverflow);
            }
            zipped.push((validator_idx, bits, points_proven));
        }

        let anchor_stats_history = zipped
            .iter()
            .map(|(validator_idx, _, points_proven)| AnchorStatsHistory {
                validator_idx: *validator_idx,
                stats: AnchorPeerStats {
                    points_proven: *points_proven,
                },
            })
            .collect::<Vec<_>>();

        let signatures_history = zipped
            .into_iter()
            .map(|(validator_idx, bits, _)| SignatureHistory {
                validator_idx,
                bits,
            })
            .collect::<Vec<_>>();

        Ok(Self(BlocksBatch {
            start_seqno,
            anchor_range,
            committed_blocks,
            signatures_history: signatures_history.into_boxed_slice(),
            anchor_stats_history: anchor_stats_history.into_boxed_slice(),
        }))
    }
}

impl Store for BlocksBatchBc {
    fn store_into(
        &self,
        builder: &mut CellBuilder,
        context: &dyn CellContext,
    ) -> Result<(), tycho_types::error::Error> {
        let batch = &self.0;

        builder.store_u32(batch.start_seqno)?;
        if let Some(anchor_range) = self.0.anchor_range.as_ref() {
            builder.store_u32(*anchor_range.start())?;
            builder.store_u32(*anchor_range.end())?;
        } else {
            builder.store_u32(0)?;
            builder.store_u32(0)?;
        };

        batch.committed_blocks.store_into(builder, context)?;

        // A subset contains items in no particular order,
        // so we need to sort by them to simplify remapping to vset.
        let mut signatures = (batch.signatures_history.iter()).collect::<Vec<_>>();
        signatures.sort_unstable_by_key(|a| a.validator_idx);

        assert_eq!(
            signatures.len(),
            batch.anchor_stats_history.len(),
            "anchor and signature stats must have the same length, but {} != {}",
            signatures.len(),
            batch.anchor_stats_history.len(),
        );

        let zipped = signatures
            .iter()
            .zip(&batch.anchor_stats_history)
            .map(|(s, a)| {
                if s.validator_idx == a.validator_idx {
                    Ok((s.validator_idx, (&s.bits, a.stats.points_proven)))
                } else {
                    Err(anyhow!(
                        "expected {} got {}",
                        s.validator_idx,
                        a.validator_idx
                    ))
                }
            })
            .collect::<Result<Vec<_>>>()
            .expect("anchor stats must share validator_idx with signature history");

        let Some(dict_root) = dict::build_dict_from_sorted_iter(zipped, context)? else {
            // Subset must not be empty.
            return Err(tycho_types::error::Error::InvalidData);
        };
        builder.store_reference(dict_root)
    }
}

#[cfg(test)]
mod tests {
    use tycho_slasher_traits::ReceivedSignature;

    use super::*;

    #[test]
    fn blocks_batch_cell() {
        let mut batch = BlocksBatch::new(230, NonZeroU32::new(10).unwrap(), &[5, 10, 12, 3]);

        for (seqno, signatures) in [
            (230, [
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
                ReceivedSignature(0),
                ReceivedSignature(ReceivedSignature::INVALID_SIGNATURE_BIT),
            ]),
            (231, [
                ReceivedSignature(0),
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
                ReceivedSignature(ReceivedSignature::INVALID_SIGNATURE_BIT),
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
            ]),
            (233, [
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
            ]),
            (234, [
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
            ]),
            (239, [
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
                ReceivedSignature(ReceivedSignature::VALID_SIGNATURE_BIT),
            ]),
        ] {
            let committed = batch.commit_signatures(seqno, &signatures);
            assert!(committed);
        }

        let cell = CellBuilder::build_from(BlocksBatchBc::wrap(&batch)).unwrap();
        println!("{}", Boc::encode_base64(cell));
    }
}
