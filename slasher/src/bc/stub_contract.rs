use std::num::NonZeroU32;

use anyhow::{Context, Result};
use tycho_types::abi::extend_signature_with_id;
use tycho_types::cell::Lazy;
use tycho_types::dict;
use tycho_types::models::{
    AccountState, BlockchainConfigParams, ExtInMsgInfo, MsgInfo, OwnedMessage, StdAddr,
};
use tycho_types::prelude::*;

use super::{BlocksBatch, SignedMessage, SlasherContract};

const PARAM_IDX: u32 = 666;

pub struct StubSlasherContract;

impl SlasherContract for StubSlasherContract {
    fn find_account_address(&self, config: &BlockchainConfigParams) -> Result<Option<StdAddr>> {
        let Some(raw) = config.get_raw_cell_ref(PARAM_IDX)? else {
            return Ok(None);
        };
        let address = raw.parse::<HashBytes>()?;
        Ok(Some(StdAddr::new(-1, address)))
    }

    fn default_batch_size(&self) -> NonZeroU32 {
        NonZeroU32::new(10).unwrap()
    }

    fn get_batch_size(&self, _state: &AccountState) -> Result<NonZeroU32> {
        Ok(self.default_batch_size())
    }

    fn encode_blocks_batch_message(
        &self,
        params: &super::EncodeBlocksBatchMessage<'_>,
    ) -> Result<SignedMessage> {
        let cell = CellBuilder::build_from(StoreBlocksBatch(params.batch))
            .context("failed to serialize blocks batch")?;

        let now = tycho_util::time::now_millis();
        let expire_at = (now / 1000).saturating_add(params.ttl.as_secs()) as u32;
        let body_to_sign = {
            let mut b = CellBuilder::new();
            b.store_u64(now)?;
            b.store_u32(expire_at)?;
            b.store_u16(params.validator_idx)?;
            b.store_reference(cell)?;
            b.build()?
        };

        // TODO: Add support for signature id.
        let signature = params.keypair.sign_raw(&extend_signature_with_id(
            body_to_sign.repr_hash().as_array(),
            None,
        ));
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
}

struct StoreBlocksBatch<'a>(&'a BlocksBatch);

impl Store for StoreBlocksBatch<'_> {
    fn store_into(
        &self,
        builder: &mut CellBuilder,
        context: &dyn CellContext,
    ) -> Result<(), tycho_types::error::Error> {
        let batch = self.0;

        builder.store_u32(batch.start_seqno)?;
        batch.committed_blocks.store_into(builder, context)?;

        // A subset contains items in no particular order,
        // so we need to sort by them to simplify remapping to vset.
        let mut entries = batch
            .signatures_history
            .iter()
            .map(|item| (item.validator_idx, &item.bits))
            .collect::<Vec<_>>();
        entries.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));

        let Some(dict_root) = dict::build_dict_from_sorted_iter(entries, context)? else {
            // Subset must not be empty.
            return Err(tycho_types::error::Error::InvalidData);
        };
        builder.store_reference(dict_root)
    }
}
