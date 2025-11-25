use std::num::NonZeroU32;

use anyhow::{Context, Result};
use tycho_types::cell::Lazy;
use tycho_types::dict;
use tycho_types::models::{
    AccountState, BlockchainConfigParams, ExtInMsgInfo, MsgInfo, OwnedMessage, StdAddr,
};
use tycho_types::prelude::*;

use super::{BlocksBatch, SignedMessage, SlasherContract};

pub struct StubContract;

impl SlasherContract for StubContract {
    fn find_account_address(&self, _config: &BlockchainConfigParams) -> Result<Option<StdAddr>> {
        Ok(None)
    }

    fn get_batch_size(&self, _state: &AccountState) -> Result<NonZeroU32> {
        Ok(NonZeroU32::new(100).unwrap())
    }

    fn encode_blocks_batch_message(
        &self,
        params: &super::EncodeBlocksBatchMessage<'_>,
    ) -> Result<SignedMessage> {
        let cell = CellBuilder::build_from(StoreBlocksBatch(params.batch))
            .context("failed to serialize blocks batch")?;

        let now = tycho_util::time::now_millis();

        let expire_at = (now / 1000).saturating_add(params.ttl.as_secs()) as u32;
        let message = Lazy::new(&OwnedMessage {
            info: MsgInfo::ExtIn(ExtInMsgInfo {
                // Stub address.
                dst: StdAddr::new(-1, HashBytes::ZERO).into(),
                ..Default::default()
            }),
            init: None,
            body: cell.into(),
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
