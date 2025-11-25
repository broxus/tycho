use std::sync::Arc;

use anyhow::{Context, Result};
use arc_swap::ArcSwapOption;
use futures_util::future::BoxFuture;
use tycho_core::block_strider::{StateSubscriber, StateSubscriberContext};
use tycho_crypto::ed25519;
use tycho_slasher_traits::ValidatorEventsListener;

pub use self::bc::{
    BlocksBatch, ContractSubscription, EncodeBlocksBatchMessage, SignatureHistory, SignedMessage,
    SlasherContract,
};
use self::collector::ValidatorEventsCollector;

pub mod collector {
    pub use self::validator_events::*;

    mod validator_events;
    // TODO: mod mempool_events;
}

mod bc;
mod util;

pub struct SlasherParams {
    pub node_keys: Arc<ed25519::KeyPair>,
    pub initial_mc_seqno: u32,
}

// NOTE: Stub
#[derive(Clone)]
#[repr(transparent)]
pub struct Slasher {
    inner: Arc<Inner>,
}

impl Slasher {
    pub fn new<C: SlasherContract>(node_keys: Arc<ed25519::KeyPair>, contract: C) -> Self {
        let collector = Arc::new(ValidatorEventsCollector::default());

        Self {
            inner: Arc::new(Inner {
                node_keys,
                validator_events_collector: collector,
                contract: Box::new(contract),
                subscription: ArcSwapOption::empty(),
            }),
        }
    }

    pub fn validator_events_listener(&self) -> Arc<dyn ValidatorEventsListener> {
        self.inner.validator_events_collector.clone()
    }

    async fn handle_state_impl(&self, cx: &StateSubscriberContext) -> Result<()> {
        if !cx.block.id().is_masterchain() {
            return Ok(());
        }

        let this = self.inner.as_ref();

        // Check config updates
        let config_params = cx.state.config_params()?;
        let Some(slasher_address) = this
            .contract
            .find_account_address(&config_params)
            .context("failed to find contract address")?
            .filter(|addr| addr.is_masterchain())
        else {
            return Ok(());
        };

        let subscription = match this.subscription.load_full() {
            Some(s) if s.address() == &slasher_address => s,
            // TODO: Use `ArcSwap::compare_and_swap`?
            _ => {
                let s = Arc::new(ContractSubscription::new(&slasher_address));
                this.subscription.store(Some(s.clone()));
                s
            }
        };

        let extra = cx.block.load_extra()?.account_blocks.load()?;
        if let Some((_, account_block)) = extra.get(&slasher_address.address)? {
            subscription.handle_account_transactions(&account_block)?;
        }

        Ok(())
    }
}

impl StateSubscriber for Slasher {
    type HandleStateFut<'a> = BoxFuture<'a, Result<()>>;

    #[inline]
    fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a> {
        Box::pin(self.handle_state_impl(cx))
    }
}

struct Inner {
    #[allow(unused)]
    node_keys: Arc<ed25519::KeyPair>,
    validator_events_collector: Arc<ValidatorEventsCollector>,
    contract: Box<dyn SlasherContract>,
    subscription: ArcSwapOption<ContractSubscription>,
}
