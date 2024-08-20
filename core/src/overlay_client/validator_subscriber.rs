use std::sync::Arc;
use std::time::Duration;

use everscale_types::models::ValidatorSet;
use futures_util::future::BoxFuture;
use tokio::sync::mpsc::Sender;
use tycho_network::{PeerId, PeerResolver};

use crate::block_strider::{BlockSubscriber, BlockSubscriberContext};
use crate::overlay_client::{Neighbour, Neighbours};

struct Inner {
    vset: Neighbours,
}

#[derive(Clone)]
pub struct ValidatorSubscriber {
    inner: Arc<Inner>,
}

impl ValidatorSubscriber {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                vset: Neighbours::new(Vec::new(), u16::MAX as usize),
            }),
        }
    }

    pub fn get_current_validator_set(&self) -> &Neighbours {
        &self.inner.vset
    }
    pub async fn update_current_validator_set(&self, vset: ValidatorSet) {
        let mut validators = Vec::new();
        for v in vset.list {
            validators.push(Neighbour::new(
                PeerId(v.public_key.0),
                vset.utime_until,
                &Duration::from_secs(1),
            ));
        }

        self.inner.vset.update(validators).await
    }
}

impl BlockSubscriber for ValidatorSubscriber {
    type Prepared = ();
    type PrepareBlockFut<'a> = futures_util::future::Ready<anyhow::Result<()>>;
    type HandleBlockFut<'a> = BoxFuture<'a, anyhow::Result<()>>;

    fn prepare_block<'a>(&'a self, _: &'a BlockSubscriberContext) -> Self::PrepareBlockFut<'a> {
        futures_util::future::ready(Ok(()))
    }

    fn handle_block<'a>(
        &'a self,
        cx: &'a BlockSubscriberContext,
        _: Self::Prepared,
    ) -> Self::HandleBlockFut<'a> {
        if cx.is_key_block {
            let config = match cx.block.load_custom() {
                Ok(extra) => extra.config,
                Err(e) => {
                    tracing::error!("Failed to load mc_extra. {e:?}");
                    None
                }
            };

            let Some(config) = config else {
                tracing::error!("Key block does not contain blockchain config");
                return Box::pin(futures_util::future::ready(Ok(())));
            };

            let future = async move {
                match config.get_current_validator_set() {
                    Ok(vset) => self.update_current_validator_set(vset).await,
                    Err(e) => {
                        tracing::error!(
                            "Failed to get validator set from blockchain config. {e:?}"
                        );
                    }
                }
                Ok(())
            };

            Box::pin(future)
        } else {
            return Box::pin(futures_util::future::ready(Ok(())));
        }
    }
}
