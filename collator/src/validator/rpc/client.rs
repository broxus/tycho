use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use backon::ExponentialBuilder;
use serde::{Deserialize, Serialize};
use tycho_network::{Network, PeerId, PrivateOverlay, Request, Response};
use tycho_util::serde_helpers;

use crate::validator::proto::{self, rpc};

// TODO: Add jitter as well?
#[derive(Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ExchangeSignaturesBackoff {
    #[serde(with = "serde_helpers::humantime")]
    pub min_interval: Duration,
    #[serde(with = "serde_helpers::humantime")]
    pub max_interval: Duration,
    pub factor: f32,
    pub max_attempts: usize,
}

impl Default for ExchangeSignaturesBackoff {
    fn default() -> Self {
        Self {
            min_interval: Duration::from_millis(50),
            max_interval: Duration::from_secs(1),
            factor: 2.0,
            max_attempts: usize::MAX,
        }
    }
}

pub struct ValidatorClientBuilder<MandatoryFields = (Network, PrivateOverlay)> {
    mandatory: MandatoryFields,
    backoff: ExchangeSignaturesBackoff,
}

impl ValidatorClientBuilder {
    pub fn build(self) -> ValidatorClient {
        let (network, overlay) = self.mandatory;

        ValidatorClient {
            inner: Arc::new(Inner {
                network,
                overlay,
                backoff: ExponentialBuilder::default()
                    .with_min_delay(self.backoff.min_interval)
                    .with_max_delay(self.backoff.max_interval)
                    .with_factor(self.backoff.factor)
                    .with_max_times(self.backoff.max_attempts),
            }),
        }
    }
}

impl<T> ValidatorClientBuilder<T> {
    pub fn with_backoff(mut self, backoff: ExchangeSignaturesBackoff) -> Self {
        self.backoff = backoff;
        self
    }
}

impl<T1> ValidatorClientBuilder<(T1, ())> {
    pub fn with_overlay(
        self,
        overlay: PrivateOverlay,
    ) -> ValidatorClientBuilder<(T1, PrivateOverlay)> {
        let (network, _) = self.mandatory;

        ValidatorClientBuilder {
            mandatory: (network, overlay),
            backoff: self.backoff,
        }
    }
}

impl<T2> ValidatorClientBuilder<((), T2)> {
    pub fn with_network(self, network: Network) -> ValidatorClientBuilder<(Network, T2)> {
        let (_, overlay) = self.mandatory;

        ValidatorClientBuilder {
            mandatory: (network, overlay),
            backoff: self.backoff,
        }
    }
}

#[derive(Clone)]
#[repr(transparent)]
pub struct ValidatorClient {
    inner: Arc<Inner>,
}

impl ValidatorClient {
    pub fn builder() -> ValidatorClientBuilder<((), ())> {
        ValidatorClientBuilder {
            mandatory: ((), ()),
            backoff: ExchangeSignaturesBackoff::default(),
        }
    }

    pub fn network(&self) -> &Network {
        &self.inner.network
    }

    pub fn overlay(&self) -> &PrivateOverlay {
        &self.inner.overlay
    }

    pub fn peer_id(&self) -> &PeerId {
        self.inner.network.peer_id()
    }

    pub async fn exchange_signatures(
        &self,
        peer_id: &PeerId,
        block_seqno: u32,
        signature: &[u8; 64],
    ) -> Result<Vec<proto::PeerSignatureOwned>> {
        let req = Request::from_tl(rpc::ExchangeSignaturesRef {
            block_seqno,
            signature,
        });

        let res = self.query(peer_id, req).await?;

        todo!()
    }

    // pub async fn query_with_retry<A>(&self, peer_id: &PeerId, req: &Request) -> Result<A>
    // where
    //     for<'a> A: tl_proto::TlRead<'a>,
    // {
    //     let mut backoff = self.inner.backoff.build();

    //     loop {
    //         self.query(peer_id, req.clone())
    //     }
    // }

    pub async fn query(self, peer_id: &PeerId, req: Request) -> Result<Response> {
        self.inner
            .overlay
            .query(&self.inner.network, peer_id, req)
            .await
    }
}

struct Inner {
    network: Network,
    overlay: PrivateOverlay,
    backoff: ExponentialBuilder,
}
