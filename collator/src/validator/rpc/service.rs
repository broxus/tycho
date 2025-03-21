use std::sync::{Arc, Weak};

use everscale_types::models::ShardIdent;
use tracing::Instrument;
use tycho_network::{try_handle_prefix, PeerId, Response, Service, ServiceRequest};
use tycho_util::futures::BoxFutureOrNoop;

use crate::tracing_targets;
use crate::validator::proto::rpc;
use crate::validator::rpc::ExchangeSignatures;
use crate::validator::ValidationSessionId;

pub struct ValidatorService<E> {
    pub shard_ident: ShardIdent,
    pub session_id: ValidationSessionId,
    pub exchanger: Weak<E>,
}

impl<E: ExchangeSignatures> ValidatorService<E> {
    fn handle_exchange_signatures(
        &self,
        peer_id: &PeerId,
        block_seqno: u32,
        signature: Arc<[u8; 64]>,
    ) -> BoxFutureOrNoop<Option<Response>> {
        let Some(exchanger) = self.exchanger.upgrade() else {
            return BoxFutureOrNoop::Noop;
        };

        let peer_id = *peer_id;
        BoxFutureOrNoop::future(
            async move {
                match exchanger
                    .exchange_signatures(&peer_id, block_seqno, signature)
                    .await
                {
                    Ok(res) => Some(Response::from_tl(res)),
                    Err(e) => {
                        // and the log will be full of these warnings.
                        tracing::debug!(
                            target: tracing_targets::VALIDATOR,
                            %peer_id,
                            block_seqno,
                            "failed to exchange signatures: {e:?}",
                        );
                        None
                    }
                }
            }
            .instrument(tracing::Span::current()),
        )
    }
}

impl<E> Clone for ValidatorService<E> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            shard_ident: self.shard_ident,
            session_id: self.session_id,
            exchanger: self.exchanger.clone(),
        }
    }
}

impl<E> Service<ServiceRequest> for ValidatorService<E>
where
    E: ExchangeSignatures,
{
    type QueryResponse = Response;
    type OnQueryFuture = BoxFutureOrNoop<Option<Self::QueryResponse>>;
    type OnMessageFuture = futures_util::future::Ready<()>;
    type OnDatagramFuture = futures_util::future::Ready<()>;

    #[tracing::instrument(
        name = "on_validator_query",
        skip_all,
        fields(
            shard_ident = %self.shard_ident,
            session_id = ?self.session_id,
        )
    )]
    fn on_query(&self, req: ServiceRequest) -> Self::OnQueryFuture {
        let (constructor, body) = match try_handle_prefix(&req) {
            Ok(rest) => rest,
            Err(e) => {
                tracing::debug!(
                    target: tracing_targets::VALIDATOR,
                    "failed to deserializer query: {e}",
                );
                return BoxFutureOrNoop::Noop;
            }
        };

        tycho_network::match_tl_request!(body, tag = constructor, {
            rpc::ExchangeSignaturesOwned as r => {
                tracing::trace!(
                    target: tracing_targets::VALIDATOR,
                    block_seqno = r.block_seqno,
                    "exchangeSignatures",
                );
                self.handle_exchange_signatures(&req.metadata.peer_id, r.block_seqno, r.signature)
            }
        }, e => {
            tracing::debug!(
                target: tracing_targets::VALIDATOR,
                "failed to deserializer query: {e}",
            );
            BoxFutureOrNoop::Noop
        })
    }

    #[inline]
    fn on_message(&self, _: ServiceRequest) -> Self::OnMessageFuture {
        futures_util::future::ready(())
    }

    #[inline]
    fn on_datagram(&self, _: ServiceRequest) -> Self::OnDatagramFuture {
        futures_util::future::ready(())
    }
}
