use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::ShardIdent;
use tycho_network::{PeerId, Response, Service, ServiceRequest};
use tycho_util::futures::BoxFutureOrNoop;

use crate::validator::proto::rpc;
use crate::validator::rpc::ExchangeSignatures;

pub struct ValidatorService<E> {
    pub shard_ident: ShardIdent,
    pub session_id: u32,
    pub exchanger: E,
}

impl<E: ExchangeSignatures + Clone> ValidatorService<E> {
    fn handle_exchange_signatures(
        &self,
        peer_id: &PeerId,
        block_seqno: u32,
        signature: Arc<[u8; 64]>,
    ) -> BoxFutureOrNoop<Option<Response>> {
        let exchanger = self.exchanger.clone();
        let peer_id = *peer_id;
        BoxFutureOrNoop::future(async move {
            match exchanger
                .exchange_signatures(&peer_id, block_seqno, signature)
                .await
            {
                Ok(_iter) => {
                    todo!()
                }
                Err(e) => {
                    // TODO: Is it ok to WARN here? Since we can be ddosed with invalid signatures
                    // and the log will be full of these warnings.
                    tracing::warn!("failed to exchange signatures: {e}");
                    None
                }
            }
        })
    }
}

impl<E: Clone> Clone for ValidatorService<E> {
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
    E: ExchangeSignatures + Clone,
{
    type QueryResponse = Response;
    type OnQueryFuture = BoxFutureOrNoop<Option<Self::QueryResponse>>;
    type OnMessageFuture = futures_util::future::Ready<()>;
    type OnDatagramFuture = futures_util::future::Ready<()>;

    #[tracing::instrument(
        level = "debug",
        name = "on_validator_query",
        skip_all,
        fields(
            shard_ident = %self.shard_ident,
            session_id = self.session_id,
        )
    )]
    fn on_query(&self, req: ServiceRequest) -> Self::OnQueryFuture {
        let (constructor, body) = match try_handle_prefix(&req) {
            Ok(rest) => rest,
            Err(e) => {
                tracing::debug!("failed to deserializer query: {e}");
                return BoxFutureOrNoop::Noop;
            }
        };

        tycho_network::match_tl_request!(body, tag = constructor, {
            rpc::ExchangeSignaturesOwned as r => {
                tracing::debug!(
                    block_seqno = r.block_seqno,
                    "exchangeSignatures",
                );
                self.handle_exchange_signatures(&req.metadata.peer_id, r.block_seqno, r.signature)
            }
        }, e => {
            tracing::debug!("failed to deserializer query: {e}");
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

fn try_handle_prefix(req: &ServiceRequest) -> Result<(u32, &[u8]), tl_proto::TlError> {
    let body = req.as_ref();
    if body.len() < 4 {
        return Err(tl_proto::TlError::UnexpectedEof);
    }

    let constructor = std::convert::identity(body).get_u32_le();
    Ok((constructor, body))
}
