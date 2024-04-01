use crate::method_to_async_task_closure;
use crate::state_node::{StateNodeAdapter, StateNodeAdapterStdImpl};
use crate::types::BlockCandidate;
use crate::utils::async_queued_dispatcher::AsyncQueuedDispatcher;
use crate::validator::state::{ValidationState, ValidationStateStdImpl};
use crate::validator::types::BlockValidationCandidate;
use crate::validator::validator_processor::{
    ValidatorProcessor, ValidatorProcessorStdImpl, ValidatorTaskResult,
};
use anyhow::anyhow;
use everscale_crypto::ed25519::PublicKey;
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, BlockIdShort, Signature};
use futures_util::FutureExt;
use log::error;
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::sync::RwLock;
use tracing::debug;
use tracing::field::debug;
use tycho_network::__internal::tl_proto::{TlRead, TlWrite};
use tycho_network::{Response, Service, ServiceRequest};

#[derive(Clone)]
pub struct NetworkService<W, ST, VS>
where
    W: ValidatorProcessor<ST, VS> + Send + Sync,
    ST: StateNodeAdapter + Send + Sync,
    VS: ValidationState + Send + Sync,
{
    dispatcher: Arc<AsyncQueuedDispatcher<W, ValidatorTaskResult>>,
    _marker: PhantomData<(ST, VS)>,
}

impl<W, ST, VS> NetworkService<W, ST, VS>
where
    W: ValidatorProcessor<ST, VS> + 'static + Send + Sync,
    ST: StateNodeAdapter + 'static + Send + Sync,
    VS: ValidationState + 'static + Send + Sync,
{
    pub(crate) fn new(dispatcher: Arc<AsyncQueuedDispatcher<W, ValidatorTaskResult>>) -> Self {
        Self {
            dispatcher,
            _marker: Default::default(),
        }
    }
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, TlRead, TlWrite)]
#[repr(transparent)]
pub struct OverlayId(pub [u8; 32]);

impl<W, ST, VS> Service<ServiceRequest> for NetworkService<W, ST, VS>
where
    W: ValidatorProcessor<ST, VS> + 'static + Send + Sync,
    ST: StateNodeAdapter + 'static + Send + Sync,
    VS: ValidationState + 'static + Send + Sync,
{
    type QueryResponse = Response;
    type OnQueryFuture = Pin<Box<dyn Future<Output = Option<Self::QueryResponse>> + Send>>;
    type OnMessageFuture = futures_util::future::Ready<()>;
    type OnDatagramFuture = futures_util::future::Ready<()>;

    fn on_query(&self, req: ServiceRequest) -> Self::OnQueryFuture {
        let dispatcher = self.dispatcher.clone(); // Clone the dispatcher

        async move {
            match req.parse_tl() {
                Ok(SignaturesQuery {
                    session_seqno,
                       block_id_short: block_header,
                    signatures,
                }) => {

                    dispatcher.enqueue_task(method_to_async_task_closure!(
                        process_candidate_signature_response,
                        session_seqno,
                        block_header,
                        signatures
                    )).await.expect("Failed to add signatures");

                    let mut receiver = dispatcher
                        .enqueue_task_with_responder(method_to_async_task_closure!(
                            get_block_signatures,
                            session_seqno,
                            &block_header
                        ))
                        .await;

                    let receiver = match receiver {
                        Ok(receiver) => receiver.await,
                        Err(e) => {
                            // error!("Error getting receiver: {:?}", e);
                            panic!("Error getting receiver: {:?}", e)
                        }
                    };

                    match receiver {
                        Ok(mut receiver) => {
                            // Now await the receiver to get the actual result
                            match receiver {
                                Ok(ValidatorTaskResult::Signatures(signatures)) => {
                                    debug!("got signatures");

                                    let signatures =
                                        signatures.into_iter().map(|(k, v)| (k.0, v.0)).collect();
                                    let response = SignaturesQuery {
                                        session_seqno,
                                        block_id_short: block_header,
                                        signatures,
                                    };
                                    Some(Response::from_tl(response))
                                }
                                Ok(ValidatorTaskResult::Void)
                                | Ok(ValidatorTaskResult::ValidationStatus(_)) => {
                                    tracing::error!(
                                        peer_id = %req.metadata.peer_id,
                                        addr = %req.metadata.remote_address,
                                        "Invalid response from get_block_signatures",
                                    );
                                    None
                                } // Handle other cases or errors
                                Err(e) => {
                                    tracing::error!(
                                        peer_id = %req.metadata.peer_id,
                                        addr = %req.metadata.remote_address,
                                        err = ?e,
                                        "Receiver error: dropped without sending",
                                    );
                                    None
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                peer_id = %req.metadata.peer_id,
                                addr = %req.metadata.remote_address,
                                "Task enqueue error: {e:?}",
                            );
                            None
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(
                        peer_id = %req.metadata.peer_id,
                        addr = %req.metadata.remote_address,
                        "Invalid request: {e:?}",
                    );
                    None
                }
            }
        }
        .boxed()
    }

    #[inline]
    fn on_message(&self, _req: ServiceRequest) -> Self::OnMessageFuture {
        futures_util::future::ready(())
    }

    #[inline]
    fn on_datagram(&self, _req: ServiceRequest) -> Self::OnDatagramFuture {
        futures_util::future::ready(())
    }
}

#[derive(Debug, Clone, TlRead, TlWrite)]
#[tl(boxed, id = 0x11112222)]
pub struct SignaturesQuery {
    pub session_seqno: u32,
    #[tl(with = "tl_block_id_short")]
    pub block_id_short: BlockIdShort,
    pub signatures: Vec<([u8; 32], [u8; 64])>,
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
            Some(shard) => shard
        };

        Ok(BlockIdShort { shard, seqno })
    }
}

impl SignaturesQuery {
    pub(crate) fn create(
        session_seqno: u32,
        block_header: BlockIdShort,
        current_signatures: &HashMap<HashBytes, Signature>,
    ) -> Self {
        let signatures = current_signatures
            .into_iter()
            .map(|(k, v)| (k.0, v.0))
            .collect();
        Self {
            session_seqno,
            block_id_short: block_header,
            signatures,
        }
    }
}
