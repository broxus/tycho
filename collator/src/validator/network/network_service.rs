use crate::method_to_async_task_closure;
use crate::state_node::{StateNodeAdapter, StateNodeAdapterStdImpl};
use crate::types::BlockCandidate;
use crate::utils::async_queued_dispatcher::AsyncQueuedDispatcher;
use crate::validator::state::{ValidationState, ValidationStateStdImpl};
use crate::validator::types::BlockValidationCandidate;
use crate::validator::validator_processor::{
    ValidatorProcessor, ValidatorProcessorStdImpl, ValidatorTaskResult,
};
use everscale_crypto::ed25519::PublicKey;
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, Signature};
use futures_util::FutureExt;
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::sync::RwLock;
use tracing::debug;
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
    _marker: PhantomData<(ST, VS)>, // Use PhantomData to satisfy the compiler for unused type parameters
}

impl<W, ST, VS> NetworkService<W, ST, VS>
where
    W: ValidatorProcessor<ST, VS> + 'static + Send + Sync,
    ST: StateNodeAdapter + 'static + Send + Sync,
    VS: ValidationState + 'static + Send + Sync,
{
    pub(crate) fn new(dispatcher: Arc<AsyncQueuedDispatcher<W, ValidatorTaskResult>>) -> Self {
        debug!("NETWORK SERVICE CREATED");
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
    // type OnQueryFuture = Pin<Box<dyn Future<Output = Option<Self::QueryResponse>> + Send>>;
    type OnQueryFuture = futures_util::future::Ready<Option<Self::QueryResponse>>;

    type OnMessageFuture = futures_util::future::Ready<()>;
    type OnDatagramFuture = futures_util::future::Ready<()>;

    fn on_query(&self, req: ServiceRequest) -> Self::OnQueryFuture {
        let dispatcher = self.dispatcher.clone(); // Clone the dispatcher

        tracing::debug!("query received");

        let response = SignaturesQuery {
            session_seqno: 1,
            block_validation_candidate: BlockId::default().into(),
            signatures: vec![],
        };
        debug!("SEND RESPONSE {:?}", response);

        futures_util::future::ready(Some(Response::from_tl(response)))

            // async {
        //     Some(Response::from_tl(response))
        // }.boxed()

        // async move {
        //     match req.parse_tl() {
        //         Ok(SignaturesQuery {
        //             session_seqno,
        //             block_validation_candidate,
        //             signatures,
        //         }) => {
        //             tracing::debug!("queue normal received");
        //             // Await the receiver to get the Result it contains
        //             match dispatcher
        //                 .enqueue_task_with_responder(method_to_async_task_closure!(
        //                     get_block_signatures,
        //                     session_seqno,
        //                     block_validation_candidate
        //                 ))
        //                 .await
        //             {
        //                 Ok(receiver) => {
        //                     // Now await the receiver to get the actual result
        //                     match receiver.await {
        //                         Ok(Ok(ValidatorTaskResult::Signatures(opt_signatures))) => {
        //                             match opt_signatures {
        //                                 None => {
        //                                     tracing::debug!("no signatures found");
        //                                     let response = SignaturesQuery {
        //                                         session_seqno,
        //                                         block_validation_candidate,
        //                                         signatures: vec![],
        //                                     };
        //                                     debug!("SEND RESPONSE {:?}", response);
        //
        //                                     Some(Response::from_tl(response))
        //                                 },
        //                                 Some(signatures_map) => {
        //                                     // let signatures = signatures_map
        //                                     //     .into_iter()
        //                                     //     .map(|(k, v)| (k.0, v.0))
        //                                     //     .collect();
        //                                     // let response = SignaturesQuery {
        //                                     //     session_seqno,
        //                                     //     block_validation_candidate,
        //                                     //     signatures,
        //                                     // };
        //                                     let response = SignaturesQuery {
        //                                         session_seqno,
        //                                         block_validation_candidate,
        //                                         signatures: vec![],
        //                                     };
        //                                     debug!("SEND RESPONSE {:?}", response);
        //
        //                                     Some(Response::from_tl(response))
        //                                 }
        //                             }
        //                         }
        //                         Ok(Ok(ValidatorTaskResult::Void)) | Ok(Err(_)) => {
        //                             debug!("ERRRRRRRR0");
        //                             None
        //
        //                         }, // Handle other cases or errors
        //                         Err(_) => {
        //                             tracing::error!(
        //                                 peer_id = %req.metadata.peer_id,
        //                                 addr = %req.metadata.remote_address,
        //                                 "Receiver error: dropped without sending",
        //                             );
        //                             None
        //                         }
        //                     }
        //                 }
        //                 Err(e) => {
        //                     tracing::error!(
        //                         peer_id = %req.metadata.peer_id,
        //                         addr = %req.metadata.remote_address,
        //                         "Task enqueue error: {e:?}",
        //                     );
        //                     None
        //                 }
        //             }
        //         }
        //         Err(e) => {
        //             debug!("ERRRRRRRR3");
        //             tracing::error!(
        //                 peer_id = %req.metadata.peer_id,
        //                 addr = %req.metadata.remote_address,
        //                 "Invalid request: {e:?}",
        //             );
        //             None
        //         }
        //     }
        // }
        // .boxed()
    }

    #[inline]
    fn on_message(&self, _req: ServiceRequest) -> Self::OnMessageFuture {
        debug!("QUERY RECEIVED2");

        futures_util::future::ready(())
    }

    #[inline]
    fn on_datagram(&self, _req: ServiceRequest) -> Self::OnDatagramFuture {
        debug!("QUERY RECEIVED3");

        futures_util::future::ready(())
    }
}

#[derive(Debug, Clone, TlRead, TlWrite)]
#[tl(boxed, id = 0x11112222)]
pub struct SignaturesQuery {
    pub session_seqno: u32,
    pub block_validation_candidate: BlockValidationCandidate,
    pub signatures: Vec<([u8; 32], [u8; 64])>,
}

impl SignaturesQuery {
    pub(crate) fn create(
        session_seqno: u32,
        block_validation_candidate: BlockId,
        current_signatures: Option<&HashMap<HashBytes, Signature>>,
    ) -> Self {
        let signatures = match current_signatures {
            None => vec![],
            Some(signatures_map) => signatures_map
                .into_iter()
                .map(|(k, v)| (k.0, v.0))
                .collect(),
        };
        Self {
            session_seqno,
            block_validation_candidate: block_validation_candidate.into(),
            signatures,
        }
    }
}

static PRIVATE_OVERLAY_ID: OverlayId = OverlayId([0; 32]);
