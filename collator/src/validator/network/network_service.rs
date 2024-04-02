use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use futures_util::future::{self, FutureExt, Ready};
use tracing::error;

use tycho_network::{Response, Service, ServiceRequest};
use tycho_network::__internal::tl_proto::{TlRead, TlWrite};

use crate::{
    state_node::StateNodeAdapter,
    utils::async_queued_dispatcher::AsyncQueuedDispatcher,
    validator::state::ValidationState,
    validator::validator_processor::{ValidatorProcessor, ValidatorTaskResult},
};
use crate::validator::network::dto::SignaturesQuery;
use crate::validator::network::handlers::handle_signatures_query;

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
    W: ValidatorProcessor<ST, VS> + Send + Sync,
    ST: StateNodeAdapter + Send + Sync,
    VS: ValidationState + Send + Sync,
{
    pub fn new(dispatcher: Arc<AsyncQueuedDispatcher<W, ValidatorTaskResult>>) -> Self {
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
    W: ValidatorProcessor<ST, VS> + Send + Sync,
    ST: StateNodeAdapter + Send + Sync,
    VS: ValidationState + Send + Sync,
{
    type QueryResponse = Response;
    type OnQueryFuture = Pin<Box<dyn Future<Output = Option<Self::QueryResponse>> + Send>>;
    type OnMessageFuture = Ready<()>;
    type OnDatagramFuture = Ready<()>;

    fn on_query(&self, req: ServiceRequest) -> Self::OnQueryFuture {
        let query_result = req.parse_tl();

        let dispatcher = Arc::clone(&self.dispatcher);

        async move {
            match query_result {
                Ok(query) => match query {
                    SignaturesQuery {
                        session_seqno,
                        block_id_short,
                        signatures,
                    } => {
                        match handle_signatures_query(
                            &dispatcher,
                            session_seqno,
                            block_id_short,
                            signatures,
                        )
                        .await
                        {
                            Ok(response_option) => response_option,
                            Err(e) => {
                                error!("Error handling signatures query: {:?}", e);
                                panic!("Error handling signatures query: {:?}", e);
                            }
                        }
                    }
                },
                Err(e) => {
                    error!("Error parsing query: {:?}", e);
                    None
                }
            }
        }
        .boxed()
    }

    fn on_message(&self, _: ServiceRequest) -> Self::OnMessageFuture {
        future::ready(())
    }

    fn on_datagram(&self, _: ServiceRequest) -> Self::OnDatagramFuture {
        future::ready(())
    }
}
