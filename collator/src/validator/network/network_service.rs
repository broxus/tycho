use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use futures_util::future::{self, FutureExt, Ready};
use tracing::error;

use tycho_network::__internal::tl_proto::{TlRead, TlWrite};
use tycho_network::{Response, Service, ServiceRequest};

use crate::validator::network::dto::SignaturesQuery;
use crate::validator::network::handlers::handle_signatures_query;
use crate::{
    state_node::StateNodeAdapter,
    utils::async_queued_dispatcher::AsyncQueuedDispatcher,
    validator::validator_processor::{ValidatorProcessor, ValidatorTaskResult},
};

#[derive(Clone)]
pub struct NetworkService<W, ST>
where
    W: ValidatorProcessor<ST> + Send + Sync,
    ST: StateNodeAdapter + Send + Sync,
{
    dispatcher: Arc<AsyncQueuedDispatcher<W, ValidatorTaskResult>>,
    _marker: PhantomData<ST>,
}

impl<W, ST> NetworkService<W, ST>
where
    W: ValidatorProcessor<ST> + Send + Sync,
    ST: StateNodeAdapter + Send + Sync,
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

impl<W, ST> Service<ServiceRequest> for NetworkService<W, ST>
where
    W: ValidatorProcessor<ST> + Send + Sync,
    ST: StateNodeAdapter + Send + Sync,
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
                Ok(query) => {
                    let SignaturesQuery {
                        session_seqno,
                        block_id_short,
                        signatures,
                    } = query;
                    {
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
                }
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
