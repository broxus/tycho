use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use everscale_types::models::BlockIdShort;
use futures_util::future::{self, FutureExt, Ready};
use tokio::sync::broadcast::Sender;
use tycho_network::{Response, Service, ServiceRequest};

use crate::validator::network::dto::SignaturesQuery;
use crate::validator::network::handlers::handle_signatures_query;
use crate::validator::state::{ValidationState, ValidationStateStdImpl};
use crate::validator::ValidatorEventListener;

#[derive(Clone)]
pub struct NetworkService {
    listeners: Vec<Arc<dyn ValidatorEventListener>>,
    state: Arc<ValidationStateStdImpl>,
    block_validated_broadcaster: Sender<BlockIdShort>,
}

impl NetworkService {
    pub fn new(
        listeners: Vec<Arc<dyn ValidatorEventListener>>,
        state: Arc<ValidationStateStdImpl>,
        block_validated_broadcaster: Sender<BlockIdShort>,
    ) -> Self {
        Self {
            listeners,
            state,
            block_validated_broadcaster,
        }
    }
}

impl Service<ServiceRequest> for NetworkService {
    type QueryResponse = Response;
    type OnQueryFuture = Pin<Box<dyn Future<Output = Option<Self::QueryResponse>> + Send>>;
    type OnMessageFuture = Ready<()>;
    type OnDatagramFuture = Ready<()>;

    fn on_query(&self, req: ServiceRequest) -> Self::OnQueryFuture {
        let query_result = req.parse_tl();

        let state = self.state.clone();
        let listeners = self.listeners.clone();
        let broadcaster = self.block_validated_broadcaster.clone();
        async move {
            match query_result {
                Ok(query) => {
                    let query: SignaturesQuery = query;

                    {
                        let session = state
                            .get_session(query.block_id_short.shard, query.session_seqno)
                            .await;

                        match handle_signatures_query(
                            session,
                            query.session_seqno,
                            query.block_id_short,
                            query.wrapped_signatures(),
                            &listeners,
                            broadcaster,
                        )
                        .await
                        {
                            Ok(response_option) => response_option,
                            Err(e) => {
                                tracing::error!("Error handling signatures query: {:?}", e);
                                panic!("Error handling signatures query: {:?}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Error parsing query: {:?}", e);
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
