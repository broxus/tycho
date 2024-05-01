use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures_util::future::{self, FutureExt, Ready};
use tracing::error;

use tycho_network::__internal::tl_proto::{TlRead, TlWrite};
use tycho_network::{Response, Service, ServiceRequest};

use crate::validator::network::dto::SignaturesQuery;
use crate::validator::network::handlers::handle_signatures_query;
use crate::validator::state::{ValidationState, ValidationStateStdImpl};
use crate::validator::ValidatorEventListener;

#[derive(Clone)]
pub struct NetworkService {
    listeners: Vec<Arc<dyn ValidatorEventListener>>,
    state: Arc<ValidationStateStdImpl>,
}

impl NetworkService {
    pub fn new(
        listeners: Vec<Arc<dyn ValidatorEventListener>>,
        state: Arc<ValidationStateStdImpl>,
    ) -> Self {
        Self { listeners, state }
    }
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, TlRead, TlWrite)]
#[repr(transparent)]
pub struct OverlayId(pub [u8; 32]);

impl Service<ServiceRequest> for NetworkService {
    type QueryResponse = Response;
    type OnQueryFuture = Pin<Box<dyn Future<Output = Option<Self::QueryResponse>> + Send>>;
    type OnMessageFuture = Ready<()>;
    type OnDatagramFuture = Ready<()>;

    fn on_query(&self, req: ServiceRequest) -> Self::OnQueryFuture {
        let query_result = req.parse_tl();

        let state = self.state.clone();
        let listeners = self.listeners.clone();
        async move {
            match query_result {
                Ok(query) => {
                    let SignaturesQuery {
                        session_seqno,
                        block_id_short,
                        signatures,
                    } = query;
                    {
                        let session = state.get_session(session_seqno).await;
                        match handle_signatures_query(
                            session,
                            session_seqno,
                            block_id_short,
                            signatures,
                            &listeners,
                        )
                        .await
                        {
                            Ok(response_option) => response_option,
                            Err(e) => {
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
