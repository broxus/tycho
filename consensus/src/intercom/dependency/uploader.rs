use std::sync::Arc;

use tokio::sync::{mpsc, oneshot, RwLock};
use tycho_util::futures::{JoinTask, Shared};

use crate::dag::DagRound;
use crate::intercom::dto::PointByIdResponse;
use crate::models::{DagPoint, PointId, Ugly};

pub struct Uploader {
    log_id: Arc<String>,
    requests: mpsc::UnboundedReceiver<(PointId, oneshot::Sender<PointByIdResponse>)>,
    top_dag_round: Arc<RwLock<DagRound>>,
}

impl Uploader {
    pub fn new(
        log_id: Arc<String>,
        requests: mpsc::UnboundedReceiver<(PointId, oneshot::Sender<PointByIdResponse>)>,
        top_dag_round: Arc<RwLock<DagRound>>,
    ) -> Self {
        Self {
            log_id,
            requests,
            top_dag_round,
        }
    }

    pub async fn run(mut self) -> ! {
        while let Some((point_id, callback)) = self.requests.recv().await {
            let found = match self.find(&point_id).await {
                // uploader must hide points that it accounts as not eligible for signature
                Some(shared) => shared.await.0.into_trusted().map(|trusted| trusted.point),
                None => None,
            };
            if let Err(_) = callback.send(PointByIdResponse(found)) {
                tracing::debug!(
                    "{} Uploader result channel closed, requester {:?} cancelled download",
                    self.log_id,
                    point_id.ugly()
                );
            };
        }
        panic!("Uploader incoming channel closed")
    }

    // Note: drops strong ref to DagRound as soon as possible - to let it be gone with weak ones
    async fn find(&self, point_id: &PointId) -> Option<Shared<JoinTask<DagPoint>>> {
        let top_dag_round = {
            let read = self.top_dag_round.read().await;
            read.clone()
        };
        if point_id.location.round > top_dag_round.round() {
            return None;
        }
        top_dag_round
            .scan(point_id.location.round)?
            .view(&point_id.location.author, |loc| {
                loc.versions().get(&point_id.digest).cloned()
            })?
    }
}
