use std::ops::Deref;
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot, watch};

use crate::dag::DagRound;
use crate::intercom::dto::PointByIdResponse;
use crate::models::{DagPoint, Point, PointId};

pub struct Uploader {
    requests: mpsc::UnboundedReceiver<(PointId, oneshot::Sender<PointByIdResponse>)>,
    top_dag_round: watch::Receiver<DagRound>,
}

impl Uploader {
    pub fn new(
        requests: mpsc::UnboundedReceiver<(PointId, oneshot::Sender<PointByIdResponse>)>,
        top_dag_round: watch::Receiver<DagRound>,
    ) -> Self {
        Self {
            requests,
            top_dag_round,
        }
    }

    pub async fn run(mut self) -> ! {
        while let Some((point_id, callback)) = self.requests.recv().await {
            if let Err(_) = callback.send(PointByIdResponse(
                self.find(&point_id).await.map(|p| p.deref().clone()),
            )) {
                tracing::error!("Uploader result channel closed for {point_id:.4?}");
            };
        }
        panic!("Uploader incoming channel closed")
    }

    async fn find(&self, point_id: &PointId) -> Option<Arc<Point>> {
        let top_dag_round = self.top_dag_round.borrow().clone();
        let shared = top_dag_round
            .scan(&point_id.location.round)
            .map(|dag_round| {
                dag_round
                    .view(&point_id.location.author, |loc| {
                        loc.versions().get(&point_id.digest).cloned()
                    })
                    .flatten()
            })
            .flatten()?;
        // keep such matching private to Uploader, it must not be used elsewhere
        match shared.await {
            (DagPoint::Trusted(valid), _) => Some(valid.point),
            (DagPoint::Suspicious(valid), _) => Some(valid.point),
            (DagPoint::Invalid(invalid), _) => Some(invalid),
            (DagPoint::NotExists(_), _) => None,
        }
    }
}
