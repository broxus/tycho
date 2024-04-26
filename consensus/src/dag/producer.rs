use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;

use tycho_network::PeerId;

use crate::dag::anchor_stage::AnchorStage;
use crate::dag::DagRound;
use crate::models::{Link, Location, Point, PointBody, PrevPoint, Round, Through, UnixTime};

// FIXME make it PointBuilder
pub struct Producer;

impl Producer {
    pub async fn new_point(
        finished_round: &DagRound,
        new_round: &DagRound,
        prev_point: Option<&PrevPoint>,
        payload: Vec<Bytes>,
    ) -> Option<Arc<Point>> {
        let key_pair = new_round.key_pair()?;
        let local_id = PeerId::from(key_pair.public_key);
        match new_round.anchor_stage() {
            Some(AnchorStage::Proof { leader, .. } | AnchorStage::Trigger { leader, .. })
                if leader == &local_id && prev_point.is_none() =>
            {
                // wave leader must skip new round if it failed to produce 3 points in a row
                return None;
            }
            _ => {}
        };
        let includes = Self::includes(finished_round);
        let mut anchor_trigger = Self::link_from_includes(&local_id, &new_round, &includes, true);
        let mut anchor_proof = Self::link_from_includes(&local_id, &new_round, &includes, false);
        let witness = Self::witness(finished_round);
        Self::update_link_from_witness(&mut anchor_trigger, finished_round.round(), &witness, true);
        Self::update_link_from_witness(&mut anchor_proof, finished_round.round(), &witness, false);
        let time = Self::get_time(
            finished_round,
            &local_id,
            &anchor_proof,
            prev_point,
            &includes,
            &witness,
        )
        .await;
        let includes = includes
            .into_iter()
            .map(|point| (point.body.location.author, point.digest.clone()))
            .collect::<BTreeMap<_, _>>();
        let witness = witness
            .into_iter()
            .map(|point| (point.body.location.author, point.digest.clone()))
            .collect::<BTreeMap<_, _>>();
        Some(Arc::new(
            PointBody {
                location: Location {
                    round: new_round.round().clone(),
                    author: local_id.clone(),
                },
                time,
                payload,
                proof: prev_point.cloned(),
                includes,
                witness,
                anchor_trigger,
                anchor_proof,
            }
            .wrap(&key_pair),
        ))
    }

    fn includes(finished_round: &DagRound) -> Vec<Arc<Point>> {
        let includes = finished_round
            .select(|(_, loc)| {
                loc.state()
                    .signed_point(finished_round.round())
                    .map(|valid| valid.point.clone())
            })
            .collect::<Vec<_>>();
        assert!(
            includes.iter().count() >= finished_round.node_count().majority(),
            "Coding error: producing point with not enough includes, check Collector logic"
        );
        includes
    }

    fn witness(finished_round: &DagRound) -> Vec<Arc<Point>> {
        if let Some(witness_round) = finished_round.prev().get() {
            witness_round
                .select(|(_, loc)| {
                    loc.state()
                        .signed_point(finished_round.round())
                        .map(|valid| valid.point.clone())
                })
                .collect()
        } else {
            vec![]
        }
    }

    fn link_from_includes(
        local_id: &PeerId,
        new_round: &DagRound,
        includes: &Vec<Arc<Point>>,
        is_for_trigger: bool,
    ) -> Link {
        match new_round.anchor_stage() {
            Some(AnchorStage::Trigger { leader, .. }) if is_for_trigger && leader == local_id => {
                Link::ToSelf
            }
            Some(AnchorStage::Proof { leader, .. }) if !is_for_trigger && leader == local_id => {
                Link::ToSelf
            }
            _ => {
                let point = includes
                    .iter()
                    .max_by_key(|point| {
                        if is_for_trigger {
                            point.anchor_trigger_round()
                        } else {
                            point.anchor_proof_round()
                        }
                    })
                    .expect("non-empty list of includes for own point");
                if point.body.location.round == new_round.round().prev()
                    && ((is_for_trigger && point.body.anchor_trigger == Link::ToSelf)
                        || (!is_for_trigger && point.body.anchor_proof == Link::ToSelf))
                {
                    Link::Direct(Through::Includes(point.body.location.author.clone()))
                } else {
                    let to = if is_for_trigger {
                        point.anchor_trigger_id()
                    } else {
                        point.anchor_proof_id()
                    };
                    Link::Indirect {
                        to,
                        path: Through::Includes(point.body.location.author.clone()),
                    }
                }
            }
        }
    }

    fn update_link_from_witness(
        link: &mut Link,
        finished_round: &Round,
        witness: &Vec<Arc<Point>>,
        is_for_trigger: bool,
    ) {
        let link_round = match link {
            Link::ToSelf | Link::Direct(_) => return,
            Link::Indirect { to, .. } => to.location.round,
        };
        fn last_round(point: &Point, is_for_trigger: bool) -> Round {
            if is_for_trigger {
                point.anchor_trigger_round()
            } else {
                point.anchor_proof_round()
            }
        }
        let Some(point) = witness
            .iter()
            .filter(|point| last_round(&point, is_for_trigger) > link_round)
            .max_by_key(|point| last_round(&point, is_for_trigger))
        else {
            return;
        };
        if point.body.location.round == finished_round.prev()
            && ((is_for_trigger && point.body.anchor_trigger == Link::ToSelf)
                || (!is_for_trigger && point.body.anchor_proof == Link::ToSelf))
        {
            *link = Link::Direct(Through::Witness(point.body.location.author))
        } else {
            let to = if is_for_trigger {
                point.anchor_trigger_id()
            } else {
                point.anchor_proof_id()
            };
            *link = Link::Indirect {
                to,
                path: Through::Witness(point.body.location.author),
            }
        };
    }

    async fn get_time(
        finished_round: &DagRound,
        local_id: &PeerId,
        anchor_proof: &Link,
        prev_point: Option<&PrevPoint>,
        includes: &Vec<Arc<Point>>,
        witness: &Vec<Arc<Point>>,
    ) -> UnixTime {
        let mut time = UnixTime::now();
        if let Some(prev_point) = prev_point {
            if let Some(valid) = finished_round
                .valid_point_exact(&local_id, &prev_point.digest)
                .await
            {
                time = valid.point.body.time.clone().max(time);
            }
        }
        match anchor_proof {
            Link::ToSelf => {}
            Link::Direct(through) => {
                let (peer_id, through) = match through {
                    Through::Includes(peer_id) => (peer_id, &includes),
                    Through::Witness(peer_id) => (peer_id, &witness),
                };
                if let Some(point) = through
                    .iter()
                    .find(|point| point.body.location.author == peer_id)
                {
                    time = point.body.time.clone().max(time);
                }
            }
            Link::Indirect { to, .. } => {
                // it's sufficient to check prev point - it can't have newer anchor proof
                if prev_point.is_none() {
                    if let Some(valid) = finished_round.valid_point(&to).await {
                        time = valid.point.body.time.clone().max(time);
                    } else {
                        panic!("last anchor proof must stay in DAG until its payload is committed")
                    }
                }
            }
        }
        // TODO maybe take the greatest time among all point's dependencies - as they must be signed
        time
    }
}
