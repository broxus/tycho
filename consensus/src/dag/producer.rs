use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use tycho_network::PeerId;

use crate::dag::anchor_stage::AnchorStage;
use crate::dag::DagRound;
use crate::models::{Link, Location, Point, PointBody, PrevPoint, Round, Through, UnixTime};

pub struct Producer;

impl Producer {
    pub async fn new_point(
        current_round: &DagRound,
        prev_point: Option<&PrevPoint>,
        payload: Vec<Bytes>,
    ) -> Option<Arc<Point>> {
        let finished_round = current_round.prev().get()?;
        let key_pair = current_round.key_pair()?;
        let local_id = PeerId::from(key_pair.public_key);
        match current_round.anchor_stage() {
            Some(AnchorStage::Proof { leader, .. } | AnchorStage::Trigger { leader, .. })
                if leader == &local_id && prev_point.is_none() =>
            {
                // wave leader must skip new round if it failed to produce 3 points in a row
                return None;
            }
            _ => {}
        };
        let includes = Self::includes(&finished_round);
        let mut anchor_trigger =
            Self::link_from_includes(&local_id, &current_round, &includes, true);
        let mut anchor_proof =
            Self::link_from_includes(&local_id, &current_round, &includes, false);
        let witness = Self::witness(&finished_round);
        Self::update_link_from_witness(&mut anchor_trigger, finished_round.round(), &witness, true);
        Self::update_link_from_witness(&mut anchor_proof, finished_round.round(), &witness, false);
        let time = Self::get_time(
            &finished_round,
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
        assert!(
            prev_point.map_or(true, |prev| includes
                .get(&local_id)
                .map_or(false, |digest| digest == &prev.digest)),
            "must include own point if it exists"
        );
        assert!(
            prev_point.map_or(true, |prev| prev.evidence.len()
                >= current_round.node_count().majority_of_others()),
            "Collected not enough evidence, check Broadcaster logic"
        );
        let witness = witness
            .into_iter()
            .map(|point| (point.body.location.author, point.digest.clone()))
            .collect::<BTreeMap<_, _>>();

        Some(Point::new(key_pair, PointBody {
            location: Location {
                round: current_round.round().clone(),
                author: local_id.clone(),
            },
            time,
            payload,
            proof: prev_point.cloned(),
            includes,
            witness,
            anchor_trigger,
            anchor_proof,
        }))
    }

    fn includes(finished_round: &DagRound) -> Vec<Arc<Point>> {
        let includes = finished_round
            .select(|(_, loc)| {
                loc.state()
                    .point()
                    .map(|dag_point| dag_point.trusted())
                    .flatten()
                    // TODO refactor Signable: we are interested not in the round of signature,
                    //   but whether was a point already included or not (just in order not to
                    //   include it twice); repeating inclusions are suboptimal but still correct
                    .filter(|_| loc.state().signed().map_or(true, |r| r.is_ok()))
                    .map(|dag_point| dag_point.point.clone())
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
        current_round: &DagRound,
        includes: &[Arc<Point>],
        is_for_trigger: bool,
    ) -> Link {
        let is_leader = match current_round.anchor_stage() {
            Some(AnchorStage::Trigger { leader, .. }) => is_for_trigger && leader == local_id,
            Some(AnchorStage::Proof { leader, .. }) => !is_for_trigger && leader == local_id,
            _ => false,
        };

        if is_leader {
            return Link::ToSelf;
        }

        let point = includes
            .iter()
            .max_by_key(|p| {
                let to = p.last_round(is_for_trigger);
            })
            .expect("includes should contain at least one point");

        let last_round = point.body.location.round;

        if last_round == current_round.round().prev() {
            let is_self_anchor =
                (is_for_trigger && point.body.anchor_trigger == Link::ToSelf) ||
                    (!is_for_trigger && point.body.anchor_proof == Link::ToSelf);

            if is_self_anchor {
                return Link::Direct(Through { source: point.body.location.author.clone() });
            }
        }

        let to = point.last_round_id(is_for_trigger);

        Link::Indirect {
            to,
            path: Through { source: point.body.location.author.clone() },
        }
    }

    fn update_link_from_witness(
        link: &mut Link,
        finished_round: &Round,
        witness: &[Arc<Point>],
        is_for_trigger: bool,
    ) {
        let link_round = match link {
            Link::ToSelf | Link::Direct(_) => return,
            Link::Indirect { to, .. } => to.location.round,
        };

        if let Some(point) = witness
            .iter()
            .filter(|p| p.last_round(is_for_trigger) > link_round) // Condition check
            .max_by_key(|p| p.last_round(is_for_trigger))
        {
            let last_round = point.body.location.round;

            let is_trigger_match = is_for_trigger && point.body.anchor_trigger == Link::ToSelf;
            let is_proof_match = !is_for_trigger && point.body.anchor_proof == Link::ToSelf;

            if last_round == finished_round.prev() && (is_trigger_match || is_proof_match) {
                *link = Link::Direct(Through::Witness(point.body.location.author));
            } else {
                let to = point.last_round_id(is_for_trigger);
                *link = Link::Indirect {
                    to,
                    path: Through::Witness(point.body.location.author),
                };
            }
        }
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
        // No need to take the greatest time among all point's dependencies -
        // only leader's time is significant and every node will have its chance
        // (or its chain will be rejected). Better throw away a single node's point
        // than requiring the whole consensus to wait once the point was included.
        // Todo: use proven anchor candidate's time, as it is unique
        time
    }
}
