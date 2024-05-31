use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use tycho_network::PeerId;

use crate::dag::anchor_stage::AnchorStage;
use crate::dag::DagRound;
use crate::models::{
    Link, LinkField, Location, Point, PointBody, PrevPoint, Round, Through, UnixTime,
};

pub struct Producer;

impl Producer {
    pub fn new_point(
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
            Self::link_from_includes(&local_id, &current_round, &includes, LinkField::Trigger);
        let mut anchor_proof =
            Self::link_from_includes(&local_id, &current_round, &includes, LinkField::Proof);
        let witness = Self::witness(&finished_round);
        Self::update_link_from_witness(
            &mut anchor_trigger,
            current_round.round(),
            &witness,
            LinkField::Trigger,
        );
        Self::update_link_from_witness(
            &mut anchor_proof,
            current_round.round(),
            &witness,
            LinkField::Proof,
        );

        let (time, anchor_time) = Self::get_time(&anchor_proof, prev_point, &includes, &witness);

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
                round: current_round.round(),
                author: local_id,
            },
            time,
            payload,
            proof: prev_point.cloned(),
            includes,
            witness,
            anchor_trigger,
            anchor_proof,
            anchor_time,
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
        match finished_round.prev().get() {
            Some(witness_round) => witness_round
                .select(|(_, loc)| {
                    loc.state()
                        .signed_point(finished_round.round())
                        .map(|valid| valid.point.clone())
                })
                .collect(),
            None => vec![],
        }
    }

    fn link_from_includes(
        local_id: &PeerId,
        current_round: &DagRound,
        includes: &[Arc<Point>],
        link_field: LinkField,
    ) -> Link {
        use AnchorStage::*;

        match (current_round.anchor_stage(), link_field) {
            (Some(Trigger { leader, .. }), LinkField::Trigger)
            | (Some(Proof { leader, .. }), LinkField::Proof)
                if leader == local_id =>
            {
                return Link::ToSelf;
            }
            _ => {}
        }

        let point = includes
            .iter()
            .max_by_key(|point| point.anchor_round(link_field))
            .expect("non-empty list of includes for own point");

        let link = if point.body.location.round == current_round.round().prev()
            && point.anchor_link(link_field) == &Link::ToSelf
        {
            Link::Direct(Through::Includes(point.body.location.author))
        } else {
            Link::Indirect {
                to: point.anchor_id(link_field),
                path: Through::Includes(point.body.location.author),
            }
        };

        link
    }

    fn update_link_from_witness(
        link: &mut Link,
        current_round: Round,
        witness: &[Arc<Point>],
        link_field: LinkField,
    ) {
        let link_round = match link {
            Link::ToSelf | Link::Direct(_) => return,
            Link::Indirect { to, .. } => to.location.round,
        };

        let Some(point) = witness
            .iter()
            .filter(|point| point.anchor_round(link_field) > link_round)
            .max_by_key(|point| point.anchor_round(link_field))
        else {
            return;
        };

        if point.body.location.round == current_round.prev().prev()
            && point.anchor_link(link_field) == &Link::ToSelf
        {
            *link = Link::Direct(Through::Witness(point.body.location.author));
        } else {
            *link = Link::Indirect {
                to: point.anchor_id(link_field),
                path: Through::Witness(point.body.location.author),
            };
        }
    }

    fn get_time(
        anchor_proof: &Link,
        prev_point: Option<&PrevPoint>,
        includes: &[Arc<Point>],
        witness: &[Arc<Point>],
    ) -> (UnixTime, UnixTime) {
        let mut time = UnixTime::now();

        let prev_point = prev_point.and_then(|prev| {
            includes
                .iter()
                .find(|point| point.digest == prev.digest)
                .map(|point| {
                    time = point.body.time.max(time);
                    point
                })
        });

        match anchor_proof {
            Link::ToSelf => {
                let point = prev_point.expect("anchor candidate should exist");

                let anchor_time = point.body.time;

                (time.max(anchor_time), anchor_time)
            }
            Link::Direct(through) | Link::Indirect { path: through, .. } => {
                let (peer_id, through) = match through {
                    Through::Includes(peer_id) => (peer_id, &includes),
                    Through::Witness(peer_id) => (peer_id, &witness),
                };

                let point = through
                    .into_iter()
                    .find(|point| point.body.location.author == peer_id)
                    .expect("anchor proof should exist");

                let anchor_time = point.body.anchor_time;

                (time.max(anchor_time), anchor_time)
            }
        }
    }
}
