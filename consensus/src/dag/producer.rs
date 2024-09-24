use std::collections::BTreeMap;

use tycho_network::PeerId;

use crate::dag::DagRound;
use crate::engine::InputBuffer;
use crate::models::{
    AnchorStageRole, Digest, Link, PeerCount, Point, PointData, PointInfo, Round, Signature,
    Through, UnixTime,
};

pub struct LastOwnPoint {
    pub digest: Digest,
    pub evidence: BTreeMap<PeerId, Signature>,
    pub round: Round,
    pub signers: PeerCount,
}

pub struct Producer;

impl Producer {
    pub fn new_point(
        current_round: &DagRound,
        last_own_point: Option<&LastOwnPoint>,
        input_buffer: &InputBuffer,
    ) -> Option<Point> {
        let finished_round = current_round.prev().upgrade()?;
        let key_pair = current_round.key_pair()?;

        let payload = input_buffer.fetch(last_own_point.as_ref().map_or(false, |last| {
            // it's not necessary to resend external messages from previous round
            // if at least 1F+1 peers (one reliable) signed previous point;
            // also notice that payload elems are deduplicated in mempool adapter
            last.evidence.len() >= last.signers.reliable_minority()
        }));
        let last_own_point = last_own_point
            // previous round's point needs 2F signatures from peers scheduled for current round
            .filter(|prev| {
                // Note: prev point is used only once until weak links are implemented
                current_round.round().prev() == prev.round
                    && prev.evidence.len() >= prev.signers.majority_of_others()
            });
        let local_id = PeerId::from(key_pair.public_key);
        match current_round.anchor_stage() {
            // wave leader must skip new round if it failed to produce 3 points in a row
            Some(stage) if stage.leader == local_id && last_own_point.is_none() => return None,
            _ => {}
        };
        let includes = Self::includes(&finished_round);
        let mut anchor_trigger = Self::link_from_includes(
            &local_id,
            current_round,
            &includes,
            AnchorStageRole::Trigger,
        );
        let mut anchor_proof =
            Self::link_from_includes(&local_id, current_round, &includes, AnchorStageRole::Proof);
        let witness = Self::witness(&finished_round);
        Self::update_link_from_witness(
            &mut anchor_trigger,
            current_round.round(),
            &witness,
            AnchorStageRole::Trigger,
        );
        Self::update_link_from_witness(
            &mut anchor_proof,
            current_round.round(),
            &witness,
            AnchorStageRole::Proof,
        );

        let (time, anchor_time) = Self::get_time(
            &anchor_proof,
            &local_id,
            last_own_point,
            &includes,
            &witness,
        );

        let includes = includes
            .into_iter()
            .map(|point| (point.data().author, point.digest().clone()))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(
            last_own_point.as_ref().map(|prev| &prev.digest),
            includes.get(&local_id),
            "must include own point if it exists and vice versa"
        );

        let witness = witness
            .into_iter()
            .map(|point| (point.data().author, point.digest().clone()))
            .collect::<BTreeMap<_, _>>();

        Some(Point::new(
            key_pair,
            current_round.round(),
            last_own_point
                .map(|p| p.evidence.clone())
                .unwrap_or_default(),
            payload,
            PointData {
                author: local_id,
                time,
                includes,
                witness,
                anchor_trigger,
                anchor_proof,
                anchor_time,
            },
        ))
    }

    fn includes(finished_round: &DagRound) -> Vec<PointInfo> {
        let includes = finished_round
            .select(|(_, loc)| {
                loc.state()
                    .point()
                    .and_then(|dag_point| dag_point.trusted())
                    // TODO refactor Signable: we are interested not in the round of signature,
                    //   but whether was a point already included or not (just in order not to
                    //   include it twice); repeating inclusions are suboptimal but still correct
                    .filter(|_| loc.state().signed().map_or(true, |r| r.is_ok()))
                    .map(|dag_point| dag_point.info.clone())
            })
            .collect::<Vec<_>>();
        assert!(
            includes.len() >= finished_round.peer_count().majority(),
            "Coding error: producing point with not enough includes, check Collector logic"
        );
        includes
    }

    fn witness(finished_round: &DagRound) -> Vec<PointInfo> {
        match finished_round.prev().upgrade() {
            Some(witness_round) => witness_round
                .select(|(_, loc)| {
                    loc.state()
                        .signed_point(finished_round.round())
                        .map(|valid| valid.info.clone())
                })
                .collect(),
            None => vec![],
        }
    }

    fn link_from_includes(
        local_id: &PeerId,
        current_round: &DagRound,
        includes: &[PointInfo],
        link_field: AnchorStageRole,
    ) -> Link {
        match current_round.anchor_stage() {
            Some(stage) if stage.role == link_field && stage.leader == local_id => {
                return Link::ToSelf;
            }
            _ => {}
        }

        let point = includes
            .iter()
            .max_by_key(|point| point.anchor_round(link_field))
            .expect("non-empty list of includes for own point");

        if point.round() == current_round.round().prev()
            && point.anchor_link(link_field) == &Link::ToSelf
        {
            Link::Direct(Through::Includes(point.data().author))
        } else {
            Link::Indirect {
                to: point.anchor_id(link_field),
                path: Through::Includes(point.data().author),
            }
        }
    }

    fn update_link_from_witness(
        link: &mut Link,
        current_round: Round,
        witness: &[PointInfo],
        link_field: AnchorStageRole,
    ) {
        let link_round = match link {
            Link::ToSelf | Link::Direct(_) => return,
            Link::Indirect { to, .. } => to.round,
        };

        let Some(point) = witness
            .iter()
            .filter(|point| point.anchor_round(link_field) > link_round)
            .max_by_key(|point| point.anchor_round(link_field))
        else {
            return;
        };

        if point.round() == current_round.prev().prev()
            && point.anchor_link(link_field) == &Link::ToSelf
        {
            *link = Link::Direct(Through::Witness(point.data().author));
        } else {
            *link = Link::Indirect {
                to: point.anchor_id(link_field),
                path: Through::Witness(point.data().author),
            };
        }
    }

    fn get_time(
        anchor_proof: &Link,
        local_id: &PeerId,
        prev_point: Option<&LastOwnPoint>,
        includes: &[PointInfo],
        witness: &[PointInfo],
    ) -> (UnixTime, UnixTime) {
        let mut time = UnixTime::now();

        let prev_info = includes
            .iter()
            .find(|point| point.data().author == local_id)
            .inspect(|info| time = info.data().time.max(time));

        assert_eq!(
            prev_info.map(|prev| prev.digest()),
            prev_point.map(|prev| &prev.digest),
            "included prev point digest does not match broadcasted one"
        );

        match anchor_proof {
            Link::ToSelf => {
                let point = prev_info.expect("anchor candidate should exist");

                let anchor_time = point.data().time;

                (time.max(anchor_time), anchor_time)
            }
            Link::Direct(through) | Link::Indirect { path: through, .. } => {
                let (peer_id, through) = match through {
                    Through::Includes(peer_id) => (peer_id, &includes),
                    Through::Witness(peer_id) => (peer_id, &witness),
                };

                let point = through
                    .iter()
                    .find(|point| point.data().author == peer_id)
                    .expect("path to anchor proof should exist in new point dependencies");

                let anchor_time = point.data().anchor_time;

                (time.max(anchor_time), anchor_time)
            }
        }
    }
}
