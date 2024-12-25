use std::collections::BTreeMap;

use everscale_crypto::ed25519::KeyPair;
use tycho_network::PeerId;

use crate::dag::{DagHead, DagRound};
use crate::effects::AltFormat;
use crate::engine::InputBuffer;
use crate::models::{
    AnchorStageRole, Digest, Link, PeerCount, Point, PointData, PointInfo, Round, Signature,
    Through, UnixTime,
};

pub struct LastOwnPoint {
    pub digest: Digest,
    pub evidence: BTreeMap<PeerId, Signature>,
    pub includes: BTreeMap<PeerId, Digest>,
    pub round: Round,
    pub signers: PeerCount,
}

pub struct Producer;

impl Producer {
    pub fn new_point(
        last_own_point: Option<&LastOwnPoint>,
        input_buffer: &InputBuffer,
        head: &DagHead,
    ) -> Option<Point> {
        let current_round = head.current();
        let finished_round = head.prev();
        let key_pair = head.keys().to_produce.as_deref()?;

        let proven_vertex = match last_own_point {
            Some(prev) if prev.round == finished_round.round() => {
                // previous round's point needs 2F signatures from peers scheduled for current round
                if prev.evidence.len() >= prev.signers.majority_of_others() {
                    Some(&prev.digest) // prev point is used only once
                } else {
                    return None; // cannot produce and has to skip round
                }
            }
            _ => None,
        };
        let local_id = PeerId::from(key_pair.public_key);
        let includes = Self::includes(finished_round);
        let mut anchor_trigger = Self::link_from_includes(
            &local_id,
            current_round,
            &includes,
            proven_vertex.is_some()
                && last_own_point.is_some_and(|prev| prev.includes.contains_key(&local_id)),
            AnchorStageRole::Trigger,
        );
        let mut anchor_proof = Self::link_from_includes(
            &local_id,
            current_round,
            &includes,
            proven_vertex.is_some(),
            AnchorStageRole::Proof,
        );
        let witness = Self::witness(
            finished_round,
            &local_id,
            head.keys().to_witness_prev.as_deref(),
            last_own_point,
        );
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

        let (time, anchor_time) =
            Self::get_time(&anchor_proof, &local_id, proven_vertex, &includes, &witness);
        let payload = input_buffer.fetch(last_own_point.as_ref().is_none_or(|last| {
            // it's not necessary to resend external messages from previous round
            // if at least 1F+1 peers (one reliable) signed previous point;
            // also notice that payload elems are deduplicated in mempool adapter
            last.evidence.len() >= last.signers.reliable_minority()
        }));

        let includes = includes
            .into_iter()
            .map(|point| (point.data().author, *point.digest()))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(
            proven_vertex,
            includes.get(&local_id),
            "must include own point if it exists and vice versa"
        );

        let witness = witness
            .into_iter()
            .map(|point| (point.data().author, *point.digest()))
            .collect::<BTreeMap<_, _>>();

        Some(Point::new(
            key_pair,
            current_round.round(),
            proven_vertex
                .zip(last_own_point)
                .map(|(_, p)| p.evidence.clone())
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

    fn includes(finished_dag_round: &DagRound) -> Vec<PointInfo> {
        let includes = finished_dag_round.threshold().get();
        assert!(
            includes.len() >= finished_dag_round.peer_count().majority(),
            "Coding error: producing point at {:?} with not enough includes, check Collector logic: {:?}",
            finished_dag_round.round().next(),
            finished_dag_round.alt()
        );
        includes
    }

    fn witness(
        finished_dag_round: &DagRound,
        local_id: &PeerId,
        key_pair: Option<&KeyPair>,
        last_own_point: Option<&LastOwnPoint>,
    ) -> Vec<PointInfo> {
        let round = finished_dag_round.round();
        let Some(witness_round) = finished_dag_round.prev().upgrade() else {
            return Vec::new();
        };

        let includes = last_own_point
            .filter(|l| l.round == round)
            .map(|l| &l.includes);

        // have to link all @ r-2 if r-1 was skipped - because we made signatures;
        witness_round
            .select(|(peer, loc)| {
                let skip = match includes {
                    // do not repeat previous point's includes (they also contain own point)
                    Some(includes) => includes.contains_key(peer),
                    // exclude own point from failed round - do not make others massively ask for it
                    _ => peer == local_id,
                };
                if skip {
                    None
                } else {
                    // there still may be spawned tasks to Signer, so have to make signatures
                    loc.state
                        .sign_or_reject(round, key_pair)
                        .ok()
                        .and_then(|signed| signed.first_resolved.valid().cloned())
                        .map(|valid| valid.info)
                }
            })
            .collect::<Vec<_>>()
    }

    fn link_from_includes(
        local_id: &PeerId,
        current_round: &DagRound,
        includes: &[PointInfo],
        has_candidate: bool,
        link_field: AnchorStageRole,
    ) -> Link {
        match current_round.anchor_stage() {
            Some(stage)
                if stage.role == link_field && stage.leader == local_id && has_candidate =>
            {
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
        proven_vertex: Option<&Digest>,
        includes: &[PointInfo],
        witness: &[PointInfo],
    ) -> (UnixTime, UnixTime) {
        let prev_info = includes
            .iter()
            .find(|point| point.data().author == local_id);

        assert_eq!(
            prev_info.map(|prev| prev.digest()),
            proven_vertex,
            "included prev point digest does not match broadcasted one"
        );

        let anchor_time = match anchor_proof {
            Link::ToSelf => {
                let point = prev_info.expect("anchor candidate should exist");

                point.data().time
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

                point.data().anchor_time
            }
        };

        let deps_time = match prev_info {
            None => anchor_time,
            Some(info) => anchor_time.max(info.data().time),
        };

        (UnixTime::now().max(deps_time.next()), anchor_time)
    }
}
