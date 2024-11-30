use std::collections::BTreeMap;

use everscale_crypto::ed25519::KeyPair;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use tycho_network::PeerId;

use crate::dag::{DagHead, DagRound};
use crate::effects::AltFormat;
use crate::engine::{CachedConfig, Genesis, InputBuffer};
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

        let proven_vertex = last_own_point
            // previous round's point needs 2F signatures from peers scheduled for current round
            .filter(|prev| {
                // Note: prev point is used only once until weak links are implemented
                finished_round.round() == prev.round
                    && prev.evidence.len() >= prev.signers.majority_of_others()
            });
        let local_id = PeerId::from(key_pair.public_key);
        match current_round.anchor_stage() {
            // wave leader must skip new round if it failed to produce 3 points in a row
            Some(stage) if stage.leader == local_id && proven_vertex.is_none() => return None,
            _ => {}
        };
        let includes = Self::includes(finished_round, key_pair);
        let mut anchor_trigger = Self::link_from_includes(
            &local_id,
            current_round,
            &includes,
            AnchorStageRole::Trigger,
        );
        let mut anchor_proof =
            Self::link_from_includes(&local_id, current_round, &includes, AnchorStageRole::Proof);
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

        let (time, anchor_time, payload) = if finished_round.round() == Genesis::id().round {
            // first produced point is reproducible
            let time = UnixTime::from_millis(CachedConfig::get().genesis.time_millis);
            (time.next(), time, Vec::new())
        } else {
            let (time, anchor_time) =
                Self::get_time(&anchor_proof, &local_id, proven_vertex, &includes, &witness);
            let payload = input_buffer.fetch(last_own_point.as_ref().map_or(false, |last| {
                // it's not necessary to resend external messages from previous round
                // if at least 1F+1 peers (one reliable) signed previous point;
                // also notice that payload elems are deduplicated in mempool adapter
                last.evidence.len() >= last.signers.reliable_minority()
            }));
            (time, anchor_time, payload)
        };

        let includes = includes
            .into_iter()
            .map(|point| (point.data().author, *point.digest()))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(
            proven_vertex.as_ref().map(|prev| &prev.digest),
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

    fn includes(finished_dag_round: &DagRound, key_pair: &KeyPair) -> Vec<PointInfo> {
        let round = finished_dag_round.round();
        let includes = Self::references(round, finished_dag_round, Some(key_pair), true);
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
        // have to make additional signatures because there still may be spawned tasks to Signer
        let references = Self::references(round, &witness_round, key_pair, false);
        let Some(includes) = last_own_point
            .filter(|l| l.round == round)
            .map(|l| &l.includes)
        else {
            // have to link all @ r-2 if r-1 was skipped - because we made signatures;
            // exclude own point from failed round - do not make other nodes massively ask for it;
            return references
                .into_iter()
                .filter(|witness| witness.data().author != local_id)
                .collect();
        };
        // do not repeat includes from previous point if it existed (they also contain own point)
        references
            .into_iter()
            .filter(|witness| !includes.contains_key(&witness.data().author))
            .collect()
    }

    fn references(
        round: Round,
        dag_round: &DagRound,
        key_pair: Option<&KeyPair>,
        include_certified: bool,
    ) -> Vec<PointInfo> {
        let mut last_references = Vec::new();
        let mut references = dag_round
            .select(|(_, loc)| {
                loc.state
                    .signable()
                    .and_then(|signable| match signable.signed() {
                        Some(Ok(_sig)) => signable.first_resolved.valid().map(|v| v.info.clone()),
                        _ if include_certified && signable.first_resolved.certified().is_some() => {
                            signable.first_resolved.certified().map(|v| v.info.clone())
                        }
                        None if key_pair.is_some() => {
                            last_references.push(loc.state.clone());
                            None
                        }
                        Some(Err(_)) | None => None,
                    })
            })
            .collect::<Vec<_>>();

        if last_references.is_empty() {
            return references;
        }

        // support known points
        let mut last_references = last_references
            .into_par_iter()
            .filter_map(|state| {
                state.signable().and_then(|signable| {
                    signable.sign(round, key_pair);
                    match signable.signed() {
                        Some(Ok(_sig)) => signable.first_resolved.valid().map(|v| v.info.clone()),
                        Some(Err(_)) | None => None,
                    }
                })
            })
            .collect();

        references.append(&mut last_references);
        references
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
        proven_vertex: Option<&LastOwnPoint>,
        includes: &[PointInfo],
        witness: &[PointInfo],
    ) -> (UnixTime, UnixTime) {
        let prev_info = includes
            .iter()
            .find(|point| point.data().author == local_id);

        assert_eq!(
            prev_info.map(|prev| prev.digest()),
            proven_vertex.map(|prev| &prev.digest),
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
