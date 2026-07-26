use tycho_crypto::ed25519::KeyPair;
use tycho_network::PeerId;
use tycho_util::FastHashMap;

use crate::dag::proof_carrier::{ProofCarrierCounts, ProofCarrierProjection};
use crate::dag::{DagHead, DagRound};
use crate::effects::{AltFormat, RoundCtx};
use crate::engine::{InputBuffer, MempoolConfig};
use crate::models::{
    AnchorLink, AnchorStageRole, AnyLink, Digest, IndirectLink, PeerCount, Point, PointData,
    PointId, PointInfo, PointRole, ProofConstraint, Round, Signature, Through, UnixTime,
    ValidPoint,
};

pub struct LastOwnPoint {
    pub digest: Digest,
    pub evidence: FastHashMap<PeerId, Signature>,
    pub includes: FastHashMap<PeerId, Digest>,
    pub sticky_anchors: Option<u8>,
    pub round: Round,
    pub signers: PeerCount,
}

#[derive(thiserror::Error, Debug)]
pub enum ProduceError {
    #[error("not enough evidence to start producer task")]
    NotAllowed, // same check in another place
    #[error("not enough evidence")]
    NotEnoughEvidence,
    #[error("reached threshold for the next round")]
    NextRoundThreshold,
    #[error("node is not scheduled at this round")]
    NotScheduled,
    #[error(
        "Included prev point digest does not match broadcasted: # {} != {:?}. \
         This may be OK after DB deletion: try to restart the node.",
        included.alt(),
        broadcasted.as_ref().map(|a| tracing::field::display(a.alt()))
    )]
    PrevPointMismatch {
        included: Digest,
        broadcasted: Option<Digest>,
    },
    #[error("cannot produce with incomplete inherited proof constraint")]
    ProofConstraintIncomplete,
    #[error(
        "cannot produce with conflicting proof constraints {:?} and {:?}",
        .0.0.alt(),
        .0.1.alt(),
    )]
    ProofConstraintConflict(Box<(PointId, PointId)>),
}

pub struct Producer;

impl Producer {
    pub fn new_point(
        last_own_point: Option<&LastOwnPoint>,
        input_buffer: &InputBuffer,
        head: &DagHead,
        conf: &MempoolConfig,
    ) -> Result<Point, ProduceError> {
        let finished_round = head.prev();
        let Some(key_pair) = head.keys().to_produce.as_deref() else {
            return Err(ProduceError::NotScheduled);
        };

        let local_id = PeerId::from(key_pair.public_key);
        let include_points = Self::includes(finished_round);
        let witness_points = Self::witness(finished_round, &local_id, last_own_point);
        let includes_peer_count = finished_round.peer_count();
        let witness_peer_count = (finished_round.prev().upgrade()).map(|round| round.peer_count());
        let proof_carriers = ProofCarrierCounts::from_valid_dependencies(
            includes_peer_count,
            witness_peer_count,
            include_points.values(),
            witness_points.values(),
        )
        .finish();
        let includes = include_points
            .into_iter()
            .map(|(peer, valid)| (peer, valid.info().clone()))
            .collect();
        let witness = witness_points
            .into_iter()
            .map(|(peer, valid)| (peer, valid.info().clone()))
            .collect();

        Self::create_with_projection(
            last_own_point,
            input_buffer,
            key_pair,
            head.current().round(),
            head.current().leader(),
            &includes,
            &witness,
            &proof_carriers,
            conf,
        )
    }

    #[allow(clippy::too_many_arguments, reason = "used in test with many peers")]
    #[cfg(any(feature = "test", test))]
    pub fn create(
        last_own_point: Option<&LastOwnPoint>,
        input_buffer: &InputBuffer,

        key_pair: &KeyPair,
        current_round: Round,
        current_leader: Option<&PeerId>,

        includes: &FastHashMap<PeerId, PointInfo>,
        witness: &FastHashMap<PeerId, PointInfo>,
        includes_peer_count: PeerCount,
        witness_peer_count: Option<PeerCount>,

        conf: &MempoolConfig,
    ) -> Result<Point, ProduceError> {
        let proof_carriers = ProofCarrierCounts::from_dependencies(
            includes_peer_count,
            witness_peer_count,
            includes.values(),
            witness.values(),
        )
        .finish();
        Self::create_with_projection(
            last_own_point,
            input_buffer,
            key_pair,
            current_round,
            current_leader,
            includes,
            witness,
            &proof_carriers,
            conf,
        )
    }

    #[allow(clippy::too_many_arguments, reason = "keeps test-facing create simple")]
    fn create_with_projection(
        last_own_point: Option<&LastOwnPoint>,
        input_buffer: &InputBuffer,

        key_pair: &KeyPair,
        current_round: Round,
        current_leader: Option<&PeerId>,

        includes: &FastHashMap<PeerId, PointInfo>,
        witness: &FastHashMap<PeerId, PointInfo>,
        proof_carriers: &ProofCarrierProjection,

        conf: &MempoolConfig,
    ) -> Result<Point, ProduceError> {
        match proof_carriers.constraint() {
            ProofConstraint::Unconstrained | ProofConstraint::Locked(_) => {}
            ProofConstraint::Incomplete => return Err(ProduceError::ProofConstraintIncomplete),
            ProofConstraint::Conflicting(conflict) => {
                return Err(ProduceError::ProofConstraintConflict(Box::new((
                    conflict.first,
                    conflict.second,
                ))));
            }
        }

        let local_id = PeerId::from(key_pair.public_key);

        let proven_vertex = match last_own_point {
            Some(prev) if prev.round == current_round.prev() => {
                // previous round's point needs 2F signatures from peers scheduled for current round
                if prev.evidence.len() >= prev.signers.majority_of_others() {
                    Some(&prev.digest) // prev point is used only once
                } else {
                    return Err(ProduceError::NotEnoughEvidence); // has to skip round
                }
            }
            _ => None,
        };

        let (anchor_proof, anchor_trigger) =
            link::anchor_links(current_round, includes, witness, proof_carriers);

        let role = if proven_vertex.is_some() {
            let last_own_point = last_own_point.as_ref().expect("guarded by `proven_vertex`");
            let is_leader = current_leader.is_some_and(|leader| leader == local_id);
            let is_trigger = matches!(&anchor_proof,
                AnchorLink::Direct(Through::Includes(author))
                if author == local_id
            );
            let is_proof_far_enough = match &anchor_proof {
                AnchorLink::Indirect(link) => {
                    let rounds_to_proof = (current_round - link.to.round.0).0;
                    if conf.consensus.sticky_anchors == 0 {
                        rounds_to_proof > 2
                    } else {
                        rounds_to_proof > 3
                    }
                }
                AnchorLink::Direct(_) => false,
            };

            if let Some(sticky_anchors) = last_own_point.sticky_anchors {
                if sticky_anchors.saturating_add(1) < conf.consensus.sticky_anchors {
                    PointRole::Sticky {
                        seq_no: sticky_anchors + 1,
                    }
                } else {
                    PointRole::AnchorTrigger
                }
            } else if is_trigger {
                if last_own_point.sticky_anchors.is_none() && 0 < conf.consensus.sticky_anchors {
                    PointRole::Sticky { seq_no: 0 }
                } else {
                    PointRole::AnchorTrigger
                }
            } else if is_leader && is_proof_far_enough {
                PointRole::AnchorProof {
                    anchor_proof: match anchor_proof {
                        AnchorLink::Indirect(link) => link,
                        AnchorLink::Direct(_) => unreachable!("guarded by bool check"),
                    },
                    anchor_trigger,
                }
            } else {
                PointRole::Regular {
                    anchor_proof,
                    anchor_trigger,
                }
            }
        } else {
            PointRole::Regular {
                anchor_proof,
                anchor_trigger,
            }
        };

        let payload = input_buffer.fetch(last_own_point.as_ref().is_none_or(|last| {
            // it's not necessary to resend external messages from previous round
            // if at least 1F+1 peers (one reliable) signed previous point;
            // also notice that payload elems are deduplicated in mempool adapter
            last.evidence.len() >= last.signers.reliable_minority()
        }));

        let prev_info = includes.get(&local_id);

        Self::check_prev_point(prev_info, proven_vertex)?;

        let (time, anchor_time) =
            Self::get_time(&role.anchor_proof(&local_id), prev_info, includes, witness);

        let includes = includes
            .values()
            .map(|info| (*info.author(), *info.digest()))
            .collect::<FastHashMap<_, _>>();

        assert_eq!(
            proven_vertex,
            includes.get(&local_id),
            "must include own point if it exists and vice versa"
        );

        let witness = witness
            .values()
            .map(|info| (*info.author(), *info.digest()))
            .collect::<FastHashMap<_, _>>();

        let evidence = proven_vertex
            .and(last_own_point)
            .map(|p| p.evidence.clone())
            .unwrap_or_default();

        Ok(Point::new(
            key_pair,
            local_id,
            current_round,
            &payload,
            PointData {
                includes,
                witness,
                evidence,
                role,
                time,
                anchor_time,
            },
            conf,
        ))
    }

    fn includes(finished_dag_round: &DagRound) -> FastHashMap<PeerId, ValidPoint> {
        let includes = finished_dag_round.threshold().get_reached();
        assert!(
            includes.len() >= finished_dag_round.peer_count().majority(),
            "Coding error: producing point at {:?} with not enough includes, check Collector logic: {:?}",
            finished_dag_round.round().next(),
            finished_dag_round.alt()
        );
        metrics::counter!("tycho_mempool_collected_includes_count")
            .increment(includes.len() as u64);
        includes
    }

    fn witness(
        finished_dag_round: &DagRound,
        local_id: &PeerId,
        last_own_point: Option<&LastOwnPoint>,
    ) -> FastHashMap<PeerId, ValidPoint> {
        let round = finished_dag_round.round();
        let Some(witness_round) = finished_dag_round.prev().upgrade() else {
            return FastHashMap::default();
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
                    // there still may be spawned tasks to Signer
                    loc.state
                        .get_or_reject()
                        .ok()
                        .map(|signed| (*peer, signed.first_resolved.clone()))
                }
            })
            .collect::<_>()
    }

    fn get_time(
        anchor_proof: &AnyLink<'_>,
        prev_info: Option<&PointInfo>,
        includes: &FastHashMap<PeerId, PointInfo>,
        witness: &FastHashMap<PeerId, PointInfo>,
    ) -> (UnixTime, UnixTime) {
        let anchor_time = match anchor_proof {
            AnyLink::ToSelf => {
                let info = prev_info.expect("anchor candidate should exist");

                info.time()
            }
            AnyLink::Direct(path) | AnyLink::Indirect(IndirectLink { path, .. }) => {
                let (peer_id, through) = match path {
                    Through::Includes(peer_id) => (peer_id, &includes),
                    Through::Witness(peer_id) => (peer_id, &witness),
                };

                let info = (through.get(peer_id))
                    .expect("path to anchor proof should exist in new point dependencies");

                info.anchor_time()
            }
        };

        let deps_time = match prev_info {
            None => anchor_time,
            Some(info) => anchor_time.max(info.time()),
        };

        let now = UnixTime::now();
        let point_time = now.max(deps_time.next());
        RoundCtx::own_point_time_skew(point_time.diff_f64(now));

        (point_time, anchor_time)
    }

    /// DB removal is a corner case: local node tries to produce a point after some downloads
    /// and also may download own point as a dependency of other's points.
    /// So if we equivocated @ r-1, we should not produce @ r+0.
    /// Otherwise, point @ r+0 most likely will be invalid, and we'll have to skip r+1.
    /// This holds with 'release' build profile, while code panics with `debug_assert`.
    ///
    /// Other mismatches (Some vs None) is a coding error.
    fn check_prev_point(
        prev_info: Option<&PointInfo>,
        proven_vertex: Option<&Digest>,
    ) -> Result<(), ProduceError> {
        const OR_REBUILD_TO_SKIP: &str =
            "Or rebuild the node in `release` profile to skip produce point at this round.";
        match (prev_info.map(|prev| prev.digest()), proven_vertex) {
            (None, None) => Ok(()),
            (Some(a), Some(b)) if a == b => Ok(()),
            (Some(&included), broadcasted) => {
                let err = ProduceError::PrevPointMismatch {
                    included,
                    broadcasted: broadcasted.cloned(),
                };
                debug_assert!(false, "{err} {OR_REBUILD_TO_SKIP}");
                Err(err)
            }
            (None, Some(broadcasted)) => {
                panic!(
                    "No point included after broadcasted # {}",
                    broadcasted.alt()
                );
            }
        }
    }
}

mod link {
    use super::*;

    pub fn anchor_links(
        current_round: Round,
        includes: &FastHashMap<PeerId, PointInfo>,
        witness: &FastHashMap<PeerId, PointInfo>,
        proof_carriers: &ProofCarrierProjection,
    ) -> (AnchorLink, AnchorLink) {
        // A carrier quorum restricts both inherited anchors to its proof branch.
        let trigger_source =
            link_source(includes, witness, AnchorStageRole::Trigger, proof_carriers);
        let max_proof_source =
            link_source(includes, witness, AnchorStageRole::Proof, proof_carriers);

        let proof_source = if trigger_source.info.anchor_round(AnchorStageRole::Trigger)
            > max_proof_source.info.anchor_round(AnchorStageRole::Proof)
        {
            trigger_source
        } else {
            max_proof_source
        };

        let anchor_proof = link(current_round, proof_source, AnchorStageRole::Proof);
        let anchor_trigger = link(current_round, trigger_source, AnchorStageRole::Trigger);
        (anchor_proof, anchor_trigger)
    }

    #[derive(Clone, Copy)]
    struct LinkSource<'a> {
        info: &'a PointInfo,
        path: Through,
    }

    fn link_source<'a>(
        includes: &'a FastHashMap<PeerId, PointInfo>,
        witness: &'a FastHashMap<PeerId, PointInfo>,
        link_field: AnchorStageRole,
        proof_carriers: &ProofCarrierProjection,
    ) -> LinkSource<'a> {
        let incl_info = includes
            .values()
            .filter(|info| proof_carriers.incompatible_proof(info).is_none())
            .max_by_key(|info| info.anchor_round(link_field));

        let wit_info = witness
            .values()
            .filter(|info| proof_carriers.incompatible_proof(info).is_none())
            .max_by_key(|info| info.anchor_round(link_field));

        match (incl_info, wit_info) {
            (Some(incl), Some(wit))
                if wit.anchor_round(link_field) > incl.anchor_round(link_field) =>
            {
                LinkSource {
                    info: wit,
                    path: Through::Witness(*wit.author()),
                }
            }
            (Some(info), _) => LinkSource {
                info,
                path: Through::Includes(*info.author()),
            },
            (None, Some(info)) => LinkSource {
                info,
                path: Through::Witness(*info.author()),
            },
            (None, None) => panic!("proof carrier quorum must leave an eligible dependency"),
        }
    }

    fn link(
        current_round: Round,
        source: LinkSource<'_>,
        link_field: AnchorStageRole,
    ) -> AnchorLink {
        let direct_round = match source.path {
            Through::Includes(_) => current_round.prev(),
            Through::Witness(_) => current_round.prev().prev(),
        };

        if source.info.round() == direct_round
            && source.info.anchor_link(link_field) == AnyLink::ToSelf
        {
            AnchorLink::Direct(source.path)
        } else {
            AnchorLink::Indirect(IndirectLink {
                to: source.info.anchor_id(link_field),
                path: source.path,
            })
        }
    }
}
