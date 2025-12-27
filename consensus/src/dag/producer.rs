use tycho_network::PeerId;
use tycho_util::FastHashMap;

use crate::dag::{DagHead, DagRound};
use crate::effects::{AltFormat, RoundCtx};
use crate::engine::{InputBuffer, MempoolConfig};
use crate::models::{
    AnchorStageRole, ChainedAnchorProof, Digest, IndirectLink, Link, PeerCount, Point, PointData,
    PointInfo, Round, Signature, Through, UnixTime,
};

pub struct LastOwnPoint {
    pub digest: Digest,
    pub evidence: FastHashMap<PeerId, Signature>,
    pub includes: FastHashMap<PeerId, Digest>,
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
}

pub struct Producer;

impl Producer {
    pub fn new_point(
        last_own_point: Option<&LastOwnPoint>,
        input_buffer: &InputBuffer,
        head: &DagHead,
        conf: &MempoolConfig,
    ) -> Result<Point, ProduceError> {
        let current_round = head.current();
        let finished_round = head.prev();
        let Some(key_pair) = head.keys().to_produce.as_deref() else {
            return Err(ProduceError::NotScheduled);
        };

        let proven_vertex = match last_own_point {
            Some(prev) if prev.round == finished_round.round() => {
                // previous round's point needs 2F signatures from peers scheduled for current round
                if prev.evidence.len() >= prev.signers.majority_of_others() {
                    Some(&prev.digest) // prev point is used only once
                } else {
                    return Err(ProduceError::NotEnoughEvidence); // has to skip round
                }
            }
            _ => None,
        };
        let local_id = PeerId::from(key_pair.public_key);
        let includes = Self::includes(finished_round);
        let witness = Self::witness(finished_round, &local_id, last_own_point);
        let anchor_trigger = Self::link(
            &local_id,
            current_round,
            &includes,
            &witness,
            proven_vertex.is_some()
                && last_own_point.is_some_and(|prev| prev.includes.contains_key(&local_id)),
            AnchorStageRole::Trigger,
        );
        let anchor_proof = Self::link(
            &local_id,
            current_round,
            &includes,
            &witness,
            proven_vertex.is_some(),
            AnchorStageRole::Proof,
        );
        let chained_anchor_proof =
            Self::chained_anchor_proof(current_round, &includes, &witness, &anchor_proof);

        let payload = input_buffer.fetch(last_own_point.as_ref().is_none_or(|last| {
            // it's not necessary to resend external messages from previous round
            // if at least 1F+1 peers (one reliable) signed previous point;
            // also notice that payload elems are deduplicated in mempool adapter
            last.evidence.len() >= last.signers.reliable_minority()
        }));

        let prev_info = (includes.iter()).find(|point| point.author() == local_id);

        Self::check_prev_point(prev_info, proven_vertex)?;

        let (time, anchor_time) = Self::get_time(&anchor_proof, prev_info, &includes, &witness);

        let includes = includes
            .into_iter()
            .map(|info| (*info.author(), *info.digest()))
            .collect::<FastHashMap<_, _>>();

        assert_eq!(
            proven_vertex,
            includes.get(&local_id),
            "must include own point if it exists and vice versa"
        );

        let witness = witness
            .into_iter()
            .map(|info| (*info.author(), *info.digest()))
            .collect::<FastHashMap<_, _>>();

        let evidence = proven_vertex
            .zip(last_own_point)
            .map(|(_, p)| p.evidence.clone())
            .unwrap_or_default();

        Ok(Point::new(
            key_pair,
            local_id,
            current_round.round(),
            &payload,
            PointData {
                time,
                includes,
                witness,
                evidence,
                chained_anchor_proof,
                anchor_trigger,
                anchor_proof,
                anchor_time,
            },
            conf,
        ))
    }

    fn includes(finished_dag_round: &DagRound) -> Vec<PointInfo> {
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
                    // there still may be spawned tasks to Signer
                    loc.state
                        .get_or_reject()
                        .ok()
                        .map(|signed| signed.first_resolved.info().clone())
                }
            })
            .collect::<Vec<_>>()
    }

    fn link(
        local_id: &PeerId,
        current_round: &DagRound,
        includes: &[PointInfo],
        witness: &[PointInfo],
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

        let incl_info = includes
            .iter()
            .max_by_key(|point| point.anchor_round(link_field))
            .expect("non-empty list of includes for own point");

        if incl_info.round() == current_round.round().prev()
            && incl_info.anchor_link(link_field) == &Link::ToSelf
        {
            return Link::Direct(Through::Includes(*incl_info.author()));
        };

        let newer_witness = witness
            .iter()
            .max_by_key(|wit_info| wit_info.anchor_round(link_field))
            .filter(|wit_info| {
                wit_info.anchor_round(link_field) > incl_info.anchor_round(link_field)
            });

        let Some(wit_info) = newer_witness else {
            return Link::Indirect(IndirectLink {
                to: incl_info.anchor_id(link_field),
                path: Through::Includes(*incl_info.author()),
            });
        };

        if wit_info.round() == current_round.round().prev().prev()
            && wit_info.anchor_link(link_field) == &Link::ToSelf
        {
            return Link::Direct(Through::Witness(*wit_info.author()));
        }

        Link::Indirect(IndirectLink {
            to: wit_info.anchor_id(link_field),
            path: Through::Witness(*wit_info.author()),
        })
    }

    fn chained_anchor_proof(
        current_round: &DagRound,
        includes: &[PointInfo],
        witness: &[PointInfo],
        anchor_proof: &Link,
    ) -> ChainedAnchorProof {
        use AnchorStageRole::Proof;

        if anchor_proof != &Link::ToSelf {
            return ChainedAnchorProof::Inapplicable;
        }

        let incl_info = includes
            .iter()
            .max_by_key(|point| point.anchor_round(Proof))
            .expect("non-empty list of includes for own point");

        let newer_witness = witness
            .iter()
            .max_by_key(|wit_info| wit_info.anchor_round(Proof))
            .filter(|wit_info| wit_info.anchor_round(Proof) > incl_info.anchor_round(Proof));

        let indirect = match newer_witness {
            None => IndirectLink {
                to: incl_info.anchor_id(Proof),
                path: Through::Includes(*incl_info.author()),
            },
            Some(wit_info) => IndirectLink {
                to: wit_info.anchor_id(Proof),
                path: Through::Witness(*wit_info.author()),
            },
        };

        assert!(
            indirect.to.round < current_round.round().prev().prev(),
            "chained anchor proof cannot be a direct link"
        );

        ChainedAnchorProof::Chained(indirect)
    }

    fn get_time(
        anchor_proof: &Link,
        prev_info: Option<&PointInfo>,
        includes: &[PointInfo],
        witness: &[PointInfo],
    ) -> (UnixTime, UnixTime) {
        let anchor_time = match anchor_proof {
            Link::ToSelf => {
                let info = prev_info.expect("anchor candidate should exist");

                info.time()
            }
            Link::Direct(through) | Link::Indirect(IndirectLink { path: through, .. }) => {
                let (peer_id, through) = match through {
                    Through::Includes(peer_id) => (peer_id, &includes),
                    Through::Witness(peer_id) => (peer_id, &witness),
                };

                let info = through
                    .iter()
                    .find(|point| point.author() == peer_id)
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
