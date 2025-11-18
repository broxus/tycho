use std::mem;
use std::sync::atomic;

use ahash::{HashMapExt, HashSetExt};
use futures_util::FutureExt;
use tycho_network::PeerId;
use tycho_util::{FastHashMap, FastHashSet};

use crate::dag::DagRound;
use crate::effects::{AltFormat, TaskResult};
use crate::models::{DagPoint, Digest, MempoolPeerCounters, MempoolPeerStats, PointInfo, Round};

type RoundDataMap = FastHashMap<PeerId, PeerRoundData>;
struct PeerRoundData {
    /// at this round this peer created points without issues
    authored: Vec<PointInfo>,
    /// at this round this peer made signatures for other's digest and peer id
    signed: FastHashSet<Digest>,
}

pub struct RoundInspector {
    stats: FastHashMap<PeerId, MempoolPeerStats>,
    last_round: Round,
    last_round_data_map: RoundDataMap,
}
impl Default for RoundInspector {
    fn default() -> Self {
        Self {
            stats: FastHashMap::default(),
            last_round: Round::BOTTOM,
            last_round_data_map: FastHashMap::default(),
        }
    }
}

impl RoundInspector {
    pub fn take_stats(&mut self) -> FastHashMap<PeerId, MempoolPeerStats> {
        let capacity = if self.stats.capacity() >= self.stats.len() * 4 {
            self.stats.len() * 2
        } else {
            self.stats.capacity()
        };
        mem::replace(&mut self.stats, FastHashMap::with_capacity(capacity))
    }

    pub fn inspect(&mut self, r_0: &DagRound) -> TaskResult<()> {
        let leader_used = r_0
            .anchor_stage()
            .map(|a| (a.leader, a.is_used.load(atomic::Ordering::Relaxed)));

        // map has full peer set for this round, but maybe with empty value
        let p_0 = Self::collect_points(r_0)?;

        for (author, authored) in &p_0 {
            let mut author_counters = MempoolPeerCounters {
                last_round: r_0.round().0,
                ..Default::default()
            };

            let mut authored_versions: u32 = 0;
            let mut has_valid = false;

            for version in authored {
                match version {
                    DagPoint::Valid(_) => {
                        authored_versions += 1;
                        has_valid = true;
                    }
                    DagPoint::Invalid(invalid) => {
                        authored_versions += 1;
                        if !invalid.is_certified() && !invalid.reason().no_dag_round() {
                            author_counters.invalid_points += 1;
                        }
                    }
                    DagPoint::IllFormed(ill) => {
                        authored_versions += 1;
                        if !ill.is_certified() {
                            author_counters.ill_formed_points += 1;
                        }
                    }
                    DagPoint::NotFound(_) => {}
                }
            }
            author_counters.valid_points += has_valid as u32;
            if authored_versions == 0 {
                author_counters.skipped_rounds += 1;
            } else {
                author_counters.equivocated += authored_versions - 1;
            }

            if let Some((leader, used)) = leader_used
                && leader == author
            {
                if used {
                    author_counters.was_leader += 1;
                } else {
                    author_counters.was_not_leader += 1;
                }
            }
            match (self.stats)
                .entry(*author)
                .or_insert_with(|| MempoolPeerStats::new(r_0.round().0))
                .add_in_order(&author_counters)
            {
                Ok(()) => {}
                Err(err) => tracing::error!(
                    author = display(author.alt()),
                    error = display(err),
                    "cannot report peer stats, skipping"
                ),
            };
        }

        // check references against evidences

        let prev_last_round = mem::replace(&mut self.last_round, r_0.round());
        let mut prev_last_round_data_map = mem::replace(
            &mut self.last_round_data_map,
            Self::must_reference(p_0.len(), p_0.values().flatten()),
        );

        if prev_last_round == self.last_round.prev() {
            Self::exclude_witness(
                &mut prev_last_round_data_map,
                (self.last_round_data_map.iter()).flat_map(|(_, a)| &a.authored),
            );
            for (signer, signer_prev_round) in prev_last_round_data_map {
                if signer_prev_round.signed.is_empty() {
                    // peer either signed nothing at prev round or referenced all as includes
                    continue;
                }
                // map contains neither silent peers nor peers from old v_set
                let Some(signer_curr_round_authored) = self
                    .last_round_data_map
                    .get(&signer)
                    .map(|curr_round| &curr_round.authored)
                else {
                    // signer was not active at current round, forgive it for skipping a round
                    continue;
                };
                if signer_prev_round.authored.is_empty() || signer_curr_round_authored.is_empty() {
                    // will proceed with peers that made two consecutive points, forgive others
                    continue;
                }
                let Some(signer_stats) = self.stats.get_mut(&signer) else {
                    // this map contains full new v_set, but prev v_set is not guaranteed
                    continue;
                };
                // signer created points at both rounds, but skipped somme points it signed
                signer_stats.add_references_skipped(signer_prev_round.signed.len() as u32);
            }
        }

        // finish

        Ok(())
    }

    /// returns full `v_set` so keys can be used instead of `PeerSchedule` or `DagRound`
    fn collect_points(r: &DagRound) -> TaskResult<FastHashMap<PeerId, Vec<DagPoint>>> {
        r.select(|(author, loc)| {
            let versions = loc
                .versions
                .values()
                .cloned()
                .filter_map(FutureExt::now_or_never)
                .collect::<TaskResult<Vec<DagPoint>>>();
            Some(versions.map(|vec| (*author, vec)))
        })
        .collect()
    }

    /// resulting map contains only peers that created a point or a signature
    fn must_reference<'a>(
        r_0_peers: usize,
        p_0: impl Iterator<Item = &'a DagPoint> + Clone,
    ) -> RoundDataMap {
        // inverted evidences: as a map of commitments to reference signed points
        let mut round_data_map = FastHashMap::with_capacity(r_0_peers);

        for point in p_0.clone() {
            if let Some(proof) = point.trusted()
                && let Some(signed_digest) = proof.prev_digest()
            {
                for signer in proof.evidence().keys() {
                    let signer_data =
                        round_data_map
                            .entry(*signer)
                            .or_insert_with(|| PeerRoundData {
                                authored: Vec::with_capacity(1),
                                signed: FastHashSet::with_capacity(r_0_peers),
                            });
                    signer_data.signed.insert(*signed_digest);
                }
            }
        }
        for point in p_0 {
            if let Some(authored) = point.trusted() {
                let author_data =
                    round_data_map
                        .entry(*authored.author())
                        .or_insert_with(|| PeerRoundData {
                            authored: Vec::with_capacity(1),
                            signed: FastHashSet::with_capacity(0),
                        });
                author_data.authored.push(authored.clone());
                for included_digest in authored.includes().values() {
                    author_data.signed.remove(included_digest);
                }
            }
        }

        round_data_map
    }

    fn exclude_witness<'a>(
        prev_must_reference: &mut RoundDataMap,
        curr_points: impl Iterator<Item = &'a PointInfo>,
    ) {
        for curr_authored in curr_points {
            if !curr_authored.witness().is_empty()
                && let Some(author_data) = prev_must_reference.get_mut(curr_authored.author())
                && !author_data.signed.is_empty()
            {
                for witnessed_digest in curr_authored.witness().values() {
                    author_data.signed.remove(witnessed_digest);
                }
            }
        }
    }
}
