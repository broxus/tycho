use std::mem;
use std::sync::atomic;

use ahash::{HashMapExt, HashSetExt};
use futures_util::FutureExt;
use tycho_network::PeerId;
use tycho_util::{FastHashMap, FastHashSet};

use crate::dag::DagRound;
use crate::effects::TaskResult;
use crate::models::{DagPoint, Digest, PeerStats, Round};

type PeerRefData = FastHashMap<PeerId, RefData>;
struct RefData {
    has_first_point: bool,
    signed: FastHashSet<Digest>,
}

pub struct RoundInspector {
    data: FastHashMap<PeerId, PeerStats>,
    last_round: Round,
    prev_peer_ref_data: PeerRefData,
}
impl Default for RoundInspector {
    fn default() -> Self {
        Self {
            data: Default::default(),
            last_round: Round::BOTTOM,
            prev_peer_ref_data: Default::default(),
        }
    }
}

impl RoundInspector {
    pub fn get_stats(&mut self) -> FastHashMap<PeerId, PeerStats> {
        let new_map = FastHashMap::with_capacity(self.data.len());
        mem::replace(&mut self.data, new_map)
    }

    pub fn inspect(&mut self, r_0: &DagRound) -> TaskResult<()> {
        let leader_used = r_0
            .anchor_stage()
            .map(|a| (a.leader, a.is_used.load(atomic::Ordering::Relaxed)));

        // map has full peer set for this round, but maybe with empty value
        let p_0 = Self::collect_points(r_0)?;

        for (peer, versions) in &p_0 {
            let peer_stats = self
                .data
                .entry(*peer)
                .or_insert_with(|| PeerStats::new(r_0.round()));

            peer_stats.last_round = r_0.round().0;

            let mut has_valid = false;
            let mut signed_versions: u32 = 0;

            for version in versions {
                match version {
                    DagPoint::Valid(_) => {
                        has_valid = true;
                        signed_versions += 1;
                    }
                    other if other.is_certified() => {
                        has_valid = true;
                        signed_versions += 1;
                    }
                    DagPoint::Invalid(_) => {
                        peer_stats.invalid_points += 1;
                        signed_versions += 1;
                    }
                    DagPoint::IllFormed(_) => {
                        peer_stats.ill_formed_points += 1;
                        signed_versions += 1;
                    }
                    DagPoint::NotFound(_) => {}
                }
            }
            peer_stats.valid_points += has_valid as u32;
            if signed_versions == 0 {
                peer_stats.skipped_rounds += 1;
            } else {
                peer_stats.equivocated += signed_versions - 1;
            }

            if let Some((leader, used)) = leader_used
                && leader == peer
            {
                if used {
                    peer_stats.was_leader += 1;
                } else {
                    peer_stats.was_not_leader += 1;
                }
            }
        }

        // check references against evidences

        let mut prev_peer_ref_data = mem::replace(
            &mut self.prev_peer_ref_data,
            Self::must_reference(p_0.len(), p_0.values().flatten()),
        );

        if self.last_round == r_0.round().prev() {
            Self::exclude_witness(&mut prev_peer_ref_data, p_0.values().flatten());
            for (signer, ref_data) in prev_peer_ref_data {
                if ref_data.signed.is_empty() {
                    continue;
                }
                if !p_0.contains_key(&signer) {
                    // forgive skipped witness for peers in old v_set out of new v_set
                    continue;
                }
                if !ref_data.has_first_point
                    && !p_0
                        .get(&signer)
                        .into_iter()
                        .flatten()
                        .any(|dag_point| dag_point.trusted().is_some())
                {
                    // forgive peers that do not have a trusted point in both rounds
                    continue;
                }
                // peer may be in `data` and belong to the old v_set, so check above is stronger
                let peer_stats = self.data.get_mut(&signer).expect("must be in set");
                peer_stats.references_skipped += ref_data.signed.len() as u32;
            }
        }

        // finish

        self.last_round = r_0.round();

        Ok(())
    }

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

    fn must_reference<'a>(
        r_0_peers: usize,
        p_0: impl Iterator<Item = &'a DagPoint> + Clone,
    ) -> PeerRefData {
        let mut peer_ref_data = FastHashMap::with_capacity(r_0_peers);

        for point in p_0.clone() {
            if let Some(info) = point.trusted()
                && let Some(signed_digest) = info.prev_digest()
            {
                for signer in info.evidence().keys() {
                    let ref_data = peer_ref_data.entry(*signer).or_insert_with(|| RefData {
                        has_first_point: false,
                        signed: FastHashSet::with_capacity(r_0_peers),
                    });
                    ref_data.signed.insert(*signed_digest);
                }
            }
        }
        for point in p_0 {
            if let Some(info) = point.trusted()
                && let Some(ref_data) = peer_ref_data.get_mut(&info.author())
            {
                ref_data.has_first_point = true;
                for included_digest in info.includes().values() {
                    ref_data.signed.remove(included_digest);
                }
            }
        }

        peer_ref_data
    }

    fn exclude_witness<'a>(
        must_reference: &mut PeerRefData,
        p_1: impl Iterator<Item = &'a DagPoint>,
    ) {
        for point in p_1 {
            if let Some(info) = point.trusted()
                && !info.witness().is_empty()
                && let Some(ref_data) = must_reference.get_mut(&info.author())
                && !ref_data.signed.is_empty()
            {
                for digest in info.witness().values() {
                    ref_data.signed.remove(digest);
                }
            }
        }
    }
}
