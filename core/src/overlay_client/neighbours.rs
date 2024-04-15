use crate::overlay_client::public_overlay_client::Peer;
use crate::overlay_client::settings::NeighboursOptions;
use rand::distributions::uniform::{UniformInt, UniformSampler};
use rand::Rng;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::neighbour::{Neighbour, NeighbourOptions};

pub struct NeighbourCollection(pub Arc<Neighbours>);

pub struct Neighbours {
    options: NeighboursOptions,
    entries: Mutex<Vec<Neighbour>>,
    selection_index: Mutex<SelectionIndex>,
}

impl Neighbours {
    pub async fn new(peers: Vec<Peer>, options: NeighboursOptions) -> Arc<Self> {
        let neighbour_options = NeighbourOptions {
            default_roundtrip_ms: options.default_roundtrip_ms,
        };

        let entries = peers
            .iter()
            .map(|x| Neighbour::new(x.id, x.expires_at, neighbour_options))
            .collect::<Vec<_>>();

        let selection_index = Mutex::new(SelectionIndex::new(options.max_neighbours));

        let result = Self {
            options,
            entries: Mutex::new(entries),
            selection_index,
        };

        tracing::info!("Initial update selection call");
        result.update_selection_index().await;
        tracing::info!("Initial update selection finished");

        Arc::new(result)
    }

    pub fn options(&self) -> &NeighboursOptions {
        &self.options
    }

    pub async fn choose(&self) -> Option<Neighbour> {
        self.selection_index
            .lock()
            .await
            .get(&mut rand::thread_rng())
    }

    pub async fn update_selection_index(&self) {
        let mut guard = self.entries.lock().await;
        guard.retain(|x| x.is_reliable());
        let mut lock = self.selection_index.lock().await;
        lock.update(guard.as_slice());
    }

    pub async fn get_sorted_neighbours(&self) -> Vec<(Neighbour, u32)> {
        let mut index = self.selection_index.lock().await;
        index
            .indices_with_weights
            .sort_by(|(_, lw), (_, rw)| rw.cmp(lw));
        return Vec::from(index.indices_with_weights.as_slice());
    }

    pub async fn get_active_neighbours(&self) -> Vec<Neighbour> {
        Vec::from(self.entries.lock().await.as_slice())
    }

    pub async fn update(&self, new: Vec<Neighbour>) {
        let now = tycho_util::time::now_sec();
        let mut guard = self.entries.lock().await;
        //remove unreliable and expired neighbours
        guard.retain(|x| x.is_reliable() && x.expires_at_secs() > now);

        //if all neighbours are reliable and valid then remove the worst
        if guard.len() >= self.options.max_neighbours {
            if let Some(worst) = guard
                .iter()
                .min_by(|l, r| l.get_stats().score.cmp(&r.get_stats().score))
            {
                if let Some(index) = guard.iter().position(|x| x.peer_id() == worst.peer_id()) {
                    guard.remove(index);
                }
            }
        }

        for n in new {
            if guard
                .iter()
                .any(|x| x.peer_id() == n.peer_id()) {
                continue;
            }
            if guard.len() < self.options.max_neighbours {
                guard.push(n);
            }
        }

        drop(guard);
        self.update_selection_index().await;
    }

    pub async fn remove_outdated_neighbours(&self) {
        let now = tycho_util::time::now_sec();
        let mut guard = self.entries.lock().await;
        //remove unreliable and expired neighbours
        guard.retain(|x| x.expires_at_secs() > now);
        drop(guard);
        self.update_selection_index().await;
    }
}

struct SelectionIndex {
    /// Neighbour indices with cumulative weight.
    indices_with_weights: Vec<(Neighbour, u32)>,
    /// Optional uniform distribution [0; total_weight).
    distribution: Option<UniformInt<u32>>,
}

impl SelectionIndex {
    fn new(capacity: usize) -> Self {
        Self {
            indices_with_weights: Vec::with_capacity(capacity),
            distribution: None,
        }
    }

    fn update(&mut self, neighbours: &[Neighbour]) {
        self.indices_with_weights.clear();
        let mut total_weight = 0;
        for neighbour in neighbours.iter() {
            if let Some(score) = neighbour.compute_selection_score() {
                total_weight += score as u32;
                self.indices_with_weights
                    .push((neighbour.clone(), total_weight));
            }
        }

        self.distribution = if total_weight != 0 {
            Some(UniformInt::new(0, total_weight))
        } else {
            None
        };

        // TODO: fallback to uniform sample from any neighbour
    }

    fn get<R: Rng + ?Sized>(&self, rng: &mut R) -> Option<Neighbour> {
        let chosen_weight = self.distribution.as_ref()?.sample(rng);

        // Find the first item which has a weight higher than the chosen weight.
        let i = self
            .indices_with_weights
            .partition_point(|(_, w)| *w <= chosen_weight);

        self.indices_with_weights
            .get(i)
            .map(|(neighbour, _)| neighbour)
            .cloned()
    }
}
