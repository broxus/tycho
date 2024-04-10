use rand::distributions::uniform::{UniformInt, UniformSampler};
use rand::seq::SliceRandom;
use rand::Rng;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tycho_network::{OverlayId, PeerId};

use super::neighbour::{Neighbour, NeighbourOptions};


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NeighboursOptions {
    pub max_neighbours: usize,
    pub max_ping_tasks: usize,
    pub default_roundtrip_ms: u64,
}

impl Default for NeighboursOptions {
    fn default() -> Self {
        Self {
            max_neighbours: 16,
            max_ping_tasks: 6,
            default_roundtrip_ms: 2000,
        }
    }
}

pub struct NeighbourCollection(pub Arc<Neighbours>);

pub struct Neighbours {
    options: NeighboursOptions,
    entries: Mutex<Vec<Neighbour>>,
    selection_index: Mutex<SelectionIndex>,
    overlay_id: OverlayId,
}

impl Neighbours {
    pub async fn new(
        options: NeighboursOptions,
        initial: Vec<PeerId>,
        overlay_id: OverlayId,
    ) -> Arc<Self> {
        let neighbour_options = NeighbourOptions {
            default_roundtrip_ms: options.default_roundtrip_ms,
        };

        let entries = initial
            .choose_multiple(&mut rand::thread_rng(), options.max_neighbours)
            .map(|&peer_id| Neighbour::new(peer_id, neighbour_options))
            .collect();

        let entries = Mutex::new(entries);

        let selection_index = Mutex::new(SelectionIndex::new(options.max_neighbours));

        let result = Self {
            options,
            entries,
            selection_index,
            overlay_id,
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
        let guard = self.entries.lock().await;
        let mut lock = self.selection_index.lock().await;
        lock.update(guard.as_slice());
    }

    pub async fn get_bad_neighbours_count(&self) -> usize {
        let guard = self.entries.lock().await;
        guard
            .iter()
            .filter(|x| !x.is_reliable())
            .cloned()
            .collect::<Vec<_>>()
            .len()
    }

    pub async fn update(&self, entries: &[Neighbour]) {
        const MINIMAL_NEIGHBOUR_COUNT: usize = 16;
        let mut guard = self.entries.lock().await;

        guard.sort_by(|a, b| a.get_stats().score.cmp(&b.get_stats().score));

        let mut all_reliable = true;

        for entry in entries {
            if let Some(index) = guard.iter().position(|x| x.peer_id() == entry.peer_id()) {
                let nbg = guard.get(index).unwrap();

                if !nbg.is_reliable() && guard.len() > MINIMAL_NEIGHBOUR_COUNT {
                    guard.remove(index);
                    all_reliable = false;
                }
            } else {
                guard.push(entry.clone());
            }
        }

        //if everything is reliable then remove the worst node
        if all_reliable && guard.len() > MINIMAL_NEIGHBOUR_COUNT {
            guard.pop();
        }

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


#[cfg(test)]
mod tests {

    use super::*;
    use weighted_rand::builder::*;

    #[tokio::test]
    pub async fn test() {
        let neighbours = create_neighbours();
        //let neighbours = Neighbours::new()
        //let n_collection = NeighbourCollection(Arc::new(neighbours));


        let index_weights = [0.55, 0.1, 0.3, 0.8, 0.0];
        let builder = WalkerTableBuilder::new(&index_weights);
        let wa_table = builder.build();

        for i in (0..10).map(|_| wa_table.next()) {
            println!("{:?}", neighbours[i].peer_id());
        }


    }

    // pub fn synthetic_ping(n: Neighbour) {
    //     let index_weights = [0.55, 0.1, 0.3, 0.8, 0.0];
    //     let builder = WalkerTableBuilder::new(&index_weights);
    //     let wa_table = builder.build();
    //     wa_table.next()
    // }

    pub fn create_neighbours() -> Vec<Neighbour> {
        let mut i = 0;
        let mut neighbours = Vec::new();
        while i < 5 {
            let n = Neighbour::new(PeerId([i;32]), NeighbourOptions {
                default_roundtrip_ms: 200,
            });
            neighbours.push(n)
        }

        neighbours

    }
}
