use std::sync::Arc;

use rand::distributions::uniform::{UniformInt, UniformSampler};
use rand::Rng;
use tokio::sync::{Mutex, Notify};

use crate::overlay_client::neighbour::Neighbour;

#[derive(Clone)]
#[repr(transparent)]
pub struct Neighbours {
    inner: Arc<Inner>,
}

impl Neighbours {
    pub fn new(entries: Vec<Neighbour>, max_neighbours: usize) -> Self {
        let mut selection_index = SelectionIndex::new(max_neighbours);
        selection_index.update(&entries);

        Self {
            inner: Arc::new(Inner {
                max_neighbours,
                entries: Mutex::new(entries),
                selection_index: Mutex::new(selection_index),
                changed: Notify::new(),
            }),
        }
    }

    pub async fn wait_for_peers(&self, count: usize) {
        loop {
            let changed = self.inner.changed.notified();

            if self.inner.entries.lock().await.len() >= count {
                break;
            }

            changed.await;
        }
    }

    pub fn changed(&self) -> &Notify {
        &self.inner.changed
    }

    pub async fn choose(&self) -> Option<Neighbour> {
        let selection_index = self.inner.selection_index.lock().await;
        selection_index.choose(&mut rand::thread_rng())
    }

    pub async fn choose_multiple(&self, n: usize) -> Vec<Neighbour> {
        let selection_index = self.inner.selection_index.lock().await;
        selection_index.choose_multiple(&mut rand::thread_rng(), n)
    }

    pub async fn update_selection_index(&self) {
        let mut guard = self.inner.entries.lock().await;
        guard.retain(|x| x.is_reliable());
        let mut lock = self.inner.selection_index.lock().await;
        lock.update(guard.as_slice());
    }

    pub async fn get_sorted_neighbours(&self) -> Vec<(Neighbour, u32)> {
        let mut index = self.inner.selection_index.lock().await;
        index
            .indices_with_weights
            .sort_by(|(_, lw), (_, rw)| rw.cmp(lw));
        return Vec::from(index.indices_with_weights.as_slice());
    }

    pub async fn get_active_neighbours(&self) -> Vec<Neighbour> {
        self.inner.entries.lock().await.as_slice().to_vec()
    }

    pub async fn update(&self, new: Vec<Neighbour>) {
        let now = tycho_util::time::now_sec();

        let mut changed = false;

        let mut guard = self.inner.entries.lock().await;
        // remove unreliable and expired neighbours
        guard.retain(|x| {
            let retain = x.is_reliable() && x.expires_at_secs() > now;
            changed |= !retain;
            retain
        });

        // if all neighbours are reliable and valid then remove the worst
        if guard.len() >= self.inner.max_neighbours {
            if let Some(worst) = guard
                .iter()
                .min_by(|l, r| l.get_stats().score.cmp(&r.get_stats().score))
            {
                if let Some(index) = guard.iter().position(|x| x.peer_id() == worst.peer_id()) {
                    guard.remove(index);
                    changed = true;
                }
            }
        }

        for n in new {
            if guard.iter().any(|x| x.peer_id() == n.peer_id()) {
                continue;
            }
            if guard.len() < self.inner.max_neighbours {
                guard.push(n);
                changed = true;
            }
        }

        drop(guard);
        self.update_selection_index().await;

        if changed {
            self.inner.changed.notify_waiters();
        }
    }

    pub async fn remove_outdated_neighbours(&self) {
        let now = tycho_util::time::now_sec();

        let mut guard = self.inner.entries.lock().await;
        // remove unreliable and expired neighbours
        guard.retain(|x| x.expires_at_secs() > now);
        drop(guard);
        self.update_selection_index().await;
    }
}

struct Inner {
    max_neighbours: usize,
    entries: Mutex<Vec<Neighbour>>,
    selection_index: Mutex<SelectionIndex>,
    changed: Notify,
}

struct SelectionIndex {
    /// Neighbour indices with cumulative weight.
    indices_with_weights: Vec<(Neighbour, u32)>,
    /// Optional uniform distribution `[0; total_weight)`.
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

    fn choose<R: Rng + ?Sized>(&self, rng: &mut R) -> Option<Neighbour> {
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

    fn choose_multiple<R: Rng + ?Sized>(&self, rng: &mut R, mut n: usize) -> Vec<Neighbour> {
        struct Element<'a> {
            key: f64,
            neighbour: &'a Neighbour,
        }

        impl Eq for Element<'_> {}
        impl PartialEq for Element<'_> {
            #[inline]
            fn eq(&self, other: &Self) -> bool {
                // Bitwise comparison is safe
                self.key == other.key
            }
        }

        impl PartialOrd for Element<'_> {
            #[inline]
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Ord for Element<'_> {
            #[inline]
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                // Weight must not be nan
                self.key.partial_cmp(&other.key).unwrap()
            }
        }

        n = std::cmp::min(n, self.indices_with_weights.len());
        if n == 0 {
            return Vec::new();
        }

        let mut candidates = Vec::with_capacity(self.indices_with_weights.len());
        let mut prev_total_weight = None;
        for (neighbour, total_weight) in &self.indices_with_weights {
            let weight = match prev_total_weight {
                Some(prev) => total_weight - prev,
                None => *total_weight,
            };
            prev_total_weight = Some(*total_weight);

            debug_assert!(weight > 0);

            let key = rng.gen::<f64>().powf(1.0 / weight as f64);
            candidates.push(Element { key, neighbour });
        }

        // Partially sort the array to find the `n` elements with the greatest
        // keys. Do this by using `select_nth_unstable` to put the elements with
        // the *smallest* keys at the beginning of the list in `O(n)` time, which
        // provides equivalent information about the elements with the *greatest* keys.
        let (_, mid, greater) = candidates.select_nth_unstable(self.indices_with_weights.len() - n);

        let mut result = Vec::with_capacity(n);
        result.push(mid.neighbour.clone());
        for element in greater {
            result.push(element.neighbour.clone());
        }

        result
    }
}
