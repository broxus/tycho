use std::sync::Arc;

use arc_swap::ArcSwap;
use parking_lot::Mutex;
use rand::distributions::uniform::{UniformInt, UniformSampler};
use rand::Rng;
use tokio::sync::Notify;
use tycho_util::FastHashSet;

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
                entries: ArcSwap::new(Arc::new(entries)),
                selection_index: Mutex::new(selection_index),
                changed: Notify::new(),
            }),
        }
    }

    pub async fn wait_for_peers(&self, count: usize) {
        loop {
            let changed = self.inner.changed.notified();

            if self.inner.entries.load().len() >= count {
                break;
            }

            changed.await;
        }
    }

    pub fn changed(&self) -> &Notify {
        &self.inner.changed
    }

    pub fn choose(&self) -> Option<Neighbour> {
        let selection_index = self.inner.selection_index.lock();
        selection_index.choose(&mut rand::thread_rng())
    }

    pub fn choose_multiple(&self, n: usize) -> Vec<Neighbour> {
        let selection_index = self.inner.selection_index.lock();
        selection_index.choose_multiple(&mut rand::thread_rng(), n)
    }

    /// Tries to apply neighbours score to selection index.
    /// Returns `true` if new values were stored. Did nothing otherwise.
    pub fn try_apply_score(&self, now: u32) -> bool {
        let entires_arc = self.inner.entries.load_full();

        let mut entries = entires_arc.as_ref().clone();
        let mut entries_changed = false;
        entries.retain(|x| {
            let retain = x.is_reliable() && x.expires_at_secs() > now;
            entries_changed |= !retain;
            retain
        });
        let new_entries_arc = Arc::new(entries);

        let mut lock = self.inner.selection_index.lock();

        {
            let prev_entires_arc = self
                .inner
                .entries
                .compare_and_swap(&entires_arc, new_entries_arc.clone());

            if !Arc::ptr_eq(&prev_entires_arc, &entires_arc) {
                // Entires were already changed, we must not overwrite the index
                return false;
            }
        }

        // Recompute distribution
        lock.update(new_entries_arc.as_ref());

        if entries_changed {
            // Notify waiters if some peers were removed
            self.inner.changed.notify_waiters();
        }

        true
    }

    pub fn get_sorted_neighbours(&self) -> Vec<(Neighbour, u32)> {
        let mut index = self.inner.selection_index.lock();
        index
            .indices_with_weights
            .sort_by(|(_, lw), (_, rw)| rw.cmp(lw));
        index.indices_with_weights.clone()
    }

    pub fn get_active_neighbours(&self) -> Arc<Vec<Neighbour>> {
        self.inner.entries.load_full()
    }

    pub fn update(&self, new: Vec<Neighbour>) {
        let now = tycho_util::time::now_sec();

        let mut new_peer_ids = new
            .iter()
            .map(|neighbour| *neighbour.peer_id())
            .collect::<FastHashSet<_>>();

        let mut entries = self.inner.entries.load().as_slice().to_vec();

        // Remove unreliable and expired neighbours.
        let mut changed = false;
        entries.retain(|x| {
            // Remove the existing peer from the `new_peers` list to prevent it
            // from appearing in the same list again (especially if it was unreliable).
            new_peer_ids.remove(x.peer_id());

            let retain = x.is_reliable() && x.expires_at_secs() > now;
            changed |= !retain;
            retain
        });

        // If all neighbours are reliable and valid then remove the worst
        if entries.len() >= self.inner.max_neighbours {
            if let Some((worst_index, _)) = entries
                .iter()
                .enumerate()
                .min_by(|(_, l), (_, r)| l.cmp_score(r))
            {
                entries.swap_remove(worst_index);
                changed = true;
            }
        }

        for neighbour in new {
            if entries.len() >= self.inner.max_neighbours {
                break;
            }
            if !new_peer_ids.contains(neighbour.peer_id()) {
                continue;
            }

            entries.push(neighbour);
            changed = true;
        }

        let new_entries_arc = Arc::new(entries);
        let mut lock = self.inner.selection_index.lock();

        // Overwrite current entries
        self.inner.entries.store(new_entries_arc.clone());
        // Recompute distribution
        lock.update(new_entries_arc.as_ref());

        if changed {
            // Notify waiter if some peers were added or removed
            self.inner.changed.notify_waiters();
        }
    }
}

struct Inner {
    max_neighbours: usize,
    entries: ArcSwap<Vec<Neighbour>>,
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
