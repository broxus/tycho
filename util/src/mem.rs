use std::collections::BTreeMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, Mutex, MutexGuard};

use bytesize::ByteSize;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct MemorySlicer {
    range: MemorySlicerRange,
    total: ByteSize,
    inner: Arc<Inner>,
}

impl MemorySlicer {
    pub fn new(range: MemorySlicerRange) -> Self {
        let total = range.measure_available();
        Self {
            range,
            total,
            inner: Arc::new(Inner {
                available: Mutex::new(total),
            }),
        }
    }

    pub fn range(&self) -> MemorySlicerRange {
        self.range
    }

    pub fn total(&self) -> ByteSize {
        self.total
    }

    pub fn lock(&self) -> MemorySlicerGuard<'_> {
        MemorySlicerGuard {
            range: self.range,
            total: self.total,
            available: self.inner.available.lock().unwrap(),
        }
    }
}

pub struct MemorySlicerGuard<'a> {
    range: MemorySlicerRange,
    total: ByteSize,
    available: MutexGuard<'a, ByteSize>,
}

impl MemorySlicerGuard<'_> {
    pub fn range(&self) -> MemorySlicerRange {
        self.range
    }

    pub fn total(&self) -> ByteSize {
        self.total
    }

    pub fn available(&self) -> ByteSize {
        *self.available
    }

    pub fn snapshot(&self) -> MemorySlicer {
        let available = *self.available;
        MemorySlicer {
            range: MemorySlicerRange::Fixed {
                capacity: self.total,
            },
            total: self.total,
            inner: Arc::new(Inner {
                available: Mutex::new(available),
            }),
        }
    }

    pub fn alloc_fixed(&mut self, bytes: ByteSize) -> bool {
        if let Some(available) = self.available.0.checked_sub(bytes.0) {
            *self.available = ByteSize(available);
            true
        } else {
            false
        }
    }

    pub fn alloc_ratio(&mut self, nom: usize, denom: usize) -> Option<ByteSize> {
        // TODO: Panic here?
        if nom > denom || denom == 0 {
            return None;
        }
        let to_alloc = self.available.0.saturating_mul(nom as u64) / (denom as u64);
        self.available.0 -= to_alloc;
        (to_alloc != 0).then_some(ByteSize(to_alloc))
    }

    pub fn alloc_in_range<R: RangeBounds<ByteSize>>(&mut self, bytes: R) -> Option<ByteSize> {
        let min_alloc = match bytes.start_bound() {
            Bound::Included(bytes) => bytes.0,
            Bound::Excluded(bytes) => bytes.0.saturating_add(1),
            Bound::Unbounded => 0,
        };

        let mut to_alloc = match bytes.end_bound() {
            Bound::Included(bytes) => bytes.0,
            Bound::Excluded(bytes) => bytes.0.saturating_sub(1),
            Bound::Unbounded => u64::MAX,
        };
        to_alloc = std::cmp::min(to_alloc, self.available.0);
        if to_alloc < min_alloc {
            return None;
        }

        self.available.0 -= to_alloc;
        Some(ByteSize(to_alloc))
    }

    /// Tries to allocate an amount of memory which satisfies provided
    /// constraints.
    ///
    /// On success returns allocated amounts in the same order as constraints.
    /// Otherwise returns the minimum required memory size (total).
    pub fn alloc_constraints<C: MemoryConstraints>(
        &mut self,
        constraints: C,
    ) -> Result<C::Output, ByteSize> {
        let (solved, result) =
            solve_constraints(*self.available, constraints.as_constraitns_slice());
        let result_total = result.iter().map(|size| size.as_u64()).sum();

        if let Some(available) = self.available.0.checked_sub(result_total) {
            if solved {
                self.available.0 = available;
                return Ok(C::make_output(result));
            }
        }

        Err(ByteSize(result_total))
    }

    /// Tries to allocate an amount of memory which satisfies provided
    /// constraints.
    ///
    /// In case of overflow still allocates, but returns the required remainder
    /// in [`AllocatedMemoryConstraints::overflow`].
    pub fn overflowing_alloc_constraints<C: MemoryConstraints>(
        &mut self,
        constraints: C,
    ) -> AllocatedMemoryConstraints<C::Output> {
        let (_, result) = solve_constraints(*self.available, constraints.as_constraitns_slice());
        let result_total = result.iter().map(|size| size.as_u64()).sum();

        let allocated = std::cmp::min(result_total, self.available.0);
        self.available.0 -= allocated;

        AllocatedMemoryConstraints {
            result: C::make_output(result),
            total: ByteSize(result_total),
            overflow: (result_total > allocated).then(|| ByteSize(result_total - allocated)),
        }
    }

    pub fn subdivide(&mut self, bytes: ByteSize) -> Option<MemorySlicer> {
        if self.alloc_fixed(bytes) {
            Some(MemorySlicer::new(MemorySlicerRange::Fixed {
                capacity: bytes,
            }))
        } else {
            None
        }
    }

    pub fn subdivide_ratio(&mut self, nom: usize, denom: usize) -> Option<MemorySlicer> {
        self.alloc_ratio(nom, denom)
            .map(|capacity| MemorySlicer::new(MemorySlicerRange::Fixed { capacity }))
    }

    pub fn subdivide_in_range<R: RangeBounds<ByteSize>>(
        &mut self,
        bytes: R,
    ) -> Option<MemorySlicer> {
        self.alloc_in_range(bytes)
            .map(|capacity| MemorySlicer::new(MemorySlicerRange::Fixed { capacity }))
    }
}

impl From<MemorySlicerRange> for MemorySlicer {
    #[inline]
    fn from(value: MemorySlicerRange) -> Self {
        Self::new(value)
    }
}

struct Inner {
    available: Mutex<ByteSize>,
}

#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum MemorySlicerRange {
    #[default]
    Available,
    Physical,
    Fixed {
        capacity: ByteSize,
    },
}

impl MemorySlicerRange {
    pub fn fixed(capacity: ByteSize) -> Self {
        Self::Fixed { capacity }
    }

    pub fn into_slicer(self) -> MemorySlicer {
        MemorySlicer::new(self)
    }

    pub fn measure_available(&self) -> ByteSize {
        match self {
            Self::Available => {
                let mut sys = sysinfo::System::new();
                sys.refresh_memory();
                ByteSize(sys.available_memory())
            }
            // TODO: Add support for cgroups?
            Self::Physical => {
                let mut sys = sysinfo::System::new();
                sys.refresh_memory();
                ByteSize(sys.total_memory())
            }
            Self::Fixed { capacity } => *capacity,
        }
    }
}

pub struct AllocatedMemoryConstraints<T> {
    pub result: T,
    pub total: ByteSize,
    pub overflow: Option<ByteSize>,
}

pub trait MemoryConstraints {
    type Output;

    fn make_output(result: Vec<ByteSize>) -> Self::Output;

    // NOTE: Not using `as_slice` to annoying import in other places.
    fn as_constraitns_slice(&self) -> &[MemoryConstraint];
}

impl<const N: usize> MemoryConstraints for [MemoryConstraint; N] {
    type Output = [ByteSize; N];

    fn make_output(result: Vec<ByteSize>) -> Self::Output {
        result.try_into().unwrap()
    }

    fn as_constraitns_slice(&self) -> &[MemoryConstraint] {
        self.as_slice()
    }
}

impl MemoryConstraints for Vec<MemoryConstraint> {
    type Output = Vec<ByteSize>;

    fn make_output(result: Vec<ByteSize>) -> Self::Output {
        result
    }

    fn as_constraitns_slice(&self) -> &[MemoryConstraint] {
        self.as_slice()
    }
}

#[derive(Debug, Clone)]
pub struct MemoryConstraint {
    priority: usize,
    ratio: NonZeroUsize,
    min_bytes: ByteSize,
    max_bytes: ByteSize,
}

impl MemoryConstraint {
    pub const HIGH_PRIORITY: usize = 0;
    pub const MID_PRIORITY: usize = 1;
    pub const LOW_PRIORITY: usize = 2;

    pub fn exact(priority: usize, bytes: ByteSize) -> Self {
        Self {
            priority,
            ratio: NonZeroUsize::MIN,
            min_bytes: bytes,
            max_bytes: bytes,
        }
    }

    pub fn range<R>(priority: usize, ratio: usize, range: R) -> Self
    where
        R: RangeBounds<ByteSize>,
    {
        let min_bytes = match range.start_bound() {
            Bound::Included(bytes) => *bytes,
            Bound::Excluded(bytes) => ByteSize(bytes.as_u64().saturating_add(1)),
            Bound::Unbounded => ByteSize(0),
        };
        let max_bytes = match range.end_bound() {
            Bound::Included(bytes) => *bytes,
            Bound::Excluded(bytes) => ByteSize(bytes.as_u64().saturating_sub(1)),
            Bound::Unbounded => ByteSize(u64::MAX),
        };
        assert!(min_bytes <= max_bytes);

        Self {
            priority,
            ratio: NonZeroUsize::new(ratio).unwrap_or(NonZeroUsize::MIN),
            min_bytes,
            max_bytes,
        }
    }
}

fn solve_constraints(total: ByteSize, constraitns: &[MemoryConstraint]) -> (bool, Vec<ByteSize>) {
    struct Item {
        idx: usize,
        ratio: Option<NonZeroU64>,
        max: NonZeroU64,
    }

    #[derive(Default)]
    struct Group {
        total_ratio: u64,
        items: Vec<Item>,
    }

    const SCALE: u64 = 1 << 16;

    let mut total = total.as_u64();
    let mut result = Vec::with_capacity(constraitns.len());
    let mut groups = BTreeMap::<usize, Group>::new();

    // Group constraints by priorities.
    let mut min_required = 0u64;
    for (idx, constraint) in constraitns.iter().enumerate() {
        min_required = min_required.saturating_add(constraint.min_bytes.as_u64());
        result.push(constraint.min_bytes);

        let range = constraint.max_bytes.0 - constraint.min_bytes.0;
        if let Some(max) = NonZeroU64::new(range) {
            let group = groups.entry(constraint.priority).or_default();
            group.total_ratio = group
                .total_ratio
                .checked_add(constraint.ratio.get() as u64)
                .expect("too big total ratio");
            group.items.push(Item {
                idx,
                ratio: NonZeroU64::new(constraint.ratio.get() as _),
                max,
            });
        }
    }

    // Consume the minimum required amount first.
    let solved = total >= min_required;
    total = total.saturating_sub(min_required);

    // Distribute memory inside groups.
    'outer: for mut group in groups.into_values() {
        // TODO: Replace loop with some math (but this might lose some precision).
        while total > 0 && group.total_ratio > 0 {
            // Compute the size of a chunk per ratio unit.
            // NOTE: We additionally multiply by `SCALE` to preserve some precision.
            let chunk = total.saturating_mul(SCALE) / group.total_ratio;

            // Try to complete some ranges first.
            let mut has_complete = false;
            for item in &mut group.items {
                // Skip items without ratio (i.e. complete)
                let Some(ratio) = item.ratio else {
                    continue;
                };

                // Compute the suggested memory for this item`s ratio.
                let available = chunk.saturating_mul(ratio.get()) / SCALE;
                if available >= item.max.get() {
                    // Found a complete constraint.

                    // Add the maximum available memory to it.
                    let slot = &mut result[item.idx];
                    slot.0 = slot.0.saturating_add(item.max.get());

                    // Decrease the total amount by the distributed amount.
                    total -= item.max.get();
                    // Remove item ratio from the group.
                    group.total_ratio -= ratio.get();
                    // Mark item as complete.
                    item.ratio = None;
                    // Update the flag.
                    has_complete = true;
                }
            }

            // Recompute chunk size if some constraints were complete.
            if has_complete {
                continue;
            }

            // At this point we will need to distribute the remaining memory across this group.
            for item in &group.items {
                // Skip items without ratio (i.e. complete)
                let Some(ratio) = item.ratio else {
                    continue;
                };

                let available = chunk.saturating_mul(ratio.get()) / SCALE;

                // Add the available memory to the result slot.
                let slot = &mut result[item.idx];
                slot.0 = slot.0.saturating_add(available);

                // Decrease the total amount by the distributed amount.
                total -= available;
            }
            continue 'outer;
        }
    }

    (solved, result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_slicer_works() {
        let slicer = MemorySlicerRange::fixed(ByteSize::gb(32)).into_slicer();

        let subslicer = slicer.lock().subdivide_ratio(2, 3).unwrap();

        let [fixed, large_range, small_range] = subslicer
            .lock()
            .alloc_constraints([
                MemoryConstraint::exact(0, ByteSize::gb(8)),
                MemoryConstraint::range(1, 1, ByteSize::mb(128)..=ByteSize::gb(8)),
                MemoryConstraint::range(1, 10, ByteSize::mb(128)..=ByteSize::gb(4)),
            ])
            .unwrap();

        assert_eq!(fixed, ByteSize::gb(8));
        assert_eq!(large_range, ByteSize::gb(8));
        assert_eq!(small_range, ByteSize::gb(4));

        println!("fixed={fixed}, large_range={large_range}, small_range={small_range}");
        println!("available_outer={}", slicer.lock().available());
        println!("available_inner={}", subslicer.lock().available());
    }

    #[test]
    fn constraints_solver_works() {
        #[derive(Debug)]
        struct Task {
            total: ByteSize,
            solved: bool,
        }

        for task in [
            Task {
                total: ByteSize::gb(4),
                solved: false,
            },
            Task {
                total: ByteSize::gb(6),
                solved: true,
            },
            Task {
                total: ByteSize::gb(8),
                solved: true,
            },
            Task {
                total: ByteSize::gb(16),
                solved: true,
            },
        ] {
            let constraitns = [
                MemoryConstraint::exact(0, ByteSize::gb(4)),
                MemoryConstraint::range(1, 1, ByteSize::gb(1)..),
                MemoryConstraint::range(1, 10, ByteSize::mb(512)..=ByteSize::gb(1)),
            ];
            let (solved, sizes) = solve_constraints(task.total, &constraitns);
            assert_eq!(solved, task.solved);

            println!("{task:?}");
            println!("{sizes:#?}");
        }
    }
}
