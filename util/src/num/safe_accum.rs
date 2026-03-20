use std::ops::Deref;

pub struct SafeAccum<T> {
    value: T,
    shift: u32,
    counter: u128,
}

impl<T> SafeAccum<T> {
    /// Returns the current global right-shift applied to accumulated values.
    pub const fn shift(&self) -> u32 {
        self.shift
    }

    /// Returns the number of values successfully added into this accumulator.
    pub const fn counter(&self) -> u128 {
        self.counter
    }
}

impl<T> Deref for SafeAccum<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

trait ShiftToZero: Copy + PartialEq + std::ops::Shr<u32, Output = Self> {
    const BITS: u32;
    const ZERO: Self;
}

trait SafeAccumScalar:
    ShiftToZero + std::ops::Div<Output = Self> + std::ops::Rem<Output = Self>
{
    /// Adds two scalar values and reports overflow via `None`.
    fn checked_add(self, rhs: Self) -> Option<Self>;
    /// Left-shifts a scalar value and reports overflow via `None`.
    fn checked_shl(self, rhs: u32) -> Option<Self>;
    /// Converts the global sample counter into the scalar domain.
    fn from_u128(value: u128) -> Option<Self>;
}

impl ShiftToZero for u128 {
    const BITS: u32 = u128::BITS;
    const ZERO: Self = 0;
}

impl ShiftToZero for i128 {
    const BITS: u32 = i128::BITS;
    const ZERO: Self = 0;
}

impl SafeAccumScalar for u128 {
    /// Uses the native checked addition for unsigned 128-bit integers.
    fn checked_add(self, rhs: Self) -> Option<Self> {
        u128::checked_add(self, rhs)
    }

    /// Uses the native checked left shift for unsigned 128-bit integers.
    fn checked_shl(self, rhs: u32) -> Option<Self> {
        u128::checked_shl(self, rhs)
    }

    /// All `u128` counters are representable in `u128`.
    fn from_u128(value: u128) -> Option<Self> {
        Some(value)
    }
}

impl SafeAccumScalar for i128 {
    /// Uses the native checked addition for signed 128-bit integers.
    fn checked_add(self, rhs: Self) -> Option<Self> {
        i128::checked_add(self, rhs)
    }

    /// Uses the native checked left shift for signed 128-bit integers.
    fn checked_shl(self, rhs: u32) -> Option<Self> {
        i128::checked_shl(self, rhs)
    }

    /// Fails when the counter does not fit into `i128`.
    fn from_u128(value: u128) -> Option<Self> {
        i128::try_from(value).ok()
    }
}

/// Applies a safe right shift where values beyond bit width collapse to zero.
fn shr_num<T: ShiftToZero>(value: T, shift: u32) -> T {
    if shift >= T::BITS {
        T::ZERO
    } else {
        value >> shift
    }
}

/// Accumulates a single scalar component and returns `(new_accum, delta_shift)`.
fn accum_scalar_value<T: SafeAccumScalar>(accum: T, value: T, shift: u32) -> (T, u32) {
    let mut accum = accum;
    let mut value = shr_num(value, shift);
    let mut d_shift = 0;
    loop {
        if let Some(sum) = accum.checked_add(value) {
            return (sum, d_shift);
        }
        accum = accum >> 1;
        value = value >> 1;
        d_shift = d_shift.saturating_add(1);
    }
}

/// Multiplies a scalar value by `2^shift` with explicit overflow handling.
fn mul_pow2_checked<T: SafeAccumScalar>(value: T, shift: u32) -> Option<T> {
    if shift >= T::BITS {
        if value == T::ZERO {
            Some(T::ZERO)
        } else {
            None
        }
    } else {
        value.checked_shl(shift)
    }
}

/// Computes the approximate average using the scaled-sum decomposition formula.
fn approx_avg_value<T: SafeAccumScalar>(sum: T, counter: u128, shift: u32) -> Option<T> {
    // Step 1: No samples means no average.
    if counter == 0 {
        return None;
    }
    // Step 2: Convert `counter` into the scalar type.
    let counter = T::from_u128(counter)?;
    // Step 3: Compute the integer whole part of `sum / counter`.
    let whole = sum / counter;
    // Step 4: Compute the remainder part of `sum / counter`.
    let rem = sum % counter;
    // Step 5: Scale the whole part by `2^shift`.
    let whole_scaled = mul_pow2_checked(whole, shift)?;
    // Step 6: Scale the remainder by `2^shift`.
    let rem_scaled = mul_pow2_checked(rem, shift)?;
    // Step 7: Divide the scaled remainder by `counter`.
    let rem_part = rem_scaled / counter;
    // Step 8: Combine both parts into the final approximate average.
    whole_scaled.checked_add(rem_part)
}

#[allow(private_bounds)]
impl<T: SafeAccumScalar> SafeAccum<T> {
    /// Creates an empty scalar accumulator with zeroed value, shift, and counter.
    pub const fn new() -> Self {
        Self {
            value: T::ZERO,
            shift: 0,
            counter: 0,
        }
    }

    /// Adds a new scalar value using overflow-aware accumulation.
    pub fn add(&mut self, value: T) {
        let (accum, d_shift) = accum_scalar_value(self.value, value, self.shift);
        self.value = accum;
        self.shift = self.shift.saturating_add(d_shift);
        self.counter = self.counter.saturating_add(1);
    }

    /// Returns the approximate average reconstructed from scaled sum and counter.
    pub fn approx_avg(&self) -> Option<T> {
        approx_avg_value(self.value, self.counter, self.shift)
    }

    /// Merges another scalar accumulator into this one preserving shift semantics.
    pub fn merge(&mut self, other: &Self) {
        if other.counter == 0 {
            return;
        }
        if self.counter == 0 {
            self.value = other.value;
            self.shift = other.shift;
            self.counter = other.counter;
            return;
        }
        let mut shift = self.shift.max(other.shift);
        let mut lhs = shr_num(self.value, shift.saturating_sub(self.shift));
        let mut rhs = shr_num(other.value, shift.saturating_sub(other.shift));
        loop {
            if let Some(sum) = lhs.checked_add(rhs) {
                self.value = sum;
                self.shift = shift;
                self.counter = self.counter.saturating_add(other.counter);
                return;
            }
            lhs = lhs >> 1;
            rhs = rhs >> 1;
            shift = shift.saturating_add(1);
        }
    }
}

#[allow(private_bounds)]
impl<T: SafeAccumScalar> Default for SafeAccum<T> {
    fn default() -> Self {
        Self::new()
    }
}

trait ShiftTupleAll {
    /// Applies a uniform right shift to all tuple components.
    fn shift_all(self, shift: u32) -> Self;
}

macro_rules! impl_safe_accum_tuple_impl {
    ($($ty:ident : $idx:tt : $avg_method:ident),+ $(,)?) => {
        #[allow(private_bounds)]
        impl<$($ty: SafeAccumScalar),+> ShiftTupleAll for ($($ty,)+) {
            /// Shifts every tuple component by the same amount.
            fn shift_all(self, shift: u32) -> Self {
                ($(shr_num(self.$idx, shift),)+)
            }
        }

        #[allow(private_bounds)]
        impl<$($ty: SafeAccumScalar),+> SafeAccum<($($ty,)+)> {
            /// Creates an empty tuple accumulator with shared shift and counter.
            pub const fn new() -> Self {
                Self {
                    value: ($($ty::ZERO,)+),
                    shift: 0,
                    counter: 0,
                }
            }

            /// Adds a tuple value while keeping all components on the same shift scale.
            pub fn add(&mut self, value: ($($ty,)+)) {
                // Track the working shared shift for this add operation.
                let mut shift = self.shift;
                // Keep a mutable snapshot of current tuple sums.
                let mut accum = self.value;
                // Restart when any component forces an additional shift.
                'accum_loop: loop {
                    // Collect the candidate next tuple for a no-overflow pass.
                    let mut next = accum;
                    $(
                        // Try to accumulate the current component at the current shift.
                        let (next_value, d_shift) = accum_scalar_value(accum.$idx, value.$idx, shift);
                        if d_shift > 0 {
                            // Overflow happened for this component: propagate the new shift.
                            shift = shift.saturating_add(d_shift);
                            // Rescale all accumulated components to the new shared shift.
                            accum = accum.shift_all(d_shift);
                            // Retry the full tuple against the updated shared scale.
                            continue 'accum_loop;
                        }
                        // No extra shift required for this component, keep its candidate sum.
                        next.$idx = next_value;
                    )+
                    // All components fit at the same shift; commit tuple, shift, and counter.
                    self.value = next;
                    self.shift = shift;
                    self.counter = self.counter.saturating_add(1);
                    break;
                }
            }

            $(
                /// Returns the approximate average for one tuple component.
                pub fn $avg_method(&self) -> Option<$ty> {
                    approx_avg_value(self.value.$idx, self.counter, self.shift)
                }
            )+

            /// Merges another tuple accumulator into this one preserving shared shift semantics.
            pub fn merge(&mut self, other: &Self) {
                if other.counter == 0 {
                    return;
                }
                if self.counter == 0 {
                    self.value = other.value;
                    self.shift = other.shift;
                    self.counter = other.counter;
                    return;
                }
                let mut shift = self.shift.max(other.shift);
                let mut lhs = self.value.shift_all(shift.saturating_sub(self.shift));
                let mut rhs = other.value.shift_all(shift.saturating_sub(other.shift));
                'merge_loop: loop {
                    let mut next = lhs;
                    $(
                        if let Some(sum) = lhs.$idx.checked_add(rhs.$idx) {
                            next.$idx = sum;
                        } else {
                            shift = shift.saturating_add(1);
                            lhs = lhs.shift_all(1);
                            rhs = rhs.shift_all(1);
                            continue 'merge_loop;
                        }
                    )+
                    self.value = next;
                    self.shift = shift;
                    self.counter = self.counter.saturating_add(other.counter);
                    break;
                }
            }
        }

        #[allow(private_bounds)]
        impl<$($ty: SafeAccumScalar),+> Default for SafeAccum<($($ty,)+)> {
            fn default() -> Self {
                Self::new()
            }
        }
    };
}

macro_rules! impl_safe_accum_tuple_with_idx {
    ([$($acc:tt)*] [$idx:tt $($rest_idx:tt)*] [$avg:ident $($rest_avg:ident)*] $head:ident, $($tail:ident),+ $(,)?) => {
        impl_safe_accum_tuple_with_idx!([$($acc)* $head:$idx:$avg,] [$($rest_idx)*] [$($rest_avg)*] $($tail),+);
    };
    ([$($acc:tt)*] [$idx:tt $($rest_idx:tt)*] [$avg:ident $($rest_avg:ident)*] $last:ident $(,)?) => {
        impl_safe_accum_tuple_impl!($($acc)* $last:$idx:$avg);
    };
}

macro_rules! impl_safe_accum_tuple {
    ($($ty:ident),+ $(,)?) => {
        impl_safe_accum_tuple_with_idx!(
            []
            [0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15]
            [
                approx_avg_0 approx_avg_1 approx_avg_2 approx_avg_3
                approx_avg_4 approx_avg_5 approx_avg_6 approx_avg_7
                approx_avg_8 approx_avg_9 approx_avg_10 approx_avg_11
                approx_avg_12 approx_avg_13 approx_avg_14 approx_avg_15
            ]
            $($ty),+
        );
    };
}

impl_safe_accum_tuple!(T0, T1);
impl_safe_accum_tuple!(T0, T1, T2);

#[cfg(test)]
mod tests {
    use super::SafeAccum;

    #[test]
    fn add_u128_without_overflow() {
        let mut accum = SafeAccum::<u128>::new();
        accum.add(10);
        accum.add(20);
        assert_eq!(*accum, 30);
        assert_eq!(accum.shift(), 0);
    }

    #[test]
    fn add_u128_with_overflow_and_shift() {
        let mut accum = SafeAccum::<u128>::new();
        accum.add(u128::MAX);
        accum.add(2);
        assert_eq!(*accum, 1u128 << 127);
        assert_eq!(accum.shift(), 1);
    }

    #[test]
    fn add_i128_with_positive_overflow_and_shift() {
        let mut accum = SafeAccum::<i128>::new();
        accum.add(i128::MAX);
        accum.add(2);
        assert_eq!(*accum, (i128::MAX >> 1) + 1);
        assert_eq!(accum.shift(), 1);
    }

    #[test]
    fn add_i128_with_negative_overflow_and_shift() {
        let mut accum = SafeAccum::<i128>::new();
        accum.add(i128::MIN);
        accum.add(-1);
        assert_eq!(*accum, (i128::MIN >> 1) - 1);
        assert_eq!(accum.shift(), 1);
    }

    #[test]
    fn add_tuple2_overflow_shifts_all_components() {
        let mut accum = SafeAccum::<(u128, u128)>::new();
        accum.add((u128::MAX, 10));
        accum.add((1, 20));
        assert_eq!(*accum, (u128::MAX >> 1, 15));
        assert_eq!(accum.shift(), 1);
    }

    #[test]
    fn add_tuple3_mixed_types_with_overflow() {
        let mut accum = SafeAccum::<(u128, i128, u128)>::new();
        accum.add((u128::MAX, i128::MAX, 8));
        accum.add((2, 3, 4));
        assert_eq!(*accum, ((u128::MAX >> 1) + 1, (i128::MAX >> 1) + 1, 6));
        assert_eq!(accum.shift(), 1);
    }

    #[test]
    fn incoming_value_is_shifted_by_current_shift() {
        let mut accum = SafeAccum::<u128>::new();
        accum.add(u128::MAX);
        accum.add(2);
        accum.add(8);
        assert_eq!(*accum, (1u128 << 127) + 4);
        assert_eq!(accum.shift(), 1);
    }

    #[test]
    fn shift_ge_128_is_safe_for_unsigned() {
        let mut accum = SafeAccum {
            value: 5u128,
            shift: 128,
            counter: 0,
        };
        accum.add(u128::MAX);
        assert_eq!(*accum, 5);
        assert_eq!(accum.shift(), 128);
        assert_eq!(accum.counter(), 1);
    }

    #[test]
    fn shift_ge_128_is_safe_for_signed() {
        let mut accum = SafeAccum {
            value: 0i128,
            shift: 128,
            counter: 0,
        };
        accum.add(-1);
        assert_eq!(*accum, 0);
        assert_eq!(accum.shift(), 128);
        assert_eq!(accum.counter(), 1);
    }

    #[test]
    fn counter_increments_on_each_add() {
        let mut accum = SafeAccum::<u128>::new();
        accum.add(1);
        accum.add(2);
        accum.add(3);
        assert_eq!(accum.counter(), 3);
    }

    #[test]
    fn approx_avg_scalar_uses_formula() {
        let mut accum = SafeAccum::<u128>::new();
        accum.add(10);
        accum.add(20);
        accum.add(40);
        assert_eq!(accum.approx_avg(), Some(23));
    }

    #[test]
    fn approx_avg_tuple_methods_work() {
        let mut accum = SafeAccum::<(u128, i128, u128)>::new();
        accum.add((10, -10, 20));
        accum.add((14, -14, 22));
        assert_eq!(accum.approx_avg_0(), Some(12));
        assert_eq!(accum.approx_avg_1(), Some(-12));
        assert_eq!(accum.approx_avg_2(), Some(21));
    }

    #[test]
    fn merge_scalar_accumulators_preserves_shift_and_counter() {
        let mut lhs = SafeAccum::<u128>::new();
        lhs.add(u128::MAX);
        lhs.add(2);
        let mut rhs = SafeAccum::<u128>::new();
        rhs.add(8);
        rhs.add(12);
        lhs.merge(&rhs);
        assert_eq!(*lhs, (1u128 << 127) + 10);
        assert_eq!(lhs.shift(), 1);
        assert_eq!(lhs.counter(), 4);
    }

    #[test]
    fn merge_tuple_accumulators_preserves_shared_shift_and_counter() {
        let mut lhs = SafeAccum::<(u128, u128)>::new();
        lhs.add((u128::MAX, 10));
        lhs.add((1, 20));
        let mut rhs = SafeAccum::<(u128, u128)>::new();
        rhs.add((8, 40));
        rhs.add((12, 60));
        lhs.merge(&rhs);
        assert_eq!(*lhs, ((u128::MAX >> 1) + 10, 65));
        assert_eq!(lhs.shift(), 1);
        assert_eq!(lhs.counter(), 4);
    }
}
