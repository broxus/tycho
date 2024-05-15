pub fn identity<'a, F, A, B, R>(f: F) -> F
    where
        F: Fn(A, B) -> R,
        A: 'a,
        B: 'a,
        R: 'a,
{
    f
}