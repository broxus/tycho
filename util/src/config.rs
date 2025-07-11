pub use tycho_util_proc::PartialConfig;

pub trait PartialConfig: Sized {
    type Partial: serde::Serialize;

    fn into_partial(self) -> Self::Partial;
}

impl<T: PartialConfig> PartialConfig for Option<T> {
    type Partial = Option<T::Partial>;

    #[inline]
    fn into_partial(self) -> Self::Partial {
        self.map(T::into_partial)
    }
}
