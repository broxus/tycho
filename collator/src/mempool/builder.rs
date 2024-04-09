use std::sync::Arc;

use super::{MempoolAdapter, MempoolEventListener};

// BUILDER

#[allow(private_interfaces)]
pub trait MempoolAdapterBuilder<T> {
    fn new() -> Self;
    fn build(self, listener: Arc<dyn MempoolEventListener>) -> T;
}

pub struct MempoolAdapterBuilderStdImpl<T> {
    _marker_adapter: std::marker::PhantomData<T>,
}

#[allow(private_interfaces)]
impl<T> MempoolAdapterBuilder<T> for MempoolAdapterBuilderStdImpl<T>
where
    T: MempoolAdapter,
{
    fn new() -> Self {
        Self {
            _marker_adapter: std::marker::PhantomData,
        }
    }
    fn build(self, listener: Arc<dyn MempoolEventListener>) -> T {
        T::create(listener)
    }
}
