pub trait MempoolAdapter {
    fn new() -> Self
    where
        Self: Sized;
}

#[derive(Clone)]
pub struct MempoolAdapterStdImpl {}

impl MempoolAdapter for MempoolAdapterStdImpl {
    fn new() -> Self
    where
        Self: Sized,
    {
        todo!()
    }
}
