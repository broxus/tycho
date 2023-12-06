use tl_proto::{Boxed, TlRead};

pub trait RpcQuery {
    type Response: for<'a> TlRead<'a, Repr = Boxed>;
}
