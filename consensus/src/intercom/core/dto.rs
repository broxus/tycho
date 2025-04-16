use std::fmt::Debug;

use tl_proto::{TlRead, TlWrite};

#[derive(TlRead, TlWrite, Debug)]
#[tl(boxed, scheme = "proto.tl")]
pub enum QueryTag {
    #[tl(id = "core.queryTag.broadcast")]
    Broadcast,
    #[tl(id = "core.queryTag.pointById")]
    PointById,
    #[tl(id = "core.queryTag.signature")]
    Signature,
}

#[derive(TlRead, Debug)]
#[tl(boxed, id = "core.queryWrapper", scheme = "proto.tl")]
pub struct ReceiveWrapper<'tl> {
    pub tag: QueryTag,
    pub body: tl_proto::RawBytes<'tl, tl_proto::Boxed>,
}

#[derive(TlWrite, Debug)]
#[tl(boxed, id = "core.queryWrapper", scheme = "proto.tl")]
pub struct SendWrapper<'a, T> {
    pub tag: QueryTag,
    pub body: &'a T,
}
