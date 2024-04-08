use tl_proto::{TlRead, TlWrite};

#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "overlay.ping", scheme = "proto.tl")]
pub struct Ping {
    pub value: u64
}

#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "overlay.pong", scheme = "proto.tl")]
pub struct Pong {
    pub value: u64
}