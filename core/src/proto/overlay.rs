use tl_proto::{TlRead, TlWrite};

#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "overlay.ping", scheme = "proto.tl")]
pub struct Ping;

#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "overlay.pong", scheme = "proto.tl")]
pub struct Pong;
