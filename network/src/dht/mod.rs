use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use self::routing::RoutingTable;
use self::storage::Storage;
use crate::types::PeerId;

mod routing;
mod storage;

pub struct Dht(Arc<DhtInner>);

impl Dht {}

struct DhtInner {
    _local_id: PeerId,
    _routing_table: Mutex<RoutingTable>,
    _last_table_refersh: Instant,
    _storage: Storage,
}
