int128 4*[ int ] = Int128;
int256 8*[ int ] = Int256;

// Transport
////////////////////////////////////////////////////////////////////////////////

---types---

/**
* @param key   compressed ed25519 verifying key
*/
transport.peerId key:int256 = transport.PeerId;

transport.address.ipv4 ip:int port:int = transport.Address;
transport.address.ipv6 ip:int128 port:int = transport.Address;

/**
* @param items      multiple possible addresses for the same peer
* @param created_at unix timestamp when the list was generated
* @param expires_at unix timestamp up to which this list is valid
*/
transport.addressList
    items:(vector transport.Address)
    created_at:int
    expires_at:int
    = transport.AddressList;

// DHT
////////////////////////////////////////////////////////////////////////////////

---types---

/**
* @param id         node public key
* @param addr_list  list of possible peer addresses
* @param created_at unix timestamp when the entry was generated
* @param signature  a ed25519 signature of the entry
*/
dht.node
    id:transport.PeerId
    addr_list:transport.addressList
    created_at:int
    signature:bytes
    = dht.Node;

/**
* @param nodes list of DHT nodes
*/
dht.nodes nodes:(vector dht.node) = dht.Nodes;


/**
* Key for the value that can only be updated by an owner
*
* @param name       key name as UTF-8 string
* @param idx        key index used for versioning
* @param peer_id    owner id
*/
dht.signedKey
    name:bytes
    idx:int
    peer_id:transport.PeerId
    = dht.Key;

/**
* Key for the overlay-managed value
*
* @param id     overlay id
* @param name   key name as UTF-8 string
* @param idx    key index used for versioning
*/
dht.overlayKey id:int256 name:bytes idx:int = dht.Key;


/**
* A value with an exact owner
*
* @param key        signed key
* @param value      any data
* @param expires_at unix timestamp up to which this value is valid
*/
dht.signedValue key:dht.signedKey data:bytes expires_at:int signature:bytes = dht.Value;

/**
* An overlay-managed value
*
* @param key        overlay key
* @param value      any data
* @param expires_at unix timestamp up to which this value is valid
*/
dht.overlayValue key:dht.overlayKey data:bytes expires_at:int = dht.Value;


/**
* A response for the `dht.store` query
*/
dht.stored = dht.Stored;


/**
* A response for the `dht.findNode` query
*
* @param value  a list of nodes with the shortest distances
*/
dht.nodesFound nodes:dht.nodes = dht.NodeResponse;


/**
* A successful response for the `dht.findValue` query
*
* @param value  an existing value
*/
dht.valueFound value:dht.Value = dht.ValueResponse;
/**
* An unsuccessul response for the `dht.findValue` query
*
* @param value  a list of nodes with the shortest distances
*/
dht.valueNotFound nodes:dht.nodes = dht.ValueResponse;


/*
* A response for the `dht.getNodeInfo` query
*
* @param info   a signed node info
*/
dht.nodeInfoFound info:dht.node = dht.NodeInfoResponse;

---functions---

/**
* Suggest a node to store that value
*
* @param value  value to store
*/
dht.store value:dht.Value = dht.Stored;
/**
* Searches for k closest nodes
*
* @param key    key hash
* @param k      max length of the result list
*/
dht.findNode key:int256 k:int = dht.NodeResponse;
/**
* Searches for a value if stored or k closest nodes
*
* @param key    key hash
* @param k      max length of the nodes list if it is not found
*/
dht.findValue key:int256 k:int = dht.ValueResponse;
/**
* Requests a signed address list
*/
dht.getNodeInfo = dht.NodeInfoResponse;
