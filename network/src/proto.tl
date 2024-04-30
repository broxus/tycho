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

// DHT
////////////////////////////////////////////////////////////////////////////////

---types---

/**
* @param id         node public key
* @param addr_list  multiple possible addresses for the same peer
* @param created_at unix timestamp when the info was generated
* @param expires_at unix timestamp up to which the info is valid
* @param signature  a ed25519 signature of the info
*/
dht.node
    id:transport.PeerId
    addr_list:(vector transport.Address)
    created_at:int
    expires_at:int
    signature:bytes
    = dht.Node;

/**
* @param nodes list of DHT nodes
*/
dht.nodes nodes:(vector dht.node) = dht.Nodes;


/**
* Key for the value that can only be updated by an owner
*
* @param name       key name enum
* @param peer_id    owner id
*/
dht.peerValueKey
    name:dht.PeerValueKeyName
    peer_id:transport.PeerId
    = dht.Key;

/**
* Key for the group-managed value
*
* @param name       key name enum
* @param group_id   group id
*/
dht.mergedValueKey
    name:dht.MergedValueKeyName
    group_id:int256
    = dht.Key;

// Peer value key names {
dht.peerValueKeyName.nodeInfo = dht.PeerValueKeyName;
// }

// Merged value key names {
dht.mergedValueKeyName.publicOverlayEntries = dht.MergedValueKeyName;
// }

/**
* A value with an exact owner
*
* @param key        peer value key
* @param value      any data
* @param expires_at unix timestamp up to which this value is valid
*/
dht.peerValue key:dht.peerValueKey data:bytes expires_at:int signature:bytes = dht.Value;

/**
* An group-managed value
*
* @param key        key info
* @param value      any data
* @param expires_at unix timestamp up to which this value is valid
*/
dht.mergedValue key:dht.mergedValueKey data:bytes expires_at:int = dht.Value;


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
* Query wrapper with an announced peer info.
*
* @param peer_info  a signed info of the sender
*/
dht.withPeerInfo peer_info:dht.node = True;

/**
* Suggest a node to store that value
*
* @param value  value to store
*/
dht.store value:dht.Value = True;
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
* Requests a signed node info
*/
dht.getNodeInfo = dht.NodeInfoResponse;

// Overlay
////////////////////////////////////////////////////////////////////////////////

---types---

/**
* A data to sign for `overlay.publicEntry`.
*
* @param overlay_id     public overlay id
* @param peer_id        node public key
* @param created_at     unix timestamp when the info was generated
*/
overlay.publicEntryToSign
    overlay_id:int256
    peer_id:transport.PeerId
    created_at:int
    = overlay.PublicEntryToSign;

/**
* A public overlay entry.
*
* @param peer_id        node public key
* @param created_at     unix timestamp when the info was generated
* @param signature      a signature of the `overlay.PublicEntryToSign` struct (as boxed)
*/
overlay.publicEntry
    peer_id:transport.PeerId
    created_at:int
    signature:bytes
    = overlay.PublicEntry;

/**
* @param entries    list of public overlay entries.
*/
overlay.publicEntries entries:(vector overlay.publicEntry) = overlay.PublicEntriesResponse;
overlay.overlayNotFound = overlay.PublicEntriesResponse;

// TODO: add broadcast

---functions---

/**
* Exchanges random entries of the specified public overlay.
*
* @param overlay_id     public overlay id
* @param entries        list of public overlay entries
*/
overlay.exchangeRandomPublicEntries
    overlay_id:int256
    entries:(vector overlay.publicEntry)
    = overlay.PublicEntries;

/**
* Overlay query/message prefix with an overlay id.
*
* @param overlay_id     overlay id
*/
overlay.prefix overlay_id:int256 = True;