int128 4*[ int ] = Int128;
int256 8*[ int ] = Int256;

// Transport
////////////////////////////////////////////////////////////////////////////////

---types---

/**
* @param id    hash of the TL repr of the full key (@see transport.id.Full)
*/
transport.id.short id:int256 = transport.id.Short;

/**
* @param key   compressed ed25519 verifying key
*/
transport.id.full.ed25519 key:int256 = transport.id.Full;

transport.address.ipv4 ip:int port:int = transport.Address;
transport.address.ipv6 ip:int128 port:int = transport.Address;

/**
* @param items      multiple possible addresses for the same peer
* @param version    unix timestamp when the list was generated
* @param created_at unix timestamp when the peer was started
* @param expire_at  unix timestamp up to which this list is valid
*/
transport.addressList
    items:(vector transport.Address)
    version:int
    created_at:int
    expire_at:int
    = transport.AddressList;

transport.message.notify data:bytes = transport.Message;
transport.message.query data:bytes = transport.Message;
transport.message.answer data:bytes = transport.Message;
