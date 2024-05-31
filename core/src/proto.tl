// Overlay
////////////////////////////////////////////////////////////////////////////////

---types---

overlay.ping = overlay.Ping;
overlay.pong = overlay.Pong;

/**
* A successful response for the overlay query
*
* @param value  an existing value
*/
overlay.response.ok value:T = overlay.Response T;
/**
* An unsuccessul response for the overlay query
*/
overlay.response.err code:int = overlay.Response T;

/**
* Message broadcast prefix
*/
overlay.broadcastPrefix = overlay.BroadcastPrefix;

// Blockchain public overlay
////////////////////////////////////////////////////////////////////////////////

---types---

/**
* Data for computing a public overlay id
*/
blockchain.overlayIdData
    zerostate_root_hash:int256
    zerostate_file_hash:int256
    = blockchain.OverlayIdData;

/**
* A full block id
*/
blockchain.blockId
    workchain:int
    shard:long
    seqno:int
    root_hash:int256
    file_hash:int256
    = blockchain.BlockId;

/**
* A response for the `getNextKeyBlockIds` query
*
* @param block_ids      list of key block ids
* @param incomplete     flag points to finishinig query
*/
blockchain.keyBlockIds block_ids:(vector blockchain.blockId) incomplete:Bool = blockchain.KeyBlockIds;

/**
* A response for getting full block info
*
* @param block_id       block id
* @param proof          block proof raw
* @param block          block data raw
* @param is_link        block proof link flag
*/
blockchain.blockFull.found block_id:blockchain.blockId proof:bytes block:bytes is_link:Bool = blockchain.BlockFull;
/**
* A response for getting empty block
*/
blockchain.blockFull.empty = blockchain.BlockFull;

/**
* An unsuccessul response for the 'getArchiveInfo' query
*/
blockchain.archiveNotFound = blockchain.ArchiveInfo;
/**
* A successul response for the 'getArchiveInfo' query
*
* @param id             archive id
*/
blockchain.archiveInfo id:long = blockchain.ArchiveInfo;

/**
* An unsuccessul response for the 'getPersistentStatePart' query
*/
blockchain.persistentStatePart.notFound = blockchain.PersistentStatePart;
/**
* A successul response for the 'getPersistentStatePart' query
*
* @param data           persistent state part
*/
blockchain.persistentStatePart.found data:bytes = blockchain.PersistentStatePart;

/**
* Raw data bytes
*/
blockchain.data data:bytes = blockchain.Data;


/**
* External message broadcast.
*/
blockchain.broadcast.message data:bytes = blockchain.Broadcast;

---functions---

/**
* Get list of next key block ids.
*
* @param block_id       first key block id
* @param count          max number of items in the response
*/
blockchain.getNextKeyBlockIds
    block_id:blockchain.blockId
    count:int
    = overlay.Response blockchain.KeyBlockIds;

/**
* Get full block info
*
* @param block_id       target block id
*/
blockchain.getBlockFull
    block_id:blockchain.blockId
    = overlay.Response blockchain.blockFull;

/**
* Get next full block info
*
* @param prev_block_id  previous block id
*/
blockchain.getNextBlockFull
    prev_block_id:blockchain.blockId
    = overlay.Response blockchain.blockFull;

/**
* Get archive info
*
* @param mc_seqno       masterchain block seqno
*/
blockchain.getArchiveInfo
    mc_seqno:int
    = overlay.Response blockchain.ArchiveInfo;

/**
* Get archive slice
*
* @param archive_id     archive id (masterchain seqno)
* @param offset         part offset in bytes
* @param max_size       max response size in bytes
*/
blockchain.getArchiveSlice
    archive_id:long
    offset:long
    max_size:int
    = overlay.Response blockchain.Data;

/**
* Get persisten state part
*
* @param block_id       requested block id
* @param mc_block_id    reference masterchain block id
* @param offset         part offset in bytes
* @param max_size       max response size in bytes
*/
blockchain.getPersistentStatePart
    block_id:blockchain.blockId
    mc_block_id:blockchain.blockId
    offset:long
    max_size:long
    = overlay.Response blockchain.PersistentStatePart;
