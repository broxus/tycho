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
* A successul response for the 'getArchiveInfo' query
*
* @param id             archive id
*/
blockchain.archiveInfo.found id:long = blockchain.ArchiveInfo;
/**
* An unsuccessul response for the 'getArchiveInfo' query
*/
blockchain.archiveInfo.notFound = blockchain.ArchiveInfo;

/**
* Node response when it has the requested persistent state
*
* @param size           state file size in bytes
*/
blockchain.persistentStateInfo.found size:long = blockchain.PersistentStateInfo;
/**
* Node response when it doesn't have the requested state
*/
blockchain.persistentStateInfo.notFound = blockchain.PersistentStateInfo;

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
* Get key block proof
*
* @param block_id  key block id
*/
blockchain.getKeyBlockProof
    block_id:blockchain.blockId
    = overlay.Response blockchain.Data;

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
* @param limit          max response size in bytes
* @param offset         part offset in bytes
*/
blockchain.getArchiveSlice
    archive_id:long
    limit:int
    offset:long
    = overlay.Response blockchain.Data;

/**
* Get persistent state info
*
* @param block_id       requested block id
*/
blockchain.getPersistentStateInfo
    block_id:blockchain.blockId
    = overlay.Response blockchain.PersistentStateInfo;

/**
* Get persisten state part
*
* @param block_id       requested block id
* @param limit          max response size in bytes
* @param offset         part offset in bytes
*/
blockchain.getPersistentStatePart
    block_id:blockchain.blockId
    limit:int
    offset:long
    = overlay.Response blockchain.Data;
