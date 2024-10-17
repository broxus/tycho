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
* @param block          block data raw
* @param proof          block proof raw
* @param queue_diff     queue diff raw
*/
blockchain.blockFull.found
    block_id:blockchain.blockId
    block:blockchain.blockData
    proof:bytes
    queue_diff:bytes
    = blockchain.BlockFull;

/**
* A response for getting block info with the first chunk
*
* @param block          block data raw
* @param size           block size in bytes
* @param chunk_size     block chunk size in bytes
*/
blockchain.blockData
    data:bytes
    size:int
    chunk_size:int
    = blockchain.BlockData;

/**
* Block not found
*/
blockchain.blockFull.notFound = blockchain.BlockFull;

/**
* A response for getting a key block proof
*
* @param proof          block proof data
*/
blockchain.keyBlockProof.found proof:bytes = blockchain.KeyBlockProof;
/**
* Key block proof not found
*/
blockchain.keyBlockProof.notFound = blockchain.KeyBlockProof;

/**
* A successul response for the 'getArchiveInfo' query
*
* @param id             archive id
* @param size           archive size in bytes
* @param chunk_size     chunk size in bytes
*/
blockchain.archiveInfo.found
    id:long
    size:long
    chunk_size:long
    = blockchain.ArchiveInfo;

/**
* A successul response for the 'getArchiveInfo' query
* says that requested block hasn't commited in archive yet
*/
blockchain.archiveInfo.tooNew = blockchain.ArchiveInfo;

/**
* An unsuccessul response for the 'getArchiveInfo' query
*/
blockchain.archiveInfo.notFound = blockchain.ArchiveInfo;

/**
* A successful response for the 'getPersistentStateInfo' query
*
* @param size           state file size in bytes
* @param chunk_size     chunk size in bytes
*/
blockchain.persistentStateInfo.found
    size:long
    chunk_size:long
    = blockchain.PersistentStateInfo;

/**
* An unsuccessul response for the 'getPersistentStateInfo' query
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
* Get block data chunk
*
* @param block_id       target block id
* @param offset         chunk offset in bytes (must be a multiple of `chunk_size`)
*/
blockchain.getBlockDataChunk
    block_id:blockchain.blockId
    offset:int
    = overlay.Response blockchain.Data;


/**
* Get key block proof
*
* @param block_id  key block id
*/
blockchain.getKeyBlockProof
    block_id:blockchain.blockId
    = overlay.Response blockchain.KeyBlockProof;

/**
* Get archive info
*
* @param mc_seqno       masterchain block seqno
*/
blockchain.getArchiveInfo
    mc_seqno:int
    = overlay.Response blockchain.ArchiveInfo;

/**
* Get archive chunk
*
* @param archive_id     archive id (masterchain seqno)
* @param offset         chunk offset in bytes (must be a multiple of `chunk_size`)
*/
blockchain.getArchiveChunk
    archive_id:long
    offset:long
    = overlay.Response blockchain.Data;

/**
* Get persistent shard state info
*
* @param block_id       requested block id
*/
blockchain.getPersistentShardStateInfo
    block_id:blockchain.blockId
    = overlay.Response blockchain.PersistentStateInfo;

/**
* Get persistent shard state part
*
* @param block_id       requested block id
* @param limit          max response size in bytes
* @param offset         part offset in bytes
*/
blockchain.getPersistentShardStateChunk
    block_id:blockchain.blockId
    offset:long
    = overlay.Response blockchain.Data;

/**
* Get persistent queue state info
*
* @param block_id       requested block id
*/
blockchain.getPersistentQueueStateInfo
    block_id:blockchain.blockId
    = overlay.Response blockchain.PersistentStateInfo;

/**
* Get persistent queue state part
*
* @param block_id       requested block id
* @param offset         part offset in bytes
*/
blockchain.getPersistentQueueStateChunk
    block_id:blockchain.blockId
    offset:long
    = overlay.Response blockchain.Data;
