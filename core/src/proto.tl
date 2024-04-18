// Overlay
////////////////////////////////////////////////////////////////////////////////

---types---

/**
* Public overlay ping model
* @param value     unix timestamp in millis when ping was sent
*/
overlay.ping
    = overlay.Ping;

/**
* Public overlay pong model. Sending pong back to sender should follow receiving ping model
* @param value     unix timestamp in millis when ping was sent
*/
overlay.pong
    = overlay.Pong;

/**
* A successful response for the overlay query
*
* @param value  an existing value
*/
publicOverlay.response.ok value:T = publicOverlay.Response T;
/**
* An unsuccessul response for the overlay query
*/
publicOverlay.response.error error:bytes = publicOverlay.Response T;

/**
* A response for the `publicOverlay.getNextKeyBlockIds` query
* @param blocks         list of key blocks
* @param incomplete     flag points to finishinig query
*/
publicOverlay.keyBlockIds blocks:(vector publicOverlay.blockId) incomplete:Bool = publicOverlay.KeyBlockIds;

/**
* A response for getting full block info
* @param id             block id
* @param proof          block proof raw
* @param block          block data raw
* @param is_link        block proof link flag
*/
publicOverlay.blockFull.found id:publicOverlay.blockId proof:bytes block:bytes is_link:Bool = publicOverlay.BlockFull;

/**
* A response for getting empty block
*/
publicOverlay.blockFull.empty = publicOverlay.BlockFull;

/**
* An unsuccessul response for the 'getArchiveInfo' query
*/
publicOverlay.archiveNotFound = publicOverlay.ArchiveInfo;
/**
* A successul response for the 'getArchiveInfo' query
*
* @param id             archive id
*/
publicOverlay.archiveInfo id:long = publicOverlay.ArchiveInfo;

/**
* An unsuccessul response for the 'getPersistentStatePart' query
*/
publicOverlay.persistentStatePart.notFound = publicOverlay.PersistentStatePart;
/**
* A successul response for the 'getPersistentStatePart' query
*
* @param data           persistent state part
*/
publicOverlay.persistentStatePart.found data:bytes = publicOverlay.PersistentStatePart;

/**
* Raw data bytes
*/
publicOverlay.data data:bytes = publicOverlay.Data;

---functions---

/**
* Get list of next key block ids.
*
* @param block          block to start with
*/
publicOverlay.getNextKeyBlockIds block:publicOverlay.blockId max_size:int = publicOverlay.Response publicOverlay.KeyBlockIds;

/**
* Get full block info
*
* @param block          block id to get
*/
publicOverlay.getBlockFull block:publicOverlay.blockId = publicOverlay.Response publicOverlay.blockFull;

/**
* Get next full block info
*
* @param prev_block     previous block id
*/
publicOverlay.getNextBlockFull prev_block:publicOverlay.blockId = publicOverlay.Response publicOverlay.blockFull;

/**
* Get archive info
*
* @param mac_seqno  masterchain sequence number
*/
publicOverlay.getArchiveInfo mc_seqno:int = publicOverlay.ArchiveInfo;

/**
* Get archive slice
*
* @param archive_id
* @param offset
* @param max_size
*/
publicOverlay.getArchiveSlice archive_id:long offset:long max_size:int = publicOverlay.Data;

/**
* Get persisten state part
*
* @param block
* @param masterchain_block
* @param offset
* @param max_size
*/
publicOverlay.getPersistentStatePart block:publicOverlay.blockId mc_block:publicOverlay.blockId offset:long max_size:long = publicOverlay.PersistentStatePart;
