---types---

/**
* Data for computing a private overlay id
*/
validator.overlayIdData
    zerostate_root_hash:int256
    zerostate_file_hash:int256
    shard_ident:validator.shardIdent
    session_id:int
    = validator.OverlayIdData;

/**
* @param workchain  workchain identifier
* @param prefix     shard prefix with tag
*/
validator.shardIdent
    workchain:int
    prefix:long
    = validator.ShardIdent;

/**
* Signature exchange response (full)
*
* @param signature  Validator signature
*/
validator.exchange.complete signature:bytes = validator.Exchange;

/**
* Signature exchange response (partial)
*/
validator.exchange.cached = validator.Exchange;

---functions---

/**
* Exchange signatures for the specified block
*
* @param block_seqno    block height for the session's shard
* @param signature      64-bytes of ed25519 signature
*/
validator.exchangeSignatures
    block_seqno:int
    signature:bytes
    = validator.Exchange;
