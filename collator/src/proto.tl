---types---

/**
* @param session_seqno  The sequence number of the session
* @param block_id_short The short identifier for a block
* @param signatures     A vector of hash-signature pairs
*/
queries.signaturesQuery
    session_seqno:int
    block_id_short:types.blockIdShort
    signatures:(vector types.hashSignaturePair)
    = queries.SignaturesQuery;

/**
* @param workchain Workchain identifier of the shard
* @param prefix    Prefix of the shard
* @param seqno     Sequence number of the block
*/
types.blockIdShort
    workchain:int
    prefix:long
    seqno:int
    = types.BlockIdShort;

/**
* @param hash       Hash of the data
* @param signature  Signature data
*/
types.hashSignaturePair
    hash:bytes
    signature:bytes
    = types.HashSignaturePair;


/**
* @param shard_ident   The identifier for a shard, serialized with tl_shard_ident
* @param session_seqno The sequence number of the session
*/
types.overlayNumber
    shard_ident:types.shardIdent
    session_seqno:int
    = overlays.OverlayNumber;

/**
* @param workchain Workchain identifier of the shard
* @param prefix    Prefix of the shard
*/
types.shardIdent
    workchain:int
    prefix:long
    = types.ShardIdent;