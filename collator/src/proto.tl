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
* A signature of a single node
*
* @param peer_id    ADNL peer id of the node
* @param signature  64-bytes of ed25519 signature
*/
validator.peerSignature
    peer_id:int256
    signature:bytes
    = validator.PeerSignature;

/**
* @param workchain  workchain identifier
* @param prefix     shard prefix with tag
*/
validator.shardIdent
    workchain:int
    prefix:long
    = validator.ShardIdent;

/**
* Signature exchange response
*
* @param items      Validator signatures
*/
validator.signatures items:(vector validator.PeerSignature) = validator.Signatures;

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
    = validator.Signatures;
