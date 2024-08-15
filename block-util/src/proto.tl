---types---

/**
* @param workchain      workchain identifier
* @param prefix         shard prefix with tag
*/
block.shardIdent
    workchain:int
    prefix:long
    = block.ShardIdent;

/**
* @param    message logical time
* @param    message hash
*/
block.queueKey lt:long hash:int256 = block.QueueKey;

/**
* Lower bound of work.
*
* @param shard_ident    shard identifier
* @param bound          processed upto this bound
*/
block.shardProcessedUpto
    shard_ident:block.shardIdent
    bound:block.queueKey
    = block.ShardProcessedUpto;

/**
* Representation of an internal messages queue diff.
*
* @param prev_queue_diff_hash   hash of the TL repr of the previous queue diff
* @param shard_ident            shard identifier of the corresponding block
* @param seqno                  seqno of the corresponding block
* @param processed_upto         collator boundaries (sorted ASC by unique `shard_ident`)
* @param min_message            min message queue key
* @param max_message            max message queue key
* @param messages               list of message hashes (sorted ASC)
*/
block.queueDiff
    prev_queue_diff_hash:int256
    shard_ident:block.shardIdent
    seqno:int
    processed_upto:(vector block.shardProcessedUpto)
    min_message:block.queueKey
    max_message:block.queueKey
    messages:(vector int256)
    = block.QueueDiff;

/**
* Representation of a persistent internal messages queue state.
*
* @param shard_ident    shard identifier of the corresponding block
* @param seqno          seqno of the corresponding block
* @param queue_diffs    the smallest list of required diffs for the block (sorted DESC by `seqno`)
* @param messages       multi-root BOC with messages
*/
block.queueState
    shard_ident:block.shardIdent
    seqno:int
    queue_diffs:(vector block.QueueDiff)
    messages:bytes
    = block.QueueState;
