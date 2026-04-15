---types---

/**
* @param start_seqno        seqno of the first block in batch
* @param committed_blocks   bitset with committed blocks
* @param entries            all non-empty histories for unique validator indexes
*/
slasher.blocksBatch
    start_seqno:int
    min_anchor:int
    max_anchor:int
    committed_blocks:bitset
    observed:(vector slasher.observedHistory)
    = slasher.BlocksBatch;

/**
* @param validator_idx  validator index relative to the validator set
* @param points_proven  amount of points with proofs at rounds DAG closed in a given anchor range
* @param bits           history bits (2 for each block)
*/
slasher.observedHistory
    validator_idx:int
    points_proven:int
    bits:bitset
    = slasher.ObservedHistory;

slasher.storedVsetInfo
    prev_vset_hash:int256
    first_session_id:slasher.validationSessionId
    start_seqno:int
    = slasher.StoredVsetInfo;

slasher.storedVsetReport
    public_key:int256
    accusations:(vector int)
    = slasher.StoredVsetReport;

slasher.validationSessionId
    catchain_seqno:int
    vset_switch_round:int
    = slasher.ValidationSessionId;

bitset length:int data:bytes = BitSet;
