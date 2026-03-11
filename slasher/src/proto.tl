---types---

/**
* @param start_seqno        seqno of the first block in batch
* @param committed_blocks   bitset with committed blocks
* @param entries            all non-empty histories for unique validator indexes
*/
slasher.blocksBatch
    start_seqno:int
    committed_blocks:bitset
    entries:(vector slasher.signatureHistory)
    = slasher.BlocksBatch;

/**
* @param validator_idx  validator index relative to the validator set
* @param bits           history bits (2 for each block)
*/
slasher.signatureHistory
    validator_idx:int
    bits:bitset
    = slasher.SignatureHistory;

/**
* @param catchain_seqno         validation session catchain seqno
* @param vset_switch_round      validation session vset switch round
* @param total_blocks_in_session total committed blocks merged for this session
* @param offenders              validators we want to punish on stage 1
*/
slasher.sessionPenaltyReport
    catchain_seqno:int
    vset_switch_round:int
    total_blocks_in_session:int
    offenders:(vector slasher.validatorPenalty)
    = slasher.SessionPenaltyReport;

/**
* @param validator_idx          validator index relative to the validator set
* @param missing_signatures     blocks where no valid signature was observed
* @param invalid_signatures     blocks where an invalid signature was observed
*/
slasher.validatorPenalty
    validator_idx:int
    missing_signatures:int
    invalid_signatures:int
    = slasher.ValidatorPenalty;

bitset length:int data:bytes = BitSet;
