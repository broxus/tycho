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
    entries:(vector slasher.signatureHistory)
    anchor_stats_history:(vector slasher.anchorStatsHistory)
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
* @param validator_idx  validator index relative to the validator set
* @param points_proven  occurrences of a point having the next one as a proof
*/
slasher.anchorStatsHistory
    validator_idx:int
    points_proven:int
    = slasher.AnchorStatsHistory;

/**
* @param catchain_seqno              validation session catchain seqno
* @param vset_switch_round           validation session vset switch round
* @param epoch_catchain_seqno        catchain seqno of the vset epoch start
* @param epoch_vset_switch_round     vset switch round of the vset epoch start
* @param session_weight              unique committed blocks observed in the session
* @param validators                  per-validator weighted scores in the session
*/
slasher.sessionPenaltyReport
    catchain_seqno:int
    vset_switch_round:int
    epoch_catchain_seqno:int
    epoch_vset_switch_round:int
    session_weight:int
    validators:(vector slasher.sessionValidatorScore)
    = slasher.SessionPenaltyReport;

/**
* @param validator_idx          validator index relative to the validator set
* @param earned_points         weighted points received from observed validators
* @param max_points            weighted maximum possible score from observed validators
* @param is_bad                whether validator is bad in this session
*/
slasher.sessionValidatorScore
    validator_idx:int
    earned_points:long
    max_points:long
    is_bad:int
    = slasher.SessionValidatorScore;

/**
* @param epoch_catchain_seqno        catchain seqno of the vset epoch start
* @param epoch_vset_switch_round     vset switch round of the vset epoch start
* @param vset_hash                   validator set hash
* @param validators                  per-validator verdict in the epoch
*/
slasher.vsetPenaltyReport
    epoch_catchain_seqno:int
    epoch_vset_switch_round:int
    vset_hash:int256
    validators:(vector slasher.vsetValidatorPenalty)
    = slasher.VsetPenaltyReport;

/**
* @param validator_idx          validator index relative to the validator set
* @param bad_sessions_weight    sum of session weights where validator was bad
* @param total_sessions_weight  total observed session weight for validator
* @param is_bad                 final epoch verdict
*/
slasher.vsetValidatorPenalty
    validator_idx:int
    bad_sessions_weight:long
    total_sessions_weight:long
    is_bad:int
    = slasher.VsetValidatorPenalty;

/**
* @param vset_hash                  validator set hash
* @param has_next_epoch             whether epoch is already closed
* @param next_epoch_catchain_seqno  next epoch start catchain seqno
* @param next_epoch_vset_switch_round next epoch start vset switch round
*/
slasher.vsetEpoch
    vset_hash:int256
    has_next_epoch:int
    next_epoch_catchain_seqno:int
    next_epoch_vset_switch_round:int
    = slasher.VsetEpoch;

/**
* @param validator_indices          validator indices participating in the session
*/
slasher.sessionMeta
    validator_indices:(vector int)
    = slasher.SessionMeta;

bitset length:int data:bytes = BitSet;
