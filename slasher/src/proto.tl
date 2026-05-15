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

slasher.report
    pubkey:int256
    validator_idx:int
    entries:(vector slasher.ReportEntry)
    = slasher.Report;

slasher.reportEntry
    validator_idx:int
    good_score:int
    bad_score:int
    punish:bool
    = slasher.ReportEntry;

bitset length:int data:bytes = BitSet;
