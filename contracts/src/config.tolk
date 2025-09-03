import "@stdlib/common"
import "@stdlib/gas-payments"
import "@stdlib/lisp-lists"
import "@stdlib/tvm-dicts"
import "@stdlib/tvm-lowlevel"
import "interface/config"
import "interface/elector"
import "lib/config-params"
import "lib/util"

tolk 1.0

//
// === Constants ===
//
const SYNC_RESULT_UPDATED = -1
const SYNC_RESULT_DELETED = 0
const SYNC_RESULT_UNCHANGED = 1

const VOTE_STATUS_PROPOSAL_NOT_FOUND = -1
const VOTE_ALREADY_EXISTS = -2
const VOTE_ACCEPTED = 2
const VOTE_ACCEPTED_EXECUTE_NORMAL = 6
const VOTE_ACCEPTED_EXECUTE_CRITICAL = 7

/// Special param id to change public key.
/// NOTE: Only works if changing via proposal.
const PARAM_IDX_OWN_PUBKEY = -999
/// Special param id to change config code.
/// NOTE: Only works if changing via proposal.
const PARAM_IDX_CONFIG_CODE = -1000
/// Special param id to change elector code.
/// NOTE: Only works if changing via proposal.
const PARAM_IDX_ELECTOR_CODE = -1001

const PROPOSALS_KEY_LEN = 256
const VOTES_KEY_LEN = 16

const MSG_VALUE_CHANGE_ELECTOR_CODE = 1 << 30
const MSG_VALUE_PROPOSAL_ACCEPTED = 1 << 30

//
// === Storage Models ===
//
/// Contract state.
struct Storage {
    /// Root of the config params dictionary.
    ///
    /// MUST remain as the first cell in data since the node relies on it.
    configRoot: cell
    /// Replay protection seqno.
    seqno: uint32
    /// Master public key.
    publicKey: uint256
    /// Votes for config update proposals.
    ///
    /// uint256 (hash) => `ConfigProposal`
    proposals: dict
}

/// Loads C4 and parses it into a typed struct.
fun Storage.load(): Storage {
    return Storage.fromCell(contract.getData());
}

/// Serializes struct and stores it into C4.
fun Storage.save(self) {
    contract.setData(self.toCell());
}

/// Config proposal status.
struct (0xce) ConfigProposal {
    /// Unix timestamp when the proposal expires.
    expireAt: uint32
    /// Info about the updated config parameter.
    content: Cell<ConfigProposalContent>
    /// Whether updated parameter was considered critical at the
    /// time of proposal creation.
    isCritical: bool
    /// Indices of participants who has voted.
    ///
    /// uint16 (vset index) => `ProposalVote`
    voters: dict
    /// Remaining validators weight required for proposal to be accepted.
    weightRemaining: int64
    /// Id of the validator set which participants can vote.
    vsetId: uint256
    /// For how many validator rounds this proposal is valid.
    roundsRemaining: uint8
    /// How many times the quorum was achieved for this proposal.
    wins: uint8
    /// How many rounds were skipped without a quorum.
    losses: uint8
}

/// Create a new builder with this value.
fun ConfigProposal.toBuilder(self): builder {
    return beginCell().storeAny(self);
}

/// Tries to start a new voting round for the proposal.
///
/// Resets `vsetId` and `weightRemaining` to the specified values,
/// moves one round into losses and returns `true` if there
/// are still some rounds left or there are not too many
/// losses.
///
/// Otherwise does nothing and returns `false`.
fun ConfigProposal.startNextRound(mutate self, vsetId: uint256, vsetTotalWeight: uint64): bool {
    val maxLosses = ConfigVotingSetup.load().getMaxLosses(self.isCritical);
    // NOTE: in TVM -1 is for true, 0 is for false so we are substracting here.
    val losses = self.losses - (self.weightRemaining >= 0) as int;
    if (losses > maxLosses || self.roundsRemaining <= 0) {
        return false;
    }
    self.roundsRemaining -= 1;
    self.losses = losses;
    self.weightRemaining = mulDivFloor(vsetTotalWeight, 3, 4);
    self.vsetId = vsetId;
    return true;
}

/// Config proposal content to apply.
struct (0xf3) ConfigProposalContent {
    /// Config param index.
    paramIdx: int32
    /// Config param value.
    ///
    /// `null` to remove value.
    paramValue: cell?
    /// Expected hash of the previous value.
    /// Used as a "replay protection" for changes.
    prevParamValueHash: uint256?
}

/// Proposal votes dictionary entry.
struct ProposalVote {
    /// Unix timestamp when the vote was accepted.
    createdAt: uint32
}

/// Create a new builder with this value.
fun ProposalVote.toBuilder(self): builder {
    return beginCell().storeAny(self);
}

//
// === Tick tock transaction logic ===
//
/// Sync state at the beginning and end of the masterchain block.
///
/// - Rotates validator sets when the time comes;
/// - Synchronizes proposal states (only one at a time).
fun onRunTickTock(_isTock: bool) {
    var data = lazy Storage.load();

    // Get next vset from the stored config params dict.
    val nextVset = data.configRoot.iDictGetRefOrNull(PARAM_KEY_LEN, PARAM_IDX_NEXT_VSET);

    var updated = false;
    if (nextVset != null) {
        // Check whether we have to set next_vset as the current validator set.
        val vset = lazy ValidatorSet.fromCell(nextVset);
        if (vset.utimeSince <= blockchain.now()) {
            // Next validator set becomes current.
            val currentVset = (data.configRoot as dict).iDictSetAndGetRefOrNull(
                PARAM_KEY_LEN,
                PARAM_IDX_CURRENT_VSET,
                nextVset
            );
            // Current becomes previous.
            (data.configRoot as dict).iDictSetRef(PARAM_KEY_LEN, PARAM_IDX_PREV_VSET, currentVset!);
            // Old next becomes null.
            (data.configRoot as dict).iDictDelete(PARAM_KEY_LEN, PARAM_IDX_NEXT_VSET);
            updated = true;
        }
    }

    if (!updated) {
        // If nothing has been done so far, scan a random voting proposal instead.
        syncRandomProposal(mutate data.proposals);
    }

    // Save changes.
    data.save();
}

//
// === External message logic ===
//
/// Handles proposal votes sent as external messages.
/// Can also execute actions directly if "dictator" pubkey is set.
fun onExternalMessage(body: slice) {
    val signature = body.loadBits(512);
    var cs = body;

    // Parse external message fields.
    val op = cs.loadUint(32);
    val seqno = cs.loadUint(32);
    val validUntil = cs.loadUint(32);

    // Discard too old messages.
    assert(validUntil >= blockchain.now(), 35);
    // Discard too big messages.
    assert(cs.depth() <= 128, 39);

    var data = lazy Storage.load();

    // Basic replay protection.
    assert(data.seqno == seqno, 33);

    if (op == CONFIG_OP_VOTE_FOR_PROPOSAL) {
        // Validators from the current vset can vote for proposals
        // via external messages signed by their keys.
        val validatorIdx = cs.loadUint(16);
        val proposalHash = cs.loadUint(256);
        cs.assertEnd();

        // Get validator pubkey by index and use it to check message signature.
        var validatorCs = CurrentVset.getValidatorDescription(validatorIdx);
        val validator = ValidatorDescr.readFromSlice(mutate validatorCs!);
        assert(isSliceSignatureValid(body, signature, validator.pubkey), 34);
        acceptExternalMessage();

        // Ensure that replay protection is applied and commited first.
        data.seqno = increaseSeqno(data.seqno);
        data.save();
        commitContractDataAndActions();

        // Continue to execute proposed action.
        proceedRegisterVote(mutate data, proposalHash, validatorIdx, validator.weight);
    } else {
        // Otherwise it is a "dictator" external message and we use the stored pubkey.
        assert(isSignatureValid(body.hash(), signature, data.publicKey), 34);
        acceptExternalMessage();

        // Ensure that replay protection is applied and commited first.
        data.seqno = increaseSeqno(data.seqno);
        data.save();
        commitContractDataAndActions();

        // Continue to execute action.
        executeAction(mutate data, op, cs);
        data.save();
    }
}

//
// === Internal message logic ===
//
/// Handles on-chain queries and answers.
fun onInternalMessage(in: InMessage) {
    var body = in.body;

    val (wc, sender) = in.senderAddress.getWorkchainAndHash();
    if (wc != MASTERCHAIN || body.isEmpty()) {
        // Only masterchain addresses can execute any logic on config.
        return;
    }

    val op = body.loadUint(32);
    val queryId = body.loadUint(64);
    match (op) {
        /// Message from elector to update the next validator set.
        CONFIG_OP_SET_NEXT_VALIDATOR_SET => processSetNextVset(sender, queryId, body),
        /// Message from anybody to create a new proposal.
        CONFIG_OP_CREATE_PROPOSAL => processCreateProposal(
            in.senderAddress,
            queryId,
            body,
            in.valueCoins
        ),
        /// Message from a validator from the current round to vote for a proposal.
        CONFIG_OP_VOTE_FOR_PROPOSAL => processVoteForProposal(in.senderAddress, queryId, body),
        /// Everything else is ignored.
        else => {
            // Trigger bounce for queries (their op is non-zero and the highest bit is not set).
            assert(((op == 0) as int) | (op & (1 << 31)), 37);
        }
    }
}

//
// === Code upgrade handler ===
//
/// Handles code upgrade.
///
/// It has a fixed method id and is called with a new code
/// after "change config code" action was executed.
@method_id(1666)
fun afterCodeUpgrade(_args: slice, _oldCode: continuation) {}

//
// === Getters ===
//
/// Returns current replay protection seqno.
get fun seqno(): int {
    val data = lazy Storage.load();
    return data.seqno;
}

/// Returns proposal with the specified hash or `null` otherwise.
get fun get_proposal(proposalHash: int) {
    val data = lazy Storage.load();
    var (cs, found) = data.proposals.uDictGet(256, proposalHash);
    if (!found) {
        return null;
    }
    return proposalToTuple(cs!);
}

/// Returns a list of all proposals.
get fun list_proposals() {
    val data = lazy Storage.load();
    var iterNext = maxInt();
    var list = createEmptyList();
    do {
        var (proposalHash, proposal, found) = data.proposals.uDictGetPrev(256, iterNext);
        if (found) {
            iterNext = proposalHash!;
            val proposal = proposalToTuple(proposal!);
            listPrepend([proposalHash!, proposal], list);
        }
    } while (found);
    return list;
}

/// Computes proposal price for the specified TTL and size.
get fun proposal_storage_price(critical: bool, seconds: int, bits: int, refs: int) {
    val data = lazy Storage.load();
    val param = data.configRoot.iDictGetRefOrNull(PARAM_KEY_LEN, PARAM_IDX_VOTE_CONFIG);
    val config = ConfigVotingSetup.fromCell(param!).get(critical);
    if (seconds < config.minStoreSec) {
        return -1;
    }
    seconds = min(seconds, config.maxStoreSec);
    return (config.bitPrice * (bits + 1024) + config.cellPrice * (refs + 2)) * seconds;
}

//
// === Logic ===
//
/// Selects a random proposal and synchronizes it.
///
/// NOTE: Can select no proposal even if there are some.
/// This method is called on each block so there is no
/// problem to try a couple times.
fun syncRandomProposal(mutate proposals: dict) {
    var (hash, cs, found) = proposals.uDictGetNextOrEqual(256, random.uint256());
    if (!found) {
        return;
    }

    var proposal = lazy ConfigProposal.fromSlice(cs!);
    match (syncProposal(mutate proposal)) {
        SYNC_RESULT_UPDATED => proposals.uDictSetBuilder(256, hash!, proposal.toBuilder()),
        SYNC_RESULT_DELETED => proposals.uDictDelete(256, hash!),
        else => {}
    }
}

/// Synchronizes config proposal status.
///
/// Returns sync result.
fun syncProposal(mutate proposal: ConfigProposal): int {
    if (proposal.expireAt <= blockchain.now()) {
        // Config proposal expired, delete.
        return SYNC_RESULT_DELETED;
    }

    val currentVset = CurrentVset.load();
    val currentVsetId = currentVset.vset.hash();
    if (proposal.vsetId == currentVsetId) {
        // Config proposal already synchronized with the current round.
        return SYNC_RESULT_UNCHANGED;
    }

    // Config proposal belongs to a previous validator set.
    if (proposal.startNextRound(currentVsetId, currentVset.totalWeight)) {
        return SYNC_RESULT_UPDATED;
    } else {
        // Discard proposal if too many losses or rounds have passed.
        return SYNC_RESULT_DELETED;
    }
}

/// Tries to register a validator vote for the specified proposal.
/// If the proposal was accepted then tries to execute it.
///
/// Returns vote status.
///
/// Mutates persistent storage fields:
/// - configRoot
/// - proposals
/// - pubkey
fun proceedRegisterVote(
    mutate data: Storage,
    proposalHash: uint256,
    validatorIdx: uint16,
    validatorWeight: uint64,
) {
    // Continue to execute proposed action.
    val registeredVote = registerVote(
        mutate data.proposals,
        proposalHash,
        validatorIdx,
        validatorWeight
    );
    data.save();

    // Execute proposal if it was accepted during this vote.
    if (registeredVote.acceptedProposal != null) {
        val isCritical = registeredVote.status == VOTE_ACCEPTED_EXECUTE_CRITICAL;
        var proposal = CheckedProposal.prepare(
            registeredVote.acceptedProposal!,
            data.configRoot,
            isCritical
        );
        if (proposal.paramIdx != null) {
            executeProposal(mutate data, proposal.paramIdx, proposal.paramValue);
        }
        data.save();
    }

    return registeredVote.status;
}

struct RegisteredVote {
    acceptedProposal: Cell<ConfigProposalContent>?
    status: int
}

/// Tries to register a validator vote for the specified proposal.
fun registerVote(
    mutate proposals: dict,
    proposalHash: uint256,
    validatorIdx: uint16,
    validatorWeight: uint64,
) {
    // Find proposal by its hash.
    val (cs, found) = proposals.uDictGet(PROPOSALS_KEY_LEN, proposalHash)!;
    if (!found) {
        return RegisteredVote { acceptedProposal: null, status: VOTE_STATUS_PROPOSAL_NOT_FOUND };
    }
    var proposal = lazy ConfigProposal.fromSlice(cs!);

    // Discard expired proposals.
    if (proposal.expireAt <= blockchain.now()) {
        proposals.uDictDelete(PROPOSALS_KEY_LEN, proposalHash);
        return RegisteredVote { acceptedProposal: null, status: VOTE_STATUS_PROPOSAL_NOT_FOUND };
    }

    val currentVset = CurrentVset.load();
    val currentVsetId = currentVset.vset.hash();

    // Try to update round if proposal is not in sync.
    if (proposal.vsetId != currentVsetId) {
        val updated = proposal.startNextRound(currentVsetId, currentVset.totalWeight);
        if (!updated) {
            // Discard proposal if too many losses or rounds have passed.
            proposals.uDictDelete(PROPOSALS_KEY_LEN, proposalHash);
            return RegisteredVote { acceptedProposal: null, status: VOTE_STATUS_PROPOSAL_NOT_FOUND };
        }
    }

    val (_, voted) = proposal.voters.uDictGet(VOTES_KEY_LEN, validatorIdx);
    if (voted) {
        // Do nothing if already voted.
        return RegisteredVote { acceptedProposal: null, status: VOTE_ALREADY_EXISTS };
    }

    // Register vote.
    proposal.voters.uDictSetBuilder(
        VOTES_KEY_LEN,
        validatorIdx,
        ProposalVote { createdAt: blockchain.now() }.toBuilder()
    );

    val prevWeight = proposal.weightRemaining;
    proposal.weightRemaining -= validatorWeight;

    // NOTE: Here we apply and check XOR result only for the sign.
    // So this will be true if either both (prev/next) of weights
    // are positive or both of them are negative.
    if ((proposal.weightRemaining ^ prevWeight) >= 0) {
        // Not enough votes, or proposal already accepted in this round.
        // Just store the updated proposal.
        proposals.uDictSetBuilder(PROPOSALS_KEY_LEN, proposalHash, proposal.toBuilder());
        return RegisteredVote { acceptedProposal: null, status: VOTE_ACCEPTED };
    }

    // Required weight achieved so the proposal wins the round.
    val minWins = ConfigVotingSetup.load().getMinWins(proposal.isCritical);
    proposal.wins += 1;
    if (proposal.wins >= minWins) {
        // Proposal is accepted, remove and process
        proposals.uDictDelete(PROPOSALS_KEY_LEN, proposalHash);
        return RegisteredVote {
            acceptedProposal: proposal.content,
            // NOTE: TVM bools are -1 for true and 0 for false, so we substract here.
            status: VOTE_ACCEPTED_EXECUTE_NORMAL - (proposal.isCritical! as int),
        };
    }

    // Not enough winned rounds. Just store the updated proposal.
    proposals.uDictSetBuilder(PROPOSALS_KEY_LEN, proposalHash, proposal.toBuilder());
    return RegisteredVote { acceptedProposal: null, status: VOTE_ACCEPTED };
}

struct CheckedProposal {
    paramIdx: int?
    paramValue: cell?
}

/// Validates proposal content using the most recent config.
fun CheckedProposal.prepare(
    rawContent: Cell<ConfigProposalContent>,
    configDict: dict,
    isCritical: bool,
) {
    val content = rawContent.load();

    // Check current value hash if the proposal expects a specific value to be changed.
    val currentValue = configDict.iDictGetRefOrNull(PARAM_KEY_LEN, content.paramIdx);
    val currentHash = (currentValue == null) ? 0 as uint256 : currentValue!.hash();
    if (content.prevParamValueHash != null && content.prevParamValueHash != currentHash) {
        // Current value has incorrect hash, do not apply changes.
        return CheckedProposal { paramIdx: null, paramValue: null };
    }

    // Check if we are trying to remove a mandatory value.
    val (mandatoryParams, _) = configDict.iDictGetRef(PARAM_KEY_LEN, PARAM_IDX_MANDATORY_PARAMS);
    val (_, foundMandatory) = mandatoryParams.iDictGet(PARAM_KEY_LEN, content.paramIdx);
    if (foundMandatory & content.paramValue == null) {
        // Cannot remove mandatory param.
        return CheckedProposal { paramIdx: null, paramValue: null };
    }

    // Check that the vote to change the critical parameter was appropriate.
    val (criticalParams, _) = configDict.iDictGetRef(PARAM_KEY_LEN, PARAM_IDX_CRITICAL_PARAMS);
    val (_, foundCritical) = criticalParams.iDictGet(PARAM_KEY_LEN, content.paramIdx);
    if (foundCritical && !isCritical) {
        // Trying to update a critical parameter after a non-critical voting.
        return CheckedProposal { paramIdx: null, paramValue: null };
    }

    // Proposal is ok to execute.
    return CheckedProposal { paramIdx: content.paramIdx, paramValue: content.paramValue };
}

/// Executes proposal content.
fun executeProposal(mutate data: Storage, paramIdx: int32, paramValue: cell?) {
    match (paramIdx) {
        // Special action to update dictator pubkey.
        PARAM_IDX_OWN_PUBKEY => {
            val newPubkey = paramValue == null ? 0 : paramValue!.beginParse().preloadUint(256);
            data.publicKey = newPubkey;
        }
        // Special action to change config code.
        PARAM_IDX_CONFIG_CODE => {
            if (paramValue != null) {
                var cs = paramValue!.beginParse();
                val newCode = cs.loadRef();
                contract.setCodePostponed(newCode);
                val oldCode = getTvmRegisterC3();
                setTvmRegisterC3(transformSliceToContinuation(newCode.beginParse()));
                afterCodeUpgrade(cs, oldCode);
                throw 0;
            }
        }
        // Special action to change elector code.
        PARAM_IDX_ELECTOR_CODE => {
            if (paramValue != null) {
                var cs = paramValue!.beginParse();
                changeElectorCode(cs);
            }
        }
        // Change configuration parameter.
        else => {
            if (paramValue == null) {
                (data.configRoot as dict).iDictDelete(PARAM_KEY_LEN, paramIdx);
            } else {
                (data.configRoot as dict).iDictSetRef(PARAM_KEY_LEN, paramIdx, paramValue);
            }
        }
    }
}

/// Execute "dictator" action.
fun executeAction(mutate data: Storage, op: int, body: slice) {
    match (op) {
        // Update dictator pubkey.
        CONFIG_OP_SET_OWN_PUBKEY => {
            data.publicKey = body.loadUint(256);
            body.assertEnd();
        }
        // Change config code.
        CONFIG_OP_SET_CONFIG_CODE => {
            val newCode = body.loadRef();
            contract.setCodePostponed(newCode);
            val oldCode = getTvmRegisterC3();
            setTvmRegisterC3(transformSliceToContinuation(newCode.beginParse()));
            afterCodeUpgrade(body, oldCode);
            throw 0;
        }
        // Change elector code.
        CONFIG_OP_SET_ELECTOR_CODE => {
            changeElectorCode(body);
        }
        // Change configuration parameter.
        CONFIG_OP_SET_PARAM => {
            val index = body.loadInt(32);
            // TODO: Make this ref optional to allow removing config params.
            val value = body.loadRef();
            body.assertEnd();
            (data.configRoot as dict).iDictSetRef(32, index, value);
        }
        else => {
            assert(op == 0, 32);
        }
    }
}

/// Sends a message with a new code to the current elector contract.
fun changeElectorCode(code: slice) {
    val elector = blockchain.configParam(1)!.beginParse().preloadUint(256);

    val message = createMessage({
        bounce: true,
        dest: (-1, elector as uint256),
        value: MSG_VALUE_CHANGE_ELECTOR_CODE,
        body: (ELECTOR_OP_UPGRADE_CODE as uint32, blockchain.now() as uint64, code),
    });
    message.send(SEND_MODE_REGULAR);
}

/// Handles a query from elector with the next validator set.
///
/// Mutates persistent storage fields:
/// - configRoot
fun processSetNextVset(sender: uint256, queryId: int, body: slice) {
    val vset = body.loadRef();
    body.assertEnd();
    val electorParam = blockchain.configParam(PARAM_IDX_ELECTOR_ADDRESS);
    val electorAddress = electorParam == null ? -1 : electorParam.beginParse().preloadUint(256);

    var status = 0;
    if (sender == electorAddress && checkValidatorSet(vset)) {
        var data = lazy Storage.load();
        (data.configRoot as dict).iDictSetRef(32, PARAM_IDX_NEXT_VSET, vset);
        data.save();
        status = ELECTOR_OP_CONFIRM_VSET;
    } else {
        status = ELECTOR_OP_REJECT_VSET;
    }
    sendMessageBack((-1, sender), status, queryId);
}

/// Checks whether the validator set cell is well-formed.
fun checkValidatorSet(vset: cell): bool {
    val parsed = ValidatorSet.fromCell(vset);
    assert(parsed.main > 0, 9);
    assert(parsed.total >= parsed.main, 9);
    return parsed.utimeSince > blockchain.now() && parsed.utimeUntil > parsed.utimeSince;
}

/// Handles a proposal creation query.
///
/// Mutates persistent storage fields:
/// - proposals
fun processCreateProposal(sender: address, queryId: int, body: slice, msgValue: coins) {
    var ttl = body.loadUint(32);
    val rawContent = body.loadRef();
    val isCritical = body.loadBool();

    val price = registerVotingProposal(ttl, rawContent, isCritical, msgValue);

    var mode = 0;
    var answerTag = 0;
    if (price > 0) {
        // Proposal was created, reserve funds to store it for its lifetime.
        reserveToncoinsOnBalance(price, RESERVE_MODE_INCREASE_BY_ORIGINAL_BALANCE);
        mode = SEND_MODE_CARRY_ALL_BALANCE;
        answerTag = ANSWER_TAG_PROPOSAL_CREATED;
    } else {
        // Proposal rejected, answer with a status encoded into "price".
        mode = SEND_MODE_CARRY_ALL_REMAINING_MESSAGE_VALUE;
        answerTag = -price;
    }

    val message = createMessage({
        bounce: false,
        value: 0,
        dest: sender,
        body: (answerTag as uint32, queryId as uint64),
    });
    message.send(SEND_MODE_CARRY_ALL_REMAINING_MESSAGE_VALUE);
}

/// Proposal creation logic.
///
/// On success returns a positive integer with a proposal price.
/// On error returns a negative integer with an error answer tag.
///
/// Mutates persistent storage fields:
/// - proposals
fun registerVotingProposal(ttl: int, rawContent: cell, isCritical: bool, msgValue: int) {
    if (ttl >> 30) {
        // Normalize TTL if it was specified as a unix timestamp in the future.
        ttl -= blockchain.now();
    }

    var errorCode = 0;
    val content = ConfigProposalContent.fromCell(rawContent);

    if (content.paramValue == null) {
        // Ensure that the proposal is not trying to remove a mandatory param.
        val mandatoryParams = blockchain.configParam(PARAM_IDX_MANDATORY_PARAMS);
        val (_, found) = mandatoryParams.iDictGet(PARAM_KEY_LEN, content.paramIdx);
        if (found) {
            errorCode = ANSWER_TAG_PROPOSAL_CANT_REMOVE_MANDATORY;
        }
    } else if (content.paramValue.depth() >= 128) {
        // Check value depth if it is present.
        errorCode = ANSWER_TAG_PROPOSAL_TOO_DEEP_VALUE;
    }

    // Check that `prevParamValueHash` (if set) is correct at the time of proposal creation.
    if (content.prevParamValueHash != null) {
        val currentValue = blockchain.configParam(content.paramIdx);
        val currentHash = currentValue == null ? 0 as uint256 : currentValue.hash();
        if (content.prevParamValueHash != currentHash) {
            errorCode = ANSWER_TAG_PROPOSAL_VALUE_HASH_MISMATCH;
        }
    }

    // Check that `isCritical` flag is set for critical params.
    // FIXME: Why don't we just read it from the config?
    if (!isCritical) {
        val criticalParams = blockchain.configParam(PARAM_IDX_CRITICAL_PARAMS);
        val (_, found) = criticalParams.iDictGet(PARAM_KEY_LEN, content.paramIdx);
        if (found) {
            errorCode = ANSWER_TAG_PROPOSAL_EXPECTED_CRITICAL;
        }
    }

    // Do nothing on error.
    if (errorCode != 0) {
        return -errorCode;
    }

    // Load proposal creation config.
    val config = ConfigVotingSetup.load().get(isCritical);

    // Check and normalize TTL according to the config.
    if (ttl < config.minStoreSec) {
        return -ANSWER_TAG_PROPOSAL_EXPIRED;
    }
    ttl = min(ttl, config.maxStoreSec);

    // Compute storage price upfront.
    // FIXME: For some reason the original impl computes a non-unique amount of cells
    val (_, bits, refs) = content.paramValue!.calculateSizeStrict(1024);
    val pps = config.bitPrice * (bits + 1024) + config.cellPrice * (refs + 2);
    var price = pps * ttl;

    // Compute expiration timestamp.
    val expireAt = ttl + blockchain.now();

    var data = lazy Storage.load();

    val proposalHash = rawContent.hash();
    var (cs, found) = data.proposals.uDictGet(256, proposalHash);
    if (found) {
        // Proposal already exists, update `expireAt`.
        var proposal = ConfigProposal.fromSlice(cs!);
        if (proposal.isCritical != isCritical) {
            return -ANSWER_TAG_PROPOSAL_CANT_CHANGE_CRITICAL;
        }
        if (proposal.expireAt >= expireAt) {
            return -ANSWER_TAG_PROPOSAL_ALREADY_EXISTS;
        }

        // We only update expiration so we need to recompute storage price.
        price = pps * (expireAt - proposal.expireAt + 16384);

        // Check that message contains enough funds to send an answer back.
        if (msgValue - price < MSG_VALUE_PROPOSAL_ACCEPTED) {
            return -ANSWER_TAG_PROPOSAL_INSUFFICIENT_FUNDS;
        }

        proposal.expireAt = expireAt;
        data.proposals.uDictSetBuilder(256, proposalHash, proposal.toBuilder());
    } else {
        // Check that message contains enough funds to send an answer back.
        if (msgValue - price < MSG_VALUE_PROPOSAL_ACCEPTED) {
            return -ANSWER_TAG_PROPOSAL_INSUFFICIENT_FUNDS;
        }

        // Obtain current validator set data.
        val currentVset = CurrentVset.load();
        val weightRemaining = mulDivFloor(currentVset.totalWeight, 3, 4);

        // Create new proposal.
        val content = ConfigProposal {
            expireAt,
            content: rawContent as Cell<ConfigProposalContent>,
            isCritical,
            voters: null,
            weightRemaining,
            vsetId: currentVset.vset.hash(),
            roundsRemaining: config.maxTotalRounds,
            wins: 0,
            losses: 0,
        };
        data.proposals.uDictSetBuilder(256, proposalHash, content.toBuilder());
    }

    // Store changes.
    data.save();
    return price;
}

/// Handles a query from a validator with a vote for some proposal.
///
/// Mutates persistent storage fields:
/// - configRoot
/// - proposals
/// - pubkey
fun processVoteForProposal(sender: address, queryId: int, body: slice) {
    val signature = body.loadBits(512);
    val signedCs = body;
    val signed = SignedProposalVote.fromSlice(body);

    // Verify message signature.
    var validatorCs = CurrentVset.getValidatorDescription(signed.validatorIdx);
    val validator = ValidatorDescr.readFromSlice(mutate validatorCs!);
    assert(isSliceSignatureValid(signedCs, signature, validator.pubkey), 34);

    // Handle vote.
    var data = lazy Storage.load();
    val status = proceedRegisterVote(
        mutate data,
        signed.proposalHash,
        signed.validatorIdx,
        validator.weight
    );
    sendMessageBack(sender, status + ANSWER_TAG_VOTE_PROCESSED, queryId);
}

/// Sends message back to the query source.
///
/// Attaches all remaining message value.
fun sendMessageBack(dest: address | (int8, uint256), answerTag: uint32, queryId: uint64) {
    val message = createMessage({
        bounce: false,
        value: 0,
        dest,
        body: (answerTag, queryId),
    });
    message.send(SEND_MODE_CARRY_ALL_REMAINING_MESSAGE_VALUE);
}

/// Parses a slice with a proposal into a tuple.
fun proposalToTuple(cs: slice) {
    val proposal = ConfigProposal.fromSlice(cs);

    var votersList = createEmptyList();
    var iterNext = 1 << VOTES_KEY_LEN;
    do {
        val (voterId, _, found) = proposal.voters.uDictGetPrev(VOTES_KEY_LEN, iterNext);
        if (found) {
            iterNext = voterId!;
            listPrepend(voterId!, votersList);
        }
    } while (found);

    val content = proposal.content.load();
    return [
        proposal.expireAt,
        proposal.isCritical,
        [content.paramIdx, content.paramValue, content.prevParamValueHash] as tuple,
        proposal.vsetId,
        votersList,
        proposal.weightRemaining,
        proposal.roundsRemaining,
        proposal.losses,
        proposal.wins
    ] as tuple;
}

/// Increases seqno using `mod 2^32`
fun increaseSeqno(seqno: int): int {
    return (seqno + 1) % (1 << 32);
}
