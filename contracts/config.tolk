import "@stdlib/gas-payments"
import "@stdlib/lisp-lists"
import "@stdlib/tvm-dicts"
import "@stdlib/tvm-lowlevel"
import "@stdlib/common"
import "lib/config-params"

tolk 1.0

const CHANGE_ELECTOR_CODE_MESSAGE_TAG = 0x4e436f64;

const SET_NEXT_VALIDATOR_SET_TAG = 0x4e565354;
const NEW_VOTING_PROPOSAL_TAG = 0x6e565052;
const VOTE_CONFIG_PROPOSAL_TAG = 0x566f7465;


const CHANGE_ONE_PARAM_ACTION = 0x43665021;
const CHANGE_CONFIG_CODE_ACTION = 0x4e436f64;
const CHANGE_MASTER_PUBLIC_KEY_ACTION = 0x50624b21;
const CHANGE_ELECTOR_CODE_ACTION = 0x4e43ef05;


const BAD_CURRENT_VALUE_ERROR = -0xe2646356;
const CANNOT_SEND_MPARAMS_NULL_ERROR = -0xcd506e6c;
const CELL_TOO_DEEP_ERROR = -0xc2616456;
const CANT_SET_CRITICAL_FLAG_ERROR = -0xc3726954;
const CANT_CHANGE_CRITICAL_FLAG_ERROR = -0xc3726955;
const EXPIRED_ERROR = -0xc5787069;
const PROPOSAL_ALREADY_EXISTS_ERROR = -0xc16c7245;
const INSUFFICIENT_AMOUNT_ERROR = -0xf0617924;

@pure
fun msg_value_change_elector_code(): coins
//asm "1000000000 int_or_env CONFIG_MSG_CHANGE_ELECTOR_CODE"
    asm "1000000000 INT"

@pure
fun price_threshold_value(): coins
    asm "1000000000 INT"
//asm "1000000000 int_or_env CONFIG_PRICE_THRESHOLD_VALUE"

struct ConfigContract {
    cfgDict: cell
    storedSeqno: uint32,
    publicKey: uint256,
    voteDict: dict
}


fun ConfigContract.load(): ConfigContract {
    return ConfigContract.fromCell(contract.getData(), {
        assertEndAfterReading: true
    });
}

fun ConfigContract.save(self) {
    contract.setData(self.toCell());
}

fun ConfigContract.getConfigDict(self): dict {
    return self.cfgDict as dict
}

fun ConfigContract.setConfigDict(mutate self, cfgDict: dict) {
    if (cfgDict != null) {
        self.cfgDict = cfgDict!
    }

}

fun ConfigContract.setConfigParam(index: int, value: cell) {
    var data = lazy ConfigContract.load();
    (data.cfgDict as dict).iDictSetRef(32, index, value);
    data.save();
}

struct (0x12) ValidatorSetInfo {
    unixTimeSince: uint16,
    unitTimeUntil: uint16,
    total: uint16,
    main: uint16
}

struct (0xce) ProposalStatus {
    expires: uint32,
    proposal: Cell<ConfigProposal>, //ConfigProposal
    critical: bool,
    voters: dict,
    weightRemaining: int64,
    vsetId: uint256,
    rest: ProposalRestInfo? // maybe can do better?
}

struct (0xf3) ConfigProposal {
    paramId: int32,
    paramValue: cell?
    hash: uint256?
}

fun ProposalStatus.storeIntoBuilder(self): builder {
    return beginCell()
        .storeUint(0xce, 8)
        .storeUint(self.expires, 32)
        .storeRef(self.proposal as cell)
        .storeBool(self.critical)
        .storeDict(self.voters)
        .storeInt(self.weightRemaining, 64)
        .storeUint(self.vsetId, 256)
        .storeAny(self.rest)
}


struct ProposalRestInfo {
    roundsRemaining: uint8,
    wins: uint8,
    losses: uint8,
}


fun ProposalStatus.updateRest(mutate self) {
    val voteConfig = VoteConfig.get(self.critical);
    var rest = self.rest!;

    if (self.weightRemaining >= 0) {
        rest.losses += 1; // double check
    }

    if (rest.losses > voteConfig.maxLosses) {
        self.rest = null;
        return;
    }
    rest.roundsRemaining -= 1;
    if (rest.roundsRemaining < 0) {
        // existed for too many rounds
        self.rest = null;
        return;
    }
    self.rest = rest;
}


fun ValidatorSetInfo.load(vset: cell): ValidatorSetInfo {
    val info = lazy ValidatorSetInfo.fromCell(vset);
    assert (info.main > 0, 9);
    assert (info.total >= info.main, 9);
    return info;
}

//
// === Tick tock transaction logic ===
//

fun onRunTickTock(_isTock: bool) {
    var config = lazy ConfigContract.load();
    val kbl = 32;

    var configDict = config.getConfigDict();
    val (nextVsetDict, found) = configDict.iDictGetRef(kbl, PARAM_IDX_NEXT_VSET);

    var updated = false;
    if (found && nextVsetDict != null) {
        //check whether we have to set next_vset as the current validator set
        val nextVset = lazy ValidatorSet.fromCell(nextVsetDict!);
        if (nextVset.utimeSince <= blockchain.now()) {
            // next validator set becomes active!
            val currentVsetDict = configDict.iDictSetAndGetRefOrNull(kbl, PARAM_IDX_CURRENT_VSET, nextVsetDict);
            configDict.iDictSetAndGetRefOrNull(kbl, PARAM_IDX_PREV_VSET, currentVsetDict!);
            configDict.iDictDelete(kbl, PARAM_IDX_NEXT_VSET);
            updated = true;
        }
    }
    config.setConfigDict(configDict);

    if (!updated) {
        //if nothing has been done so far, scan a random voting proposal instead
        scanRandomProposal(mutate config.voteDict);
    }
    config.save();

}

//
// === External message logic ===
//

fun onExternalMessage(body: slice) {
    val signature = body.loadBits(512);
    var cs = body;

    val action = cs.loadUint(32);
    val msg_seqno = cs.loadUint(32);
    val validUntil = cs.loadUint(32);

    assert (validUntil >= blockchain.now(), 37);
    assert (cs.depth() <= 128, 39);

    var configContract = lazy ConfigContract.load();
    assert (configContract.storedSeqno == msg_seqno, 33);

    if (action == VOTE_CONFIG_PROPOSAL_TAG) {
        val idx = cs.loadUint(16);
        val proposalHash = cs.loadUint(256);
        cs.assertEnd();

        var valSlice = CurrentVset.getValidatorDescription(idx);
        val validator = ValidatorDescr.unpackFromSlice(mutate valSlice!);
        assert (isSliceSignatureValid(body, signature, validator.pubkey), 34);
        acceptExternalMessage();

        configContract.storedSeqno = (configContract.storedSeqno + 1) % (1 << 32);
        configContract.save();
        commitContractDataAndActions();
        configContract.proceedRegisterVote(proposalHash, idx, validator.weight);
        return;
    }
    assert (isSignatureValid(body.hash(), signature, configContract.publicKey), 34);
    acceptExternalMessage();
    configContract.storedSeqno = (configContract.storedSeqno + 1) % (1 << 32);
    configContract.save();
    commitContractDataAndActions();

    configContract.performAction(action, cs);
    configContract.save();

}

//
// === Internal message logic ===
//

fun onInternalMessage(in: InMessage) {
    val (senderWc, senderAddress) = in.senderAddress.getWorkchainAndHash();
    if (senderWc != MASTERCHAIN || in.body.isEmpty()) {
        //source not in masterchain or a simple transfer
        return;
    }

    val messageTag = in.body.loadUint(32);
    val queryId = in.body.loadUint(64);

    match (messageTag) {
        SET_NEXT_VALIDATOR_SET_TAG => {
            val vset = in.body.loadRef();
            in.body.assertEnd();
            val electorParam = blockchain.configParam(PARAM_IDX_ELECTOR_ADDRESS);
            val electorAddress: uint256 = electorParam == null ? -1 : electorParam.beginParse().preloadUint(256);

            var success = false;

            if (senderAddress == electorAddress) {
                val vsetInfo = ValidatorSetInfo.load(vset);
                success = (vsetInfo.unixTimeSince > blockchain.now()) && vsetInfo.unitTimeUntil > vsetInfo.unixTimeSince;
            }

            if (success) {
                ConfigContract.setConfigParam(PARAM_IDX_NEXT_VSET, vset);
                //success msg
                sendAnswerMessage(in.senderAddress, queryId, 0xee764f4b, SEND_MODE_CARRY_ALL_REMAINING_MESSAGE_VALUE)
            } else {
                //error msg
                sendAnswerMessage(in.senderAddress, queryId, 0xee764f6f, SEND_MODE_CARRY_ALL_REMAINING_MESSAGE_VALUE)
            }
        }
        NEW_VOTING_PROPOSAL_TAG => {
            val price = registerVotingProposal(mutate in.body, in.valueCoins as int);
            var mode = SEND_MODE_CARRY_ALL_REMAINING_MESSAGE_VALUE;
            var answerTag = -price;
            if (price > 0) {
                //ok. debit price
                reserveToncoinsOnBalance(price, RESERVE_MODE_INCREASE_BY_ORIGINAL_BALANCE);
                answerTag = 0xee565052;
                mode = SEND_MODE_CARRY_ALL_BALANCE;
            }
            sendAnswerMessage(in.senderAddress, queryId, answerTag, mode);
            return;
        }
        VOTE_CONFIG_PROPOSAL_TAG => {
            val signature = in.body.loadBits(512);
            val msgBody = in.body;

            val sign_tag = in.body.loadUint(32);
            val idx = in.body.loadUint(16);
            val proposalHash = in.body.loadUint(256);
            in.body.assertEnd();

            assert (sign_tag == 0x566f7445, 37);
            var valSlice = CurrentVset.getValidatorDescription(idx);
            val validator = ValidatorDescr.unpackFromSlice(mutate valSlice!);

            assert (isSliceSignatureValid(msgBody, signature, validator.pubkey), 34);
            var slf = lazy ConfigContract.load();
            val result = slf.proceedRegisterVote(proposalHash, idx, validator.weight);
            sendAnswerMessage(in.senderAddress, queryId, result + 0xd6745240, SEND_MODE_CARRY_ALL_REMAINING_MESSAGE_VALUE);
            return;
        }
        else => {
            // if tag is non-zero and its higher bit is zero, throw an exception (the message is an unsupported query)
            // to bounce message back to sender
            val cmp = messageTag == 0 ? -1 : 0;
            assert (cmp | (messageTag & (1 << 31)), 37);
            // do nothing for other internal messages
        }
    }
}

fun sendAnswerMessage(
    dst: address,
    queryTag: uint32,
    answerTag: uint64,
    mode: int
) {
    val message = createMessage({
        bounce: true,
        value: 0,
        dest: dst,
        body: (queryTag, answerTag)
    });
    message.send(mode)
}

fun changeElectorCode(code: slice) {
    val dst = blockchain.configParam(1)!
        .beginParse()
        .preloadUint(256);

    val message = createMessage({
        bounce: true,
        dest: (-1, dst as uint256),
        value: msg_value_change_elector_code(),
        body: (CHANGE_ELECTOR_CODE_MESSAGE_TAG as uint32, blockchain.now() as uint64, code)
    });
    message.send(SEND_MODE_REGULAR)
}


fun ConfigContract.performAction(mutate self, action: int, cs: slice) {
    match (action) {
        CHANGE_ONE_PARAM_ACTION => {
            val index = cs.loadInt(32);
            val value = cs.loadRef();
            cs.assertEnd();
            var configDict = self.getConfigDict();
            configDict.iDictSetRef(32, index, value);
            self.setConfigDict(configDict);
        }
        CHANGE_CONFIG_CODE_ACTION => {
            val newCode = cs.loadRef();
            contract.setCodePostponed(newCode);
            //val oldCode = getTvmRegisterC3();
            setTvmRegisterC3(transformSliceToContinuation(newCode.beginParse()));
            //afterCodeUpgrade(cs, oldCode) empty in original impl
            throw 0;
        }
        CHANGE_MASTER_PUBLIC_KEY_ACTION => {
            self.publicKey = cs.loadUint(256);
            cs.assertEnd();
        }
        CHANGE_ELECTOR_CODE_ACTION => {
            changeElectorCode(cs);
        }
        else => {
            assert (!action, 32);
        }
    }
}

fun ConfigContract.proceedRegisterVote(mutate self, hash: uint256, id: uint16, weight: uint64) {
    val registeredVote = self.registerVote(hash, id, weight);
    self.save();
    if (registeredVote.acceptedProposal != null) {
        val is_critical = 6 - registeredVote.status == 0 ? true : false; // registeredVote.status can be only 6 or 7 in case of present acceptedProposal
        val (paramId, paramValueOpt) = self.acceptProposal(registeredVote.acceptedProposal!, is_critical);
        self.save();
        if (paramId != 0) {
            self.performProposedAction(paramId, paramValueOpt);
            self.save();
        }
    }
    return registeredVote.status
}


fun ConfigContract.performProposedAction(mutate self, paramId: int, paramValue: cell?) {
    if (paramId == -999) {
        val newPublicKey = paramValue == null ? 0 : paramValue!.beginParse().preloadUint(256);
        self.publicKey = newPublicKey;
    }

    if (paramValue == null) {
        return;
    }

    if (paramId == -1000) {
        //upgrade code
        var cs = paramValue!.beginParse();
        val newCode = cs.loadRef();
        contract.setCodePostponed(newCode);
        setTvmRegisterC3(transformSliceToContinuation(newCode.beginParse()));
        throw (0);
    }

    if (paramId == -1001) {
        //upgrade elector code
        var cs = paramValue!.beginParse();
        changeElectorCode(cs);
    }
}


fun ConfigContract.acceptProposal(mutate self, proposal: cell, critical: bool): (int, cell?) {
    val configProposal = lazy ConfigProposal.fromCell(proposal);
    var configDict = self.getConfigDict();
    val (currentValue, found) = configDict.iDictGetRef(32, configProposal.paramId);
    val currentHash = !found ? 0 as uint256 : currentValue!.hash();

    if ((currentHash != proposal.hash()) & (proposal.hash() >= 0)) {
        // current value has incorrect hash, do not apply changes
        return (0, null);
    }

    val (mandatoryParams, _) = configDict.iDictGetRef(32, PARAM_IDX_MANDATORY_PARAMS);

    val (_, foundMandatory) = mandatoryParams.iDictGet(32, configProposal.paramId);
    if (foundMandatory & configProposal.paramValue == null) {
        // cannot set a mandatory parameter to (null)
        return (0, null);
    }

    val (criticalParams, _) = configDict.iDictGetRef(32, PARAM_IDX_CRITICAL_PARAMS);
    val (_, foundCritical) = criticalParams.iDictGet(32, configProposal.paramId);

    if (foundCritical && !critical) {
        //trying to set a critical parameter after a non-critical voting
        return (0, null);
    }

    // CHANGE ONE CONFIGURATION PARAMETER (!)
    configDict.iDictSetRef(32, configProposal.paramId, configProposal.paramValue!);
    self.setConfigDict(configDict);

    return (configProposal.paramId, configProposal.paramValue)
}

struct RegisteredVote {
    acceptedProposal: Cell<ProposalStatus>?,
    status: int
}
fun ConfigContract.registerVote(mutate self, hash: uint256, id: uint16, weight: uint64) {
    var (proposalStatusOpt, found) = self.voteDict.uDictGet(256, hash)!;
    if (!found) {
        return RegisteredVote {
            acceptedProposal: null,
            status: -1
        }
    }

    val currentVset = CurrentVset.load();
    var proposalStatus = lazy ProposalStatus.fromSlice(proposalStatusOpt!);
    if (proposalStatus.expires <= blockchain.now()) {
        self.voteDict.uDictDelete(256, hash);
        return RegisteredVote {
            acceptedProposal: null,
            status: -1
        }
    }

    if (proposalStatus.vsetId != currentVset.vset.hash()) {
        // config proposal belongs to a previous validator set
        proposalStatus.vsetId = currentVset.vset.hash();
        proposalStatus.updateRest();
        proposalStatus.voters = null;
        proposalStatus.weightRemaining = mulDivFloor(currentVset.totalWeight, 3, 4)
    }

    if (proposalStatus.rest == null) {
        // discard proposal (existed for too many rounds, or too many losses)
        self.voteDict.uDictDelete(256, hash);
        return RegisteredVote {
            acceptedProposal: null,
            status: -1
        }
    }

    val (_, foundVoters) = proposalStatus.voters.uDictGet(16, id);
    if (foundVoters) {
        // already voted for this proposal, ignore vote
        return RegisteredVote {
            acceptedProposal: null,
            status: -2
        }
    }

    //register vote
    proposalStatus
        .voters //
        .uDictSetBuilder(
        16,
        id,
        beginCell().storeUint(blockchain.now(), 32)
    );

    val weightRemainingPrev = proposalStatus.weightRemaining;
    proposalStatus.weightRemaining -= weight;

    val cmp = weightRemainingPrev >= 0 ? -1 : 0;
    if (proposalStatus.weightRemaining ^ cmp) {
        //not enough votes, or proposal already accepted in this round
        //simply update weight_remaining
        self.voteDict.uDictSetBuilder(256, hash, proposalStatus.storeIntoBuilder());
        return RegisteredVote {
            acceptedProposal: null,
            status: 2
        }
    }

    // proposal wins in this round
    val config = VoteConfig.get(proposalStatus.critical);
    var psRest = proposalStatus.rest!;
    psRest.wins += 1;
    if (psRest.wins >= config.minWins) {
        // proposal is accepted, remove and process
        self.voteDict.uDictDelete(256, hash);
        return RegisteredVote {
            acceptedProposal: proposalStatus.toCell(),
            status: proposalStatus.critical! ? 7 : 6, // 6 - critial as int (-1: 0)
        }
    }
    // update proposal info
    proposalStatus.rest = psRest;
    self.voteDict.uDictSetBuilder(
        256,
        hash,
        proposalStatus.storeIntoBuilder()
    );
    return RegisteredVote {
        acceptedProposal: null,
        status: 2
    }
}


fun scanRandomProposal(mutate voteDict: dict) {
    var (hash, psSlice, found) = voteDict.uDictGetNextOrEqual(256, random.uint256());
    if (!found) {
        return;
    }

    var (pStatus, changed) = scanProposal(psSlice!);
    if (changed) {
        if (pStatus == null) {
            voteDict.uDictDelete(256, hash!);
        } else {
            voteDict.uDictSet(256, hash!, pStatus);
        }
    }
}

fun scanProposal(psSlice: slice) {
    val currentVset = CurrentVset.load();
    val currentVsetId = currentVset.vset.hash();
    var proposalStatus = ProposalStatus.fromSlice(psSlice);
    if (proposalStatus.expires <= blockchain.now()) {
        //config proposal expired, delete
        return (null, true);
    }

    if (proposalStatus.vsetId == currentVsetId) {
        // config proposal already processed or voted for in this round, change nothing
        return (psSlice, false);
    }

    // config proposal belongs to a previous validator set
    proposalStatus.vsetId = currentVsetId;
    proposalStatus.updateRest(); //TODO: check
    proposalStatus.voters = null;
    proposalStatus.weightRemaining = mulDivFloor(currentVset.totalWeight, 3, 4);
    if (proposalStatus.rest == null) {
        // discard proposal (existed for too many rounds, or too many losses)
        return (null, true);
    }
    // return updated proposal
    return (proposalStatus.storeIntoBuilder().endCell().beginParse(), true);
}

fun registerVotingProposal(mutate msgBody: slice, msgValue: int) {
    var errorCode = 0;

    var proposalStatus = ProposalStatus.fromSlice(msgBody);
    if (proposalStatus.expires >> 30) {
        proposalStatus.expires -= blockchain.now();
    }

    var configProposal = proposalStatus.proposal.load();
    if (configProposal.hash! >= 0) {
        val currentValue = blockchain.configParam(configProposal.paramId);
        val currentHash = currentValue == null ? 0 as uint256 : currentValue.hash();
        if (configProposal.hash! != currentHash) {
            errorCode = BAD_CURRENT_VALUE_ERROR;
        }
    } else {
        val mandatoryParams = blockchain.configParam(PARAM_IDX_MANDATORY_PARAMS);
        val (_, found) = mandatoryParams.iDictGet(32, configProposal.paramId);
        if (found) {
            errorCode = CANNOT_SEND_MPARAMS_NULL_ERROR;
        }
    }

    if (configProposal.paramValue!.depth() >= 128) {
        errorCode = CELL_TOO_DEEP_ERROR;
    }

    if (errorCode < -1) {
        return errorCode;
    }

    if (!proposalStatus.critical) {
        val criticalParams = blockchain.configParam(PARAM_IDX_CRITICAL_PARAMS);
        val (_, found) = criticalParams.iDictGet(32, configProposal.paramId);
        if (found) {
            errorCode = CANT_SET_CRITICAL_FLAG_ERROR;
        }
    }

    if (errorCode < -1) {
        return errorCode;
    }

    // obtain vote proposal configuration
    val voteConfig = VoteConfig.get(proposalStatus.critical);
    if (proposalStatus.expires < voteConfig.minStoreSec) {
        return EXPIRED_ERROR; //expired
    }
    proposalStatus.expires = min(proposalStatus.expires, voteConfig.maxStoreSec);

    //compute price
    val (_, bits, refs) = configProposal.paramValue!.calculateSizeStrict(1024);
    val pps = voteConfig.bitPrice * (bits + 1024) + voteConfig.cellPrice * (refs + 2);
    var price = pps * proposalStatus.expires;
    proposalStatus.expires += blockchain.now();

    var configContract = lazy ConfigContract.load();
    var (pStatus, found) = configContract.voteDict.uDictGet(256, proposalStatus.proposal.hash());
    if (found) {
        // proposal already exists; we can only extend it
        var rProposalStatus = ProposalStatus.fromSlice(pStatus!);
        if (rProposalStatus.critical != proposalStatus.critical) {
            return CANT_CHANGE_CRITICAL_FLAG_ERROR; // cannot upgrade critical parameter to non-critical...
        }

        if (rProposalStatus.expires >= proposalStatus.expires) {
            return PROPOSAL_ALREADY_EXISTS_ERROR;  // proposal already exists
        }
        // recompute price
        price = pps * (proposalStatus.expires - rProposalStatus.expires + 16384);
        if (msgValue - price < price_threshold_value()) {
            return INSUFFICIENT_AMOUNT_ERROR;   // need more money
        }

        //update expiration time
        rProposalStatus.expires = proposalStatus.expires;
        configContract.voteDict.uDictSetBuilder(256, proposalStatus.proposal.hash(), rProposalStatus.storeIntoBuilder());
        configContract.save();
        return price;
    }

    if (msgValue - price < price_threshold_value()) {
        return INSUFFICIENT_AMOUNT_ERROR;   // need more money
    }

    // obtain current validator set data
    val currentVset = CurrentVset.load();
    val weightRemaining = mulDivFloor(currentVset.totalWeight, 3, 4);

    // create new proposal
    proposalStatus.voters = null;
    proposalStatus.weightRemaining = weightRemaining;
    proposalStatus.vsetId = currentVset.vset.hash();
    proposalStatus.rest = ProposalRestInfo {
        roundsRemaining: voteConfig.maxTotRounds,
        wins: 0,
        losses: 0
    };

    configContract.voteDict.uDictSetBuilder(256, proposalStatus.proposal.hash(), proposalStatus.storeIntoBuilder());
    configContract.save();
    return price;
}

//
// === Getters ===
//

get fun seqno(): int {
    val data = lazy ConfigContract.load();
    return data.storedSeqno
}

get fun get_proposal(pHash: int) {
    val slf = lazy ConfigContract.load();
    var (pStatus, found) = slf.voteDict.uDictGet(256, pHash);
    if (!found) {
        return null;
    }
    return unpackProposal(pStatus!);
}

get fun list_proposals() {
    val slf = lazy ConfigContract.load();
    var iterNext = maxInt();
    var list = createEmptyTuple();
    do {
        var (pHash, pStatus, found) = slf.voteDict.uDictGetPrev(256, iterNext);
        if (found) {
            iterNext = pHash!;
            val proposal = unpackProposal(pStatus!);
            listPrepend([pHash!, proposal], list);
        }
    } while (found);
    return list;
}

get fun proposal_storage_price(critical: bool, seconds: int, bits: int, refs: int) {
    val sfl = lazy ConfigContract.load();
    val voteConfig = VoteConfig.get(critical);
    if (seconds < voteConfig.minStoreSec) {
        return -1;
    }
    seconds = min(seconds, voteConfig.maxStoreSec);
    return (voteConfig.bitPrice * (bits + 1024) + voteConfig.cellPrice * (refs + 2)) * seconds;
}


fun unpackProposal(proposalStatus: slice) {
    val pStatus = ProposalStatus.fromSlice(proposalStatus);
    var votersList = createEmptyTuple();
    var iterNext = 1 << 32;
    do {
        val (voterId, _, found) = pStatus.voters.uDictGetPrev(16, iterNext);
        if (found) {
            iterNext = voterId!;
            listPrepend(voterId!, votersList);

        }
    } while (found);

    val rest = pStatus.rest!;
    val configProposal = pStatus.proposal.load();
    return [
        pStatus.expires,
        pStatus.critical,
        [configProposal.paramId, configProposal.paramValue, configProposal.hash] as tuple,
        pStatus.vsetId,
        votersList,
        pStatus.weightRemaining,
        rest.roundsRemaining,
        rest.losses,
        rest.wins
    ] as tuple;
}
