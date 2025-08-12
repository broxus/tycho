import "@stdlib/gas-payments"
import "@stdlib/lisp-lists"
import "@stdlib/tvm-dicts"
import "@stdlib/tvm-lowlevel"
import "lib/config-params"

tolk 1.0

const newStakeMessageTag = 0x4e73744b
const recoverStakeMessageTag = 0x47657424
const upgradeCodeMessageTag = 0x4e436f64

const UPDATE_VSET_MSG_VALUE = 1 << 30 // ~1 token

/// Contract state.
struct Storage {
    /// Active election.
    currentElection: Cell<Election>?
    /// Funds available to be recovered.
    ///
    /// uint256 (address) => coins
    credits: dict
    /// Finished elections.
    ///
    /// uint32 (electAt) => `PastElection`
    pastElections: dict
    /// Tracked bounces balance.
    grams: coins
    /// Election id of the current validator set.
    activeId: uint32
    /// Hash of the current validator set.
    activeHash: uint256
}

fun Storage.load(): Storage {
    return Storage.fromCell(contract.getData());
}

fun Storage.save(self) {
    contract.setData(self.toCell());
}

/// Active election state.
struct Election {
    /// Unix timestamp when election starts.
    ///
    /// Used as an unique election id.
    electAt: uint32
    /// Unix timestamp when election finishes.
    electClose: uint32
    /// Minimal stake for participants.
    minStake: coins
    /// Minimal total stake for elections to be considered successful.
    totalStake: coins
    /// Participants map.
    ///
    /// uint256 (pubkey) => `ElectionParticipant`
    participants: dict
    /// Whether the election finished without reaching required conditions.
    failed: bool
    /// Whether the election finished sucessfully.
    finished: bool
}

/// Active election participant info.
struct ElectionParticipant {
    /// Uncut stake.
    stake: coins
    /// Unix timestamp when this entry was registered.
    time: uint32
    /// Max allowed factor for stake (encoded with 16-bit fixed point).
    maxFactor: uint32
    /// Address (in masterchain) of the validator.
    addr: uint256
    /// ADNL address of the validator.
    ///
    /// NOTE: Same as pubkey for Tycho-based networks.
    adnlAddr: uint256
}

/// Finished election state.
struct PastElection {
    /// Unix timestamp when participant stakes are unfrozen.
    unfreezeAt: uint32
    /// A period in seconds for which validators' stakes are frozen.
    stakeHeld: uint32
    /// Hash of the serialized validator set.
    vsetHash: uint256
    /// Frozen stakes.
    ///
    /// uint256 (pubkey) => `FrozenStake`
    frozenDict: dict
    totalStake: coins
    bonuses: coins
    // Unused for now
    complaints: dict
}

/// Description of frozen validator stake.
struct FrozenStake {
    /// Address (in masterchain) of the validator.
    addr: uint256
    /// Validator's weight in vset.
    weight: uint64
    /// Frozen amount.
    stake: coins
    /// Whether validator was banned.
    ///
    /// NOTE: will be used by slashing, maybe should have two bits
    ///       (only disallow bonuses / full stake seizure).
    banned: bool
}

// struct CurrentVset {
//     vset: Cell<ValidatorSet>
//     totalWeight: uint64
//     list: dict
// }
// fun CurrentVset.load() {
//     val vset = blockchain.configParam(34)! as Cell<ValidatorSet>;
//     var cs: ValidatorSet = lazy vset.load();
//     return CurrentVset {
//         vset,
//         totalWeight: cs.totalWeight,
//         list: cs.list,
//     };
// }
// struct UnfreezeResult {
//     credits: dict
//     recovered: coins
// }
// === Tick-Tock transaction logic ===
// Tick-tock implementation
fun onRunTickTock(isTock: bool) {
    // check whether an election is being conducted
    var data = lazy Storage.load();
    if (data.currentElection != null) {
        var election: Election = data.currentElection!.load();

        // have an active election
        if (data.conductElections(election)) {
            return; // elections conducted, exit
        }
        if (data.validatorSetInstalled(election)) {
            return; // validator set installed, current elections removed
        }
    } else {
        if (data.announceNewElections()) {
            return; // new elections announced, exit
        }
    }

    if (data.updateActiveVsetId()) {
        return; // active validator set id updated, exit
    }

    data.checkUnfreeze();
}

fun Storage.conductElections(mutate self, election: Election): bool {
    if (blockchain.now() < election.electClose) {
        // Elections not finished yet.
        return false;
    }
    if (blockchain.configParam(0) == null) {
        // No configuration smart contract to send result to.
        return postponeElections();
    }

    val stakeParams = ValidatorSetStakeConfig.load();
    if (election.totalStake < stakeParams.minTotalStake) {
        // Insufficient total stake, postpone elections.
        return postponeElections();
    }
    if (election.failed) {
        // Do not retry failed elections until new stakes arrive.
        return postponeElections();
    }
    if (election.finished) {
        // Elections finished.
        return false;
    }

    var countParams = ValidatorSetSizeConfig.load();
    countParams.minValidators = max(countParams.minValidators, 1);

    val result = ElectionResult.compute(
        self.credits,
        election.participants,
        stakeParams,
        countParams
    );
    val failed = result.validatorCount == 0;
    election.failed = failed;
    election.finished = !failed;
    if (failed) {
        // Elections failed, update stored state with updated flags.
        self.credits = result.credits;
        self.currentElection = election.toCell();
        self.save();
        return postponeElections();
    }

    // Create the resulting validator set.
    val timingsConfig = ValidatorSetTimingsConfig.load();
    val start = max(blockchain.now() + timingsConfig.electionsEndBefore - 60, election.electAt);

    var vset = ValidatorSet {
        utimeSince: start,
        utimeUntil: start + timingsConfig.electedFor,
        total: result.validatorCount,
        main: min(result.validatorCount, countParams.maxMainValidators),
        totalWeight: result.totalWeight,
        list: result.validators,
    }
        .toCell();

    // Notify config contract with a new validator set.
    var configAddress = blockchain.configParam(0)!.beginParse().preloadUint(256);
    createMessage({
        bounce: true,
        value: UPDATE_VSET_MSG_VALUE,
        dest: (MASTERCHAIN, configAddress),
        body: beginCell() //
            .storeUint(0x4e565354, 32)
            .storeUint(election.electAt, 64)
            .storeRef(vset),
    })
        .send(SEND_MODE_PAY_FEES_SEPARATELY);

    // Update storage.
    var pastElection = PastElection {
        unfreezeAt: start + timingsConfig.electedFor + timingsConfig.stakeHeldFor,
        stakeHeld: timingsConfig.stakeHeldFor,
        vsetHash: vset.hash(),
        frozenDict: result.frozen,
        totalStake: result.totalStake,
        bonuses: 0,
        complaints: createEmptyDict(),
    };
    self.pastElections.uDictSetBuilder(32, election.electAt, beginCell().storeAny(pastElection));

    self.credits = result.credits;
    self.currentElection = election.toCell();
    self.save();

    return true;
}

struct ElectionResult {
    credits: dict
    validators: dict
    totalWeight: int
    frozen: dict
    totalStake: coins
    validatorCount: int
}

fun ElectionResult.compute(
    credits: dict,
    participants: dict,
    stakeConfig: ValidatorSetStakeConfig,
    sizeConfig: ValidatorSetSizeConfig,
): ElectionResult {
    // Count and sort all members by a composite key of (stake, reverse(time), pubkey).
    var stakeCount = 0;
    var sortedMembers = createEmptyDict();
    var nextPubkey = -1;
    do {
        var (pubkey, cs, found) = participants.uDictGetNext(256, nextPubkey);
        if (found) {
            nextPubkey = pubkey!;
            var participant = ElectionParticipant.fromSlice(cs!);

            val key = beginCell() //
                .storeUint(participant.stake, 128)
                .storeInt(-participant.time, 32)
                .storeUint(pubkey!, 256)
                .endCell()
                .beginParse();

            val value = beginCell() //
                .storeUint(min(participant.maxFactor, stakeConfig.maxStakeFactor), 32)
                .storeUint(participant.adnlAddr, 256);

            sortedMembers.sDictSetBuilder(128 + 32 + 256, key, value);
            stakeCount += 1;
        }
    } while (found);

    // Truncate validator count.
    stakeCount = min(stakeCount, sizeConfig.maxValidators);
    if (stakeCount < sizeConfig.minValidators) {
        return ElectionResult.empty(credits);
    }

    // Parse `sortedMembers` into a list of stakes.
    var allStakes = createEmptyList();
    do {
        // TODO: Replace `sDictDeleteFirstAndGet` with `sDictGetNext`.
        var (key, cs, found) = sortedMembers.sDictDeleteFirstAndGet(128 + 32 + 256);
        if (found) {
            val stake = min(key!.loadUint(128), stakeConfig.maxStake);
            key!.loadUint(32); // skip time
            val pubkey = key!.preloadUint(256);

            val maxF = cs!.loadUint(32);
            val adnlAddr = cs!.preloadUint(256);

            allStakes = listPrepend([stake, maxF, pubkey, adnlAddr], allStakes);
        }
    } while (found);

    // `allStakes` is the list of all stakes in decreasing order.
    // We now need to skip the required amount of validators with the greatest stake.
    var tailOffset = sizeConfig.minValidators - 1;
    var allStakesTail = allStakes;
    repeat (tailOffset) {
        allStakesTail = listGetTail(allStakesTail)!;
    }

    // `allStakesTail` contains all stakes except the first `minValidators - 1` items.
    // For each item in this tail we recompute the total stake and select
    // only the configuration with the maximum value.
    var (bestStake, selectedCount) = (0, 0);
    do {
        val (head: tuple, tail) = listSplit(allStakesTail);
        allStakesTail = tail!;

        var stake = head.get<coins>(0);
        tailOffset += 1;

        if (stake >= stakeConfig.minStake) {
            val totalStake = computeTotalStake(allStakes, tailOffset, stake);
            if (totalStake > bestStake) {
                (bestStake, selectedCount) = (totalStake, tailOffset);
            }
        }
    } while (tailOffset < stakeCount);

    // Do nothing if either no total stakes were computed or the result stake is too low.
    if ((selectedCount == 0) || (bestStake < stakeConfig.minTotalStake)) {
        return ElectionResult.empty(credits);
    }

    // Find minimal stake.
    // TODO: Why not just use `stake` from the previous loop?
    allStakes.stackMoveToTop();
    allStakesTail = allStakes;
    repeat (selectedCount - 1) {
        allStakesTail = listGetTail(allStakesTail)!;
    }
    val minStake = listGetHead<tuple>(allStakesTail).get<coins>(0);

    // Create both the new validator set and the refund set.
    var i = 0;
    var totalStake: coins = 0;
    var totalWeight = 0;
    var validators = createEmptyDict();
    var frozen = createEmptyDict();
    do {
        val (value: tuple, tail) = listSplit(allStakes);
        allStakes = tail!;

        var stake = value.get<coins>(0);
        val maxF = value.get<int>(1);
        val pubkey = value.get<uint256>(2);
        val adnlAddr = value.get<uint256>(3);

        // Lookup source address first.
        var (cs, memberExists) = participants.uDictGet(256, pubkey);
        assert(memberExists, 61);
        var participant = lazy ElectionParticipant.fromSlice(cs!);
        val addr = participant.addr;

        if (i < selectedCount) {
            // One of the first selected members, include into validator set.
            var trueStake = min(stake, (maxF * minStake) >> 16);
            stake -= trueStake;

            // ed25519_pubkey#8e81278a pubkey:bits256 = SigPubKey;  // 288 bits
            // validator_addr#73 public_key:SigPubKey weight:uint64 adnl_addr:bits256 = ValidatorDescr;
            var weight = (trueStake << 60) / bestStake;
            totalStake += trueStake;
            totalWeight += weight;

            // Add validator set item.
            var validatorDescr = ValidatorDescr {
                pubkey,
                weight,
                adnlAddr,
            };
            validators.uDictSetBuilder(16, i, beginCell().storeAny(validatorDescr));

            // Add frozen stakes item.
            var frozenStakeDescr = FrozenStake {
                addr,
                weight,
                stake: trueStake,
                banned: false,
            };
            frozen.uDictSetBuilder(256, pubkey, beginCell().storeAny(frozenStakeDescr));
        }

        // Return unused part of stake to the source address.
        if (stake != 0) {
            credits = creditTo(credits, addr, stake);
        }

        i += 1;
    } while (tail != null);

    assert(totalStake == bestStake, 49);

    return ElectionResult {
        credits,
        validators,
        totalWeight,
        frozen,
        totalStake,
        validatorCount: selectedCount,
    };
}

fun NewElection.empty(credits: dict) {
    return ElectionResult {
        credits,
        validators: createEmptyDict(),
        totalWeight: 0,
        frozen: createEmptyDict(),
        totalStake: 0,
        validatorCount: 0,
    };
}

// Сomputes the total stake out of the first n entries of all stakes.
fun computeTotalStake(allStakes: tuple, n: int, lowestStake: coins): coins {
    var totalStake: coins = 0;
    repeat (n) {
        var (head, tail) = listSplit<tuple>(allStakes);
        allStakes = tail!;

        var stake = head.get<coins>(0);
        var maxF = head.get<int>(1);
        stake = min(stake, (maxF * lowestStake) >> 16);
        totalStake += stake;
    }
    return totalStake;
}

fun postponeElections(): bool {
    return false;
}

fun Storage.validatorSetInstalled(mutate self, election: Election): bool {
    if (!election.finished) {
        // Elections not finished yet.
        return false;
    }
    val (cs, found) = self.pastElections.uDictGet(32, election.electAt);
    if (!found) {
        // No election data in dictionary.
        return false;
    }

    // Get vset hash.
    val pastElection = lazy PastElection.fromSlice(cs!);
    val vsetHash = pastElection.vsetHash;

    // Find vset hash in config params (search in current or next vset).
    if (
        blockchain.configParam(PARAM_IDX_CURRENT_VSET).hashEq(vsetHash) ||
        blockchain.configParam(PARAM_IDX_NEXT_VSET).hashEq(vsetHash)
    ) {
        // Reset `currentElection` since it is not needed anymore.
        self.currentElection = null;
        self.save();

        self.updateActiveVsetId();
        return true;
    }

    return false;
}

fun Storage.announceNewElections(mutate self): bool {
    if (blockchain.configParam(PARAM_IDX_NEXT_VSET) != null) {
        // Next validator set exists, no elections needed.
        return false;
    }

    // Check that current account is an elector.
    var electorAddr = blockchain.configParam(1)!.beginParse().preloadUint(256);
    var (myWc, myAddr) = contract.getAddress().getWorkchainAndHash();
    if (myWc != MASTERCHAIN || myAddr != electorAddr) {
        // this smart contract is not the elections smart contract anymore, no new elections
        return false;
    }

    // Get time when the current validator set expires.
    var currentVsetCs = blockchain.configParam(34); // current validator set
    if (currentVsetCs == null) {
        return false;
    }
    val currentVset = lazy ValidatorSet.fromCell(currentVsetCs!);

    val timingsConfig = ValidatorSetTimingsConfig.load();
    var t = blockchain.now();
    val t0 = currentVset.utimeUntil - timingsConfig.electionsBeginBefore;
    if (t < t0) {
        // Too early for the next elections.
        return false;
    }
    // Less than `electionsBeginBefore` seconds left, create new elections.
    if (t - t0 < 60) {
        // pretend that the elections started at t0
        t = t0;
    }

    // Get stake parameters
    val stakeConfig = ValidatorSetStakeConfig.load();

    // Init current elections.
    val electAt = t + timingsConfig.electionsBeginBefore;
    val electClose = electAt - timingsConfig.electionsEndBefore;

    self.currentElection = Election {
        electAt,
        electClose,
        minStake: stakeConfig.minStake,
        totalStake: 0,
        participants: createEmptyDict(),
        failed: false,
        finished: false,
    }
        .toCell();

    self.save();

    return true;
}

fun Storage.updateActiveVsetId(mutate self): bool {
    val currentVsetHash = blockchain.configParam(PARAM_IDX_CURRENT_VSET)!.hash();
    if (currentVsetHash == self.activeHash) {
        // Validator set unchanged.
        return false;
    }

    if (self.activeId != 0) {
        // activeId becomes inactive
        val (cs, found) = self.pastElections.uDictGet(32, self.activeId);
        if (found) {
            var pastElection = lazy PastElection.fromSlice(cs!);
            assert(pastElection.vsetHash == self.activeHash, 57);

            // Adjust unfreeze time of this validator set.
            pastElection.unfreezeAt = blockchain.now() + pastElection.stakeHeld;
            self.pastElections.uDictSetBuilder(
                32,
                self.activeId,
                beginCell().storeAny(pastElection)
            );
        }
    }

    // Look up new activeId by hash.
    var nextId = -1;
    do {
        var (id, cs, found) = self.pastElections.uDictGetNext(32, nextId);
        if (found) {
            nextId = id!;
            var pastElection = lazy PastElection.fromSlice(cs!);

            if (pastElection.vsetHash == currentVsetHash) {
                // Transfer 1/8 of accumulated everybody's grams to this validator set as bonuses.
                var amount = (self.grams >> 3);
                self.grams -= amount;
                pastElection.bonuses += amount;

                // Serialize back
                self.pastElections.uDictSetBuilder(32, nextId, beginCell().storeAny(pastElection));

                // Break.
                found = false;
            }
        }
    } while (found);

    // Save active election id and vset hash.
    self.activeId = nextId == -1 ? 0 : nextId;
    self.activeHash = currentVsetHash;
    self.save();
    return true;
}

fun Storage.checkUnfreeze(mutate self): bool {
    var nextId = -1;
    do {
        var (id, cs, found) = self.pastElections.uDictGetNext(32, nextId);
        if (found) {
            nextId = id!;
            var pastElection = lazy PastElection.fromSlice(cs!);

            if ((pastElection.unfreezeAt <= blockchain.now()) && (nextId != self.activeId)) {
                // Unfreeze!
                var (newCredits, newPastElections, unusedPrizes) = unfreezeAll(
                    self.credits,
                    self.pastElections,
                    nextId
                );
                self.credits = newCredits;
                self.pastElections = newPastElections;
                self.grams += unusedPrizes;
                // Unfreeze only one at time, exit loop.
                self.save();
                // Exit loop.
                found = false;
            }
        }
    } while (found);
    return nextId != -1;
}

// Main message handler
fun onInternalMessage(msgValue: coins, inMsgCell: cell, inMsgBody: slice) {
    var cs = inMsgCell.beginParse();
    val flags = cs.loadUint(4); // int_msg_info$0 ihr_disabled:Bool bounce:Bool bounced:Bool
    if (flags & 1) {
        // ignore all bounced messages
        return;
    }
    val sAddr = cs.loadAddress();
    if (inMsgBody.isEmpty()) {
        // inbound message has empty body
        processSimpleTransfer(sAddr, msgValue);
        return;
    }
    val op = inMsgBody.loadUint(32);
    if (op == 0) {
        // simple transfer with comment, return
        processSimpleTransfer(sAddr, msgValue);
        return;
    }
    val queryId = inMsgBody.loadUint(64);
    if (op == newStakeMessageTag) {
        // new stake message
        processNewStake(sAddr, msgValue, inMsgBody, queryId);
        return;
    }
    if (op == recoverStakeMessageTag) {
        // recover stake request
        recoverStake(op, sAddr, inMsgBody, queryId);
        return;
    }
    if (op == upgradeCodeMessageTag) {
        // upgrade code (accepted only from configuration smart contract)
        val ok = upgradeCode(sAddr, inMsgBody, queryId);
        sendMessageBack(sAddr, ok ? 0xce436f64 : 0xffffffff, queryId, op, 0, 64);
        return;
    }
    var cfgOk = (op == 0xee764f4b);
    if (cfgOk || (op == 0xee764f6f)) {
        // confirmation from configuration smart contract
        configSetConfirmed(sAddr, inMsgBody, queryId, cfgOk);
        return;
    }

    if (!(op & (1 << 31))) {
        // unknown query, return error
        sendMessageBack(
            sAddr,
            0xffffffff,
            queryId,
            op,
            0,
            SEND_MODE_CARRY_ALL_REMAINING_MESSAGE_VALUE
        );
        return;
    }
}

fun sendMessageBack(
    dest: address,
    functionTag: uint32,
    queryId: uint64,
    body: uint32,
    grams: coins,
    mode: int,
) {
    var outMessage: OutMessage;

    if (body >= 0) {
        outMessage = createMessage({
            bounce: true,
            value: grams,
            dest,
            body: (functionTag, queryId, body),
        });
    } else {
        outMessage = createMessage({
            bounce: true,
            value: grams,
            dest,
            body: (functionTag, queryId),
        });
    }

    outMessage.send(mode);
}

fun returnStake(addr: address, queryId: int, reason: int) {
    sendMessageBack(
        addr,
        0xee6f454c,
        queryId,
        reason,
        0,
        SEND_MODE_CARRY_ALL_REMAINING_MESSAGE_VALUE
    );
}

fun sendConfirmation(addr: address, queryId: int, comment: int) {
    sendMessageBack(addr, 0xf374484c, queryId, comment, ton("1"), SEND_MODE_IGNORE_ERRORS);
}

fun sendValidatorSetToConfig(configAddr: int, vset: cell, queryId: int) {
    var msg = beginCell().storeUint(0xc4ff, 17) // 0 11000100 0xff
        .storeUint(configAddr, 256)
        .storeCoins(ton("1")) // ~1 gram of value to process and obtain answer
        .storeUint(0, 1 + 4 + 4 + 64 + 32 + 1 + 1)
        .storeUint(0x4e565354, 32)
        .storeUint(queryId, 64)
        .storeRef(vset)
        .endCell();

    sendRawMessage(msg, SEND_MODE_PAY_FEES_SEPARATELY);
}

fun creditTo(credits: dict, addr: int, amount: coins): dict {
    val (valueOpt, found) = credits.uDictGet(256, addr);
    if (found) {
        var value = valueOpt!;
        amount += value.loadCoins();
    }
    credits.uDictSetBuilder(256, addr, beginCell().storeCoins(amount));
    return credits;
}

fun processSimpleTransfer(sAddr: address, msgValue: coins) {
    var data = Storage.load();
    val (srcWc, srcAddr) = sAddr.getWorkchainAndHash();
    if ((srcAddr != 0) | ((srcWc + 1) != BASECHAIN) | (data.activeId == 0)) {
        //TODO: check
        // simple transfer to us (credit "nobody's" account)
        data.grams += msgValue;
        Storage.save(data);
        return;
    }

    // zero source address -1:00..00 (collecting validator fees)
    val (fs, found) = data.pastElections.uDictGet(32, data.activeId);
    if (!found) {
        data.grams += msgValue;
    } else {
        var pe = PastElection.fromSlice(fs!, { assertEndAfterReading: true });
        pe.bonuses += msgValue;
        data.pastElections.uDictSetBuilder(32, data.activeId, beginCell().storeAny(pe));
    }

    data.save();
}

fun processNewStake(sAddr: address, msgValue: coins, cs: slice, queryId: int) {
    val (srcWc, srcAddr) = sAddr.getWorkchainAndHash();
    var ds = contract.getData().beginParse();
    val electionDict = ds.loadDict();
    if (electionDict == null || (srcWc + 1) != BASECHAIN) {
        // no elections active, or source is not in masterchain
        returnStake(sAddr, queryId, 0);
        return;
    }

    // parse the remainder of new stake message
    val validatorPubkey = cs.loadUint(256);
    val stakeAt = cs.loadUint(32);
    val maxFactor = cs.loadUint(32);
    val adnlAddr = cs.loadUint(256);
    val signature = cs.loadRef().beginParse().preloadBits(512);
    cs.assertEnd();

    if (
        !isSliceSignatureValid(
            beginCell().storeUint(0x654c5074, 32).storeUint(stakeAt, 32).storeUint(maxFactor, 32)
                .storeUint(srcAddr, 256)
                .storeUint(adnlAddr, 256)
                .endCell()
                .beginParse(),
            signature,
            validatorPubkey
        )
    ) {
        // incorrect signature, return stake
        returnStake(sAddr, queryId, 1);
        return;
    }

    if (maxFactor < 0x10000) {
        // factor must be >= 1. = 65536/65536
        returnStake(sAddr, queryId, 6);
        return;
    }

    // parse current election data
    var election = Election.fromCell(electionDict!, { assertEndAfterReading: true });
    msgValue -= ton("1"); // deduct GR$1 for sending confirmation
    if ((msgValue << 12) < election.totalStake) {
        // stake smaller than 1/4096 of the total accumulated stakes, return
        returnStake(sAddr, queryId, 2);
        return;
    }

    election.totalStake += msgValue; // (provisionally) increase total stake
    if (stakeAt != election.electAt) {
        // stake for some other elections, return
        returnStake(sAddr, queryId, 3);
        return;
    }
    if (election.finished) {
        // elections already finished, return stake
        returnStake(sAddr, queryId, 0);

        return;
    }
    var (member, found) = election.participants.uDictGet(256, validatorPubkey);
    if (found) {
        // entry found, merge stakes
        msgValue += member!.loadCoins();
        member!.skipBits(64); // skip timestamp and max_factor
        found = (srcAddr != member!.loadUint(256));
    }
    if (found) {
        // can make stakes for a public key from one address only
        returnStake(sAddr, queryId, 4);
        return;
    }
    if (msgValue < election.minStake) {
        // stake too small, return it
        returnStake(sAddr, queryId, 5);
        return;
    }
    assert(msgValue > 0, 44);

    acceptExternalMessage();

    // store stake in the dictionary
    election.participants.uDictSetBuilder(
        256,
        validatorPubkey,
        beginCell().storeCoins(msgValue).storeUint(blockchain.now(), 32).storeUint(maxFactor, 32)
            .storeUint(srcAddr, 256)
            .storeUint(adnlAddr, 256)
    );
    // gather and save election data
    val new_elect_dict = Election.toCell(election);
    contract.setData(beginCell().storeDict(new_elect_dict).storeSlice(ds).endCell());
    // return confirmation message
    if (queryId != 0) {
        sendConfirmation(sAddr, queryId, 0);
    }
}

struct UnfreezeResult {
    credits: dict
    recovered: coins
}

fun unfreezeWithoutBonuses(credits: dict, freezeDict: dict, totStakes: coins): UnfreezeResult {
    var new_credits = credits;
    var total: coins = 0;
    var recovered: coins = 0;
    var pubkey = -1;
    do {
        var (nextKey, csOpt, found) = freezeDict.uDictGetNext(256, pubkey);
        if (found) {
            pubkey = nextKey!;
            var cs = csOpt!;

            val addr = cs.loadUint(256);
            val _weight = cs.loadUint(64);
            val stake = cs.loadCoins();
            val banned = cs.loadInt(1) != 0;
            cs.assertEnd();

            if (banned) {
                recovered += stake;
            } else {
                new_credits = creditTo(new_credits, addr, stake);
            }
            total += stake;
        }
    } while (found);
    assert(total == totStakes, 59);
    return UnfreezeResult { credits: new_credits, recovered };
}

fun unfreezeWithBonuses(credits: dict, freezeDict: dict, totStakes: coins, totBonuses: coins): UnfreezeResult {
    var total: coins = 0;
    var recovered: coins = 0;
    var returnedBonuses: coins = 0;
    var pubkey = -1;
    do {
        var (nextKey, csOpt, found) = freezeDict.uDictGetNext(256, pubkey);
        if (found) {
            pubkey = nextKey!;
            var cs = csOpt!;
            val addr = cs.loadUint(256);
            val _ = cs.loadUint(64); //weight
            val stake = cs.loadCoins();
            val banned = cs.loadInt(1) != 0;
            cs.assertEnd();

            if (banned) {
                recovered += stake;
            } else {
                var bonus = mulDivFloor(totBonuses, stake, totStakes);
                returnedBonuses += bonus;
                credits = creditTo(credits, addr, stake + bonus);
            }
            total += stake;
        }
    } while (found);
    assert((total == totStakes) & (returnedBonuses <= totBonuses), 59);
    return UnfreezeResult { credits, recovered: recovered + totBonuses - returnedBonuses };
}

fun unfreezeAll(credits: dict, pastElections: dict, electId: int): (dict, dict, coins) {
    var (fsOpt, found) = pastElections.uDictDeleteAndGet(32, electId);
    if (!found) {
        // no elections with this id
        return (credits, pastElections, 0);
    }
    var fs = fsOpt!;
    var pe: PastElection = PastElection.fromSlice(fs, { assertEndAfterReading: true });
    var result = pe.bonuses > 0
         ? unfreezeWithBonuses(credits, pe.frozenDict, pe.totalStake, pe.bonuses)
         : unfreezeWithoutBonuses(credits, pe.frozenDict, pe.totalStake);
    return (result.credits, pastElections, result.recovered);
}

// Config set confirmed
fun configSetConfirmed(sAddr: address, cs: slice, queryId: int, success: bool) {
    val (srcWc, srcAddr) = sAddr.getWorkchainAndHash();
    val configAddr = blockchain.configParam(0)!.beginParse().preloadUint(256);
    var ds = contract.getData().beginParse();
    var electionDict = ds.loadDict();
    if ((srcWc + 1) != BASECHAIN || (srcAddr != configAddr) || electionDict == null) {
        return;
    }
    var election = Election.fromCell(electionDict!, { assertEndAfterReading: true });
    if ((election.electAt != queryId) || !election.finished) {
        //TODO: double check this condition
        // not these elections, or elections not finished yet
        return;
    }

    acceptExternalMessage();

    if (!success) {
        // cancel elections, return stakes
        val (credits, pastElections, grams) = (ds.loadDict(), ds.loadDict(), ds.loadCoins());
        val (newCredits, newPastElections, unusedPrizes) = unfreezeAll(
            credits,
            pastElections,
            election.electAt
        );
        contract.setData(beginCell().storeInt(0, 1).storeDict(newCredits).storeDict(newPastElections)
            .storeCoins(grams + unusedPrizes)
            .storeSlice(ds)
            .endCell());
    }
}
// ... do not remove elect until we see this set as the next elected validator set
// Recover stake
fun recoverStake(op: int, sAddr: address, cs: slice, queryId: int) {
    var (srcWc, srcAddr) = sAddr.getWorkchainAndHash();
    if ((srcWc + 1) != 0) {
        // not from masterchain, return error
        sendMessageBack(
            sAddr,
            0xfffffffe,
            queryId,
            op,
            0,
            SEND_MODE_CARRY_ALL_REMAINING_MESSAGE_VALUE
        );
        return;
    }
    var ds = contract.getData().beginParse();
    var (elect, credits) = (ds.loadDict(), ds.loadDict());
    var (dataCs, found) = credits.uDictDeleteAndGet(256, srcAddr);
    if (!found) {
        // no credit for sender, return error
        sendMessageBack(
            sAddr,
            0xfffffffe,
            queryId,
            op,
            0,
            SEND_MODE_CARRY_ALL_REMAINING_MESSAGE_VALUE
        );
        return;
    }
    var amount = dataCs!.loadCoins();
    dataCs!.assertEnd();
    // save data
    contract.setData(beginCell().storeDict(elect).storeDict(credits).storeSlice(ds).endCell());
    // send amount to sender in a new message
    val reply = createMessage({
        bounce: true,
        value: amount,
        dest: sAddr,
        body: (0xf96f7324 as uint64, queryId as uint32),
    });

    reply.send(SEND_MODE_CARRY_ALL_REMAINING_MESSAGE_VALUE);
}

fun afterCodeUpgrade(sAddr: address, cs: slice, queryId: int) {
    sendMessageBack(sAddr, 0xce436f64, queryId, upgradeCodeMessageTag, 0, 64);
}

fun upgradeCode(sAddr: address, cs: slice, queryId: int): bool {
    val cAddr = blockchain.configParam(0);
    if (cAddr == null) {
        return false;
    }

    val configAddr = cAddr!.beginParse().preloadUint(256);
    val (srcWc, srcAddr) = sAddr.getWorkchainAndHash();

    if ((srcWc + 1) != BASECHAIN || (srcAddr != configAddr)) {
        return false;
    }

    acceptExternalMessage();
    val code = cs.loadRef();
    contract.setCodePostponed(code);

    if (!cs.isEmpty()) {
        setTvmRegisterC3(transformSliceToContinuation(code.beginParse()));
        afterCodeUpgrade(sAddr, cs, queryId);
        throw 0
    }

    return true;
}

// === Getters ===
/// Returns active election id or 0.
get fun active_election_id(): int {
    val data = lazy Storage.load();
    if (data.currentElection == null) {
        return 0;
    }

    var election = lazy data.currentElection.load();
    return election.electAt;
}

/// Checks whether a public key participates in current elections.
get fun participates_in(validatorPubkey: int): coins {
    val data = lazy Storage.load();
    if (data.currentElection == null) {
        return 0;
    }

    val election = lazy data.currentElection.load();
    var (cs, found) = election.participants.uDictGet(256, validatorPubkey);
    if (!found) {
        return 0;
    }

    val member = lazy ElectionParticipant.fromSlice(cs!);
    return member.stake;
}

/// Returns the list of all participants of current elections with their stakes.
get fun participant_list(): tuple {
    val data = lazy Storage.load();
    if (data.currentElection == null) {
        return createEmptyTuple();
    }

    val election = lazy data.currentElection.load();
    var l = createEmptyTuple();
    var nextId = maxInt();
    do {
        var (id, cs, found) = election.participants.uDictGetPrev(256, nextId);
        if (found) {
            val participant = lazy ElectionParticipant.fromSlice(cs!);
            l = listPrepend([id, participant.stake], l);
            nextId = id!;
        }
    } while (found);
    return l;
}

// returns the list of all participants of current elections with their data
get fun participant_list_extended(): tuple {
    val data = lazy Storage.load();
    if (data.currentElection == null) {
        return [0, 0, 0, 0, [], 0, 0] as tuple;
    }

    val election = lazy data.currentElection.load();
    var l = createEmptyTuple();
    var nextId = maxInt();
    do {
        var (id, cs, found) = election.participants.uDictGetPrev(256, nextId);
        if (found) {
            val participant = lazy ElectionParticipant.fromSlice(cs!);
            val info = [
                participant.stake, participant.maxFactor, participant.addr, participant.adnlAddr
            ];
            l = listPrepend([id, info], l);
            nextId = id!;
        }
    } while (found);

    return [
        election.electAt, election.electClose, election.minStake, election.totalStake, l, election
            .failed, election.finished
    ] as tuple;
}

// computes the return stake
get fun compute_returned_stake(walletAddr: int): coins {
    val data = lazy Storage.load();
    var (cs, found) = data.credits.uDictGet(256, walletAddr);
    return found ? cs!.loadCoins() : 0;
}

// returns the list of past election ids
get fun past_election_ids(): tuple {
    val data = lazy Storage.load();
    var nextId = (1 << 32);
    var list = createEmptyTuple();
    do {
        var (id, _, found) = data.pastElections.uDictGetPrev(32, nextId);
        if (found) {
            list = listPrepend(id!, list);
            nextId = id!;
        }
    } while (found);
    return list;
}

// === Stuff ===
@pure
fun maxInt(): int
    asm "256 PUSHPOW2DEC"

fun cell?.hashEq(self, expectedVsetHash: int): bool {
    if (self == null) {
        return false;
    } else {
        return expectedVsetHash == self.hash();
    }
}
