import "@stdlib/gas-payments"
import "@stdlib/lisp-lists"
import "@stdlib/tvm-dicts"
import "@stdlib/tvm-lowlevel"
import "lib/config-params"

tolk 1.0

//
// === Constants ===
//
const MSG_TAG_NEW_STAKE = 0x4e73744b
const MSG_TAG_RECOVER_STAKE = 0x47657424
const MSG_TAG_UPGRADE_CODE = 0x4e436f64
const MSG_TAG_VSET_CONFIRMED = 0xee764f4b
const MSG_TAG_VSET_REJECTED = 0xee764f6f

const ANSWER_TAG_STAKE_REJECTED = 0xee6f454c
const ANSWER_TAG_STAKE_ACCEPTED = 0xf374484c
const ANSWER_TAG_STAKE_RECOVERED = 0xf96f7324
const ANSWER_TAG_CODE_ACCEPTED = 0xce436f64
const ANSWER_TAG_NO_STAKE_TO_RECOVER = 0xfffffffe
const ANSWER_TAG_ERROR = 0xffffffff

const CONFIG_MSG_TAG_SET_NEXT_VSET = 0x4e565354

const STAKE_ERR_NO_ELECTION = 0
const STAKE_ERR_INVALID_SIGNATURE = 1
const STAKE_ERR_DIFF_TOO_SMALL = 2
const STAKE_ERR_ELECTION_ID_MISMATCH = 3
const STAKE_ERR_ADDR_CHANGED = 4
const STAKE_ERR_STAKE_TOO_SMALL = 5
const STAKE_ERR_MAX_FACTOR_TOO_SMALL = 6

@pure
fun msg_value_update_vset(): coins
    asm "1000000000 int_or_env ELECTOR_MSG_VALUE_UPDATE_VSET"

@pure
fun msg_value_stake_accepted(): coins
    asm "1000000000 int_or_env ELECTOR_MSG_VALUE_STAKE_ACCEPTED"

//
// === Storage Models ===
//
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

/// Loads C4 and parses it into a typed struct.
fun Storage.load(): Storage {
    return Storage.fromCell(contract.getData());
}

/// Serializes struct and stores it into C4.
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
    /// Minimal total stake for election to be considered successful.
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
    /// The sum of all stakes.
    totalStake: coins
    /// Accumulated fees for this validation round.
    bonuses: coins
    /// Unused for now
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

//
// === Tick-Tock transaction logic ===
//
/// Sync state at the beginning and end of the masterchain block.
fun onRunTickTock(_isTock: bool) {
    var data = lazy Storage.load();
    if (data.currentElection != null) {
        // Sync current election state if there is any.
        var election: Election = data.currentElection!.load();

        // Try to form a new vset.
        if (conductElection(data, election)) {
            // New vset formed. Wait for a new transaction.
            return;
        }

        // Check that `currentElection` vset was installed.
        if (syncElectionWithVset(data, election)) {
            // Validator set installed. Wait for a new transaction.
            return;
        }
    } else {
        // Otherwise try to start a new election.
        if (announceNewElection(data)) {
            // New election started. Wait for a new transaction.
            return;
        }
    }

    // Sync current validator set info.
    if (updateActiveVsetId(data)) {
        return; // active validator set id updated, exit
    }

    // Unfreeze stakes if any.
    checkUnfreeze(data);
}

//
// === Internal messages logic ===
//
/// Handles on-chain queries and answers.
fun onInternalMessage(in: InMessage) {
    var body = in.body;

    if (body.isEmpty()) {
        // Inbound message has empty body.
        processSimpleTransfer(in.senderAddress, in.valueCoins);
        return;
    }

    val op = body.loadUint(32);
    if (op == 0) {
        // Simple transfer with comment.
        processSimpleTransfer(in.senderAddress, in.valueCoins);
        return;
    }

    val queryId = body.loadUint(64);
    match (op) {
        MSG_TAG_NEW_STAKE => {
            processNewStake(in.senderAddress, in.valueCoins, body, queryId);
        }
        MSG_TAG_RECOVER_STAKE => {
            recoverStake(in.senderAddress, queryId);
        }
        MSG_TAG_UPGRADE_CODE => {
            val ok = upgradeCode(in.senderAddress, body, queryId);
            val tag = ok ? ANSWER_TAG_CODE_ACCEPTED : ANSWER_TAG_ERROR;
            sendMessageBack(in.senderAddress, tag, queryId, op);
        }
        MSG_TAG_VSET_CONFIRMED => {
            processConfigVsetResponse(in.senderAddress, queryId, true);
        }
        MSG_TAG_VSET_REJECTED => {
            processConfigVsetResponse(in.senderAddress, queryId, false);
        }
        else => {
            if ((op & (1 << 31)) == 0) {
                // Operations without the highest bit are "queries".
                // If the query is unknown we are answering with an error here.
                sendMessageBack(in.senderAddress, ANSWER_TAG_ERROR, queryId, op);
            }
        }
    }
}

//
// === Code upgrade handler ===
//
/// Handles code upgrade.
///
/// It has a fixed method id and is called with a new code
/// after an `UPGRADE_CODE` message was received and
/// it contained some arguments besides the code.
@method_id(1666)
fun afterCodeUpgrade(source: address, _args: slice, queryId: int) {
    sendMessageBack(source, ANSWER_TAG_CODE_ACCEPTED, queryId, MSG_TAG_UPGRADE_CODE);
}

//
// === Getters ===
//
/// Returns an active election id or 0 if none.
get fun active_election_id(): int {
    val data = lazy Storage.load();
    if (data.currentElection == null) {
        return 0;
    }

    val election = lazy data.currentElection.load();
    return election.electAt;
}

/// Checks whether the public key participates in the current election.
///
/// Returns participant's stake if yes and 0 otherwise.
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

    val participant = lazy ElectionParticipant.fromSlice(cs!);
    return participant.stake;
}

/// Returns the list of all participants of current elections with their stakes.
get fun participant_list(): tuple {
    val data = lazy Storage.load();
    if (data.currentElection == null) {
        return createEmptyTuple();
    }

    val election = lazy data.currentElection.load();
    var l = createEmptyTuple();
    var iterNext = maxInt();
    do {
        var (id, cs, found) = election.participants.uDictGetPrev(256, iterNext);
        if (found) {
            iterNext = id!;
            val participant = lazy ElectionParticipant.fromSlice(cs!);
            l = listPrepend([id, participant.stake], l);
        }
    } while (found);
    return l;
}

/// Returns a list of all participants of current election with their data.
get fun participant_list_extended(): tuple {
    val data = lazy Storage.load();
    if (data.currentElection == null) {
        return [0, 0, 0, 0, [], 0, 0] as tuple;
    }

    val election = lazy data.currentElection.load();
    var l = createEmptyTuple();
    var iterNext = maxInt();
    do {
        var (id, cs, found) = election.participants.uDictGetPrev(256, iterNext);
        if (found) {
            iterNext = id!;
            val participant = lazy ElectionParticipant.fromSlice(cs!);
            val info = [
                participant.stake,
                participant.maxFactor,
                participant.addr,
                participant.adnlAddr
            ];
            l = listPrepend([id, info], l);
        }
    } while (found);

    return [
        election.electAt,
        election.electClose,
        election.minStake,
        election.totalStake,
        l,
        election.failed,
        election.finished
    ] as tuple;
}

// Returns a stake for the address.
get fun compute_returned_stake(walletAddr: int): coins {
    val data = lazy Storage.load();
    var (cs, found) = data.credits.uDictGet(256, walletAddr);
    return found ? cs!.loadCoins() : 0;
}

// Returns a list of past election ids
get fun past_election_ids(): tuple {
    val data = lazy Storage.load();
    var iterNext = (1 << 32);
    var l = createEmptyTuple();
    do {
        var (id, _, found) = data.pastElections.uDictGetPrev(32, iterNext);
        if (found) {
            iterNext = id!;
            l = listPrepend(id!, l);
        }
    } while (found);
    return l;
}

//
// === Logic ===
//
/// Tries to finish the current election and form a new validator set.
///
/// In case of successful election sends a message to the config contract
/// with a new validator set. Note that it doesn't immediately reset
/// current election. It will wait for a config response instead.
///
/// Returns `false` if no validator set was formed. In that case
/// we can execute some other logic.
///
/// Mutates persistent storage fields:
/// - currentElection
/// - pastElections
/// - credits
fun conductElection(data: Storage, election: Election): bool {
    if (blockchain.now() < election.electClose) {
        // Election not finished yet.
        return false;
    }
    if (blockchain.configParam(0) == null) {
        // No configuration smart contract to send result to.
        return postponeElection();
    }

    val stakeParams = ValidatorSetStakeConfig.load();
    if (election.totalStake < stakeParams.minTotalStake) {
        // Insufficient total stake, postpone election.
        return postponeElection();
    }
    if (election.failed) {
        // Do not retry failed election until new stakes arrive.
        return postponeElection();
    }
    if (election.finished) {
        // Election finished.
        return false;
    }

    var countParams = ValidatorSetSizeConfig.load();
    countParams.minValidators = max(countParams.minValidators, 1);

    val result = ElectionResult.compute(
        data.credits,
        election.participants,
        stakeParams,
        countParams
    );
    val failed = result.validatorCount == 0;
    election.failed = failed;
    election.finished = !failed;
    if (failed) {
        // Election failed, update stored state with updated flags.
        data.credits = result.credits;
        data.currentElection = election.toCell();
        data.save();
        return postponeElection();
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
    }.toCell();

    // Notify config contract with a new validator set.
    var configAddress = blockchain.configParam(0)!.beginParse().preloadUint(256);
    createMessage({
        bounce: true,
        value: msg_value_update_vset(),
        dest: (MASTERCHAIN, configAddress),
        body: beginCell() //
            .storeUint(CONFIG_MSG_TAG_SET_NEXT_VSET, 32)
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
    data.pastElections.uDictSetBuilder(32, election.electAt, beginCell().storeAny(pastElection));

    data.credits = result.credits;
    data.currentElection = election.toCell();
    data.save();

    return true;
}

/// Named alias for a postponed elections outcome.
fun postponeElection(): bool {
    return false;
}

struct ElectionResult {
    credits: dict
    validators: dict
    totalWeight: int
    frozen: dict
    totalStake: coins
    validatorCount: int
}

/// Computes possible validator set entries for specified parameters.
fun ElectionResult.compute(
    credits: dict,
    participants: dict,
    stakeConfig: ValidatorSetStakeConfig,
    sizeConfig: ValidatorSetSizeConfig,
): ElectionResult {
    // Count and sort all members by a composite key of (stake, reverse(time), pubkey).
    var stakeCount = 0;
    var sortedMembers = createEmptyDict();
    var iterNext = -1;
    do {
        var (pubkey, cs, found) = participants.uDictGetNext(256, iterNext);
        if (found) {
            iterNext = pubkey!;
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
        assert (memberExists, 61);
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
            creditTo(mutate credits, addr, stake);
        }

        i += 1;
    } while (tail != null);

    assert (totalStake == bestStake, 49);

    return ElectionResult {
        credits,
        validators,
        totalWeight,
        frozen,
        totalStake,
        validatorCount: selectedCount,
    };
}

fun ElectionResult.empty(credits: dict) {
    return ElectionResult {
        credits,
        validators: createEmptyDict(),
        totalWeight: 0,
        frozen: createEmptyDict(),
        totalStake: 0,
        validatorCount: 0,
    };
}

/// Сomputes the total stake out of the first n entries of all stakes.
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

/// Ensures that `currentElection` is reset when its validator set
/// is present in a blockchain config.
///
/// In case of successful reset also ensures that `activeId` and
/// `activeHash` are in sync with the config.
///
/// Returns `false` if nothing changed. In that case we can
/// execute some other logic.
///
/// Mutates persistent storage fields:
/// - currentElection
/// - pastElections
/// - grams
/// - activeId
/// - activeHash
fun syncElectionWithVset(data: Storage, election: Election): bool {
    if (!election.finished) {
        // Election not finished yet.
        return false;
    }
    val (cs, found) = data.pastElections.uDictGet(32, election.electAt);
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
        data.currentElection = null;
        data.save();

        updateActiveVsetId(data);
        return true;
    }

    return false;
}

/// Tries to start a new election.
///
/// If it is time to start the new election and there is no next vset,
/// resets `currentElection` using the current blockchain config params.
///
/// Returns `false` if nothing changed. In that case we can
/// execute some other logic.
///
/// Mutates persistent storage fields:
/// - currentElection
fun announceNewElection(data: Storage): bool {
    if (blockchain.configParam(PARAM_IDX_NEXT_VSET) != null) {
        // Next validator set exists, no election needed.
        return false;
    }

    // Check that current account is an elector.
    var electorAddr = blockchain.configParam(1)!.beginParse().preloadUint(256);
    var (myWc, myAddr) = contract.getAddress().getWorkchainAndHash();
    if (myWc != MASTERCHAIN || myAddr != electorAddr) {
        // This smart contract is not an elector smart contract anymore, no new election.
        return false;
    }

    // Get time when the current validator set expires.
    var currentVsetCs = blockchain.configParam(PARAM_IDX_CURRENT_VSET);
    if (currentVsetCs == null) {
        return false;
    }
    val currentVset = lazy ValidatorSet.fromCell(currentVsetCs!);

    val timingsConfig = ValidatorSetTimingsConfig.load();
    var t = blockchain.now();
    val t0 = currentVset.utimeUntil - timingsConfig.electionsBeginBefore;
    if (t < t0) {
        // Too early for the next election.
        return false;
    }
    // Less than `electionsBeginBefore` seconds left, create new election.
    if (t - t0 < 60) {
        // pretend that the election started at t0
        t = t0;
    }

    // Get stake parameters
    val stakeConfig = ValidatorSetStakeConfig.load();

    // Init current election.
    val electAt = t + timingsConfig.electionsBeginBefore;
    val electClose = electAt - timingsConfig.electionsEndBefore;

    data.currentElection = Election {
        electAt,
        electClose,
        minStake: stakeConfig.minStake,
        totalStake: 0,
        participants: createEmptyDict(),
        failed: false,
        finished: false,
    }.toCell();
    data.save();

    return true;
}

/// Ensures that `activeId` and `activeHash` are in sync with the current
/// blockchain config state.
///
/// When validator set changes also updates `pastElections` so that:
/// - an entry for the previous known id has a valid `unfreezeAt`;
/// - an entry for the newly known set received a portion of unused bonuses.
///
/// Returns `false` if nothing changed. In that case we can
/// execute some other logic.
///
/// Mutates persistent storage fields:
/// - pastElections
/// - grams
/// - activeId
/// - activeHash
fun updateActiveVsetId(data: Storage): bool {
    val currentVsetHash = blockchain.configParam(PARAM_IDX_CURRENT_VSET)!.hash();
    if (currentVsetHash == data.activeHash) {
        // Validator set unchanged.
        return false;
    }

    if (data.activeId != 0) {
        // activeId becomes inactive
        val (cs, found) = data.pastElections.uDictGet(32, data.activeId);
        if (found) {
            var pastElection = lazy PastElection.fromSlice(cs!);
            assert (pastElection.vsetHash == data.activeHash, 57);

            // Adjust unfreeze time of this validator set.
            pastElection.unfreezeAt = blockchain.now() + pastElection.stakeHeld;
            data.pastElections.uDictSetBuilder(
                32,
                data.activeId,
                beginCell().storeAny(pastElection)
            );
        }
    }

    // Look up new activeId by hash.
    var iterNext = -1;
    do {
        var (id, cs, found) = data.pastElections.uDictGetNext(32, iterNext);
        if (found) {
            iterNext = id!;
            var pastElection = lazy PastElection.fromSlice(cs!);

            if (pastElection.vsetHash == currentVsetHash) {
                // Transfer 1/8 of accumulated everybody's grams to this validator set as bonuses.
                var amount = (data.grams >> 3);
                data.grams -= amount;
                pastElection.bonuses += amount;

                // Serialize back
                data.pastElections.uDictSetBuilder(32, iterNext, beginCell().storeAny(pastElection));

                // Break.
                found = false;
            }
        }
    } while (found);

    // Save active election id and vset hash.
    data.activeId = iterNext == -1 ? 0 : iterNext;
    data.activeHash = currentVsetHash;
    data.save();

    return true;
}

/// Tries to unfreeze stakes for some past elections.
///
/// Unfreezes only for a single entry at once.
/// Unfreezed funds are moved to the `credits` dict for
/// each participant that was not banned. All undistributed
/// funds are added to `grams`.
///
/// Returns `false` if nothing changed. In that case we can
/// execute some other logic.
///
/// Mutates persistent storage fields:
/// - credits
/// - pastElections
/// - grams
fun checkUnfreeze(data: Storage): bool {
    var iterNext = -1;
    do {
        var (id, cs, found) = data.pastElections.uDictGetNext(32, iterNext);
        if (found) {
            iterNext = id!;
            var pastElection = lazy PastElection.fromSlice(cs!);

            if ((pastElection.unfreezeAt <= blockchain.now()) && (iterNext != data.activeId)) {
                // Unfreeze only one at time, exit loop.
                data.unfreezeAll(iterNext);
                data.save();
                found = false;
            }
        }
    } while (found);
    return iterNext != -1;
}

/// Processes funds which were received without a query.
///
/// Adds funds to the current validator set if any when the source is `-1:00..00`.
/// Otherwise adds them to `grams.
///
/// Mutates persistent storage fields:
/// - pastElections
/// - grams
fun processSimpleTransfer(source: address, value: coins) {
    var data = lazy Storage.load();
    val (wc, addr) = source.getWorkchainAndHash();
    if (addr != 0 || wc != MASTERCHAIN || data.activeId == 0) {
        // There are no active election or the source address is not a "minter".
        // FIXME: Should we compare the source address with config param 2?
        data.grams += value;
        data.save();
        return;
    }

    // Handle transfer from `-1:00...00` with new validator fees.
    val (cs, found) = data.pastElections.uDictGet(32, data.activeId);
    if (found) {
        var pastElection = lazy PastElection.fromSlice(cs!);
        pastElection.bonuses += value;
        data.pastElections.uDictSetBuilder(32, data.activeId, beginCell().storeAny(pastElection));
    } else {
        data.grams += value;
    }

    data.save();
}

struct (0x654c5074) SignedStake {
    stakeAt: uint32
    maxFactor: uint32
    addr: uint256
    adnlAddr: uint256
}

/// Processes a stake from participant.
///
/// Validates stake info and sends back either a confirmation
/// with `MSG_VALUE_STAKE_ACCEPTED` tokens or rejection
/// with remaining original message balance.
///
/// Mutates persistent storage fields:
/// - currentElection
fun processNewStake(source: address, value: coins, body: slice, queryId: int) {
    val (wc, addr) = source.getWorkchainAndHash();
    var data = lazy Storage.load();
    if (data.currentElection == null || wc != MASTERCHAIN) {
        // No election active, or source is not in masterchain.
        returnStake(source, queryId, STAKE_ERR_NO_ELECTION);
        return;
    }

    // Parse the remainder of a new stake message.
    val pubkey = body.loadUint(256);
    val stakeAt = body.loadUint(32);
    val maxFactor = body.loadUint(32);
    val adnlAddr = body.loadUint(256);

    var signature = body.loadRef().beginParse();
    if (signature.remainingBitsCount() != 512 || signature.remainingRefsCount() != 0) {
        // Invalid signature slice.
        returnStake(source, queryId, STAKE_ERR_INVALID_SIGNATURE);
        return;
    }
    body.assertEnd();

    var signedData = SignedStake {
        stakeAt,
        maxFactor,
        addr,
        adnlAddr,
    }.toCell();
    if (!isSliceSignatureValid(signedData.beginParse(), signature, pubkey)) {
        // Invalid signature.
        returnStake(source, queryId, STAKE_ERR_INVALID_SIGNATURE);
        return;
    }

    if (maxFactor < 0x10000) {
        // Factor must be >= 1. = 65536/65536.
        returnStake(source, queryId, STAKE_ERR_MAX_FACTOR_TOO_SMALL);
        return;
    }

    // Parse current election data.
    var election = lazy data.currentElection.load();
    value -= msg_value_stake_accepted(); // deduct GR$1 for sending confirmation
    if ((value << 12) < election.totalStake) {
        // Stake smaller than 1/4096 of the total accumulated stakes.
        returnStake(source, queryId, STAKE_ERR_DIFF_TOO_SMALL);
        return;
    }

    election.totalStake += value; // (provisionally) increase total stake
    if (stakeAt != election.electAt) {
        // Stake for some other election.
        returnStake(source, queryId, STAKE_ERR_ELECTION_ID_MISMATCH);
        return;
    }
    if (election.finished) {
        // Election already finished.
        returnStake(source, queryId, STAKE_ERR_NO_ELECTION);
        return;
    }

    var (cs, found) = election.participants.uDictGet(256, pubkey);
    if (found) {
        // Entry found, merge stakes.
        var participant = lazy ElectionParticipant.fromSlice(cs!);

        value += participant.stake;
        found = addr != participant.addr;
    }
    if (found) {
        // Can make stakes for a public key from one address only.
        returnStake(source, queryId, STAKE_ERR_ADDR_CHANGED);
        return;
    }

    if (value < election.minStake) {
        // Stake too small, return it.
        // NOTE: At this point the value could be increased by `participant.stake`
        //       so after that branch code will never go here because only
        //       entries with `stake >= election.minStake` were stored.
        returnStake(source, queryId, STAKE_ERR_STAKE_TOO_SMALL);
        return;
    }

    assert (value > 0, 44);
    setGasLimitToMaximum();

    // Store stake into the dictionary.
    val participant = ElectionParticipant {
        stake: value,
        time: blockchain.now(),
        maxFactor,
        addr,
        adnlAddr,
    };
    election.participants.uDictSetBuilder(256, pubkey, beginCell().storeAny(participant));

    // Update storage.
    data.currentElection = election.toCell();
    data.save();

    // Return confirmation message.
    if (queryId != 0) {
        val reply = createMessage({
            bounce: false,
            value: msg_value_stake_accepted(),
            dest: source,
            body: (ANSWER_TAG_STAKE_ACCEPTED as uint32, queryId as uint64),
        });
        reply.send(SEND_MODE_IGNORE_ERRORS);
    }
}

fun returnStake(addr: address, queryId: int, reason: int) {
    sendMessageBack(addr, ANSWER_TAG_STAKE_REJECTED, queryId, reason);
}

/// Process stake withdrawal request from a participant.
///
/// Removes `credits` entry for the `source` address.
/// Sends a new message with a recovered amount if the entry was found,
/// or rejection otherwise.
///
/// Mutates persistent storage fields:
/// - credits
fun recoverStake(source: address, queryId: int) {
    var (wc, addr) = source.getWorkchainAndHash();
    if (wc != MASTERCHAIN) {
        // Not from masterchain.
        sendMessageBack(source, ANSWER_TAG_NO_STAKE_TO_RECOVER, queryId, MSG_TAG_RECOVER_STAKE);
        return;
    }

    var data = lazy Storage.load();
    var (creditsCs, found) = data.credits.uDictDeleteAndGet(256, addr);
    if (!found) {
        // No credit for sender.
        sendMessageBack(source, ANSWER_TAG_NO_STAKE_TO_RECOVER, queryId, MSG_TAG_RECOVER_STAKE);
        return;
    }

    var amount = creditsCs!.loadCoins();
    creditsCs!.assertEnd();

    // Save data.
    data.save();

    // Send the unfrozen amount to sender in a new message.
    val reply = createMessage({
        // NOTE: We don't want to bounce this message back since the stake
        // will be lost if the validator wallet was frozen. With `bounce: false`
        // it will receive these funds and will be able to use them after unfreeze.
        bounce: false,
        value: amount,
        dest: source,
        body: (ANSWER_TAG_STAKE_RECOVERED as uint32, queryId as uint64),
    });
    reply.send(SEND_MODE_CARRY_ALL_REMAINING_MESSAGE_VALUE);
}

/// Update the code of this contract.
///
/// Only accepts messages from the blockchain config account.
///
/// Calls `afterCodeUpgrade` if `body` had some arguments
/// besides the new code. `afterCodeUpgrade` will be executed
/// on a new version of code and should contain a migration logic.
///
/// Mutates persistent storage fields:
/// - any fields that are updated inside `afterCodeUpgrade`
fun upgradeCode(source: address, body: slice, queryId: int): bool {
    val param0 = blockchain.configParam(0);
    if (param0 == null) {
        return false;
    }

    val configAddr = param0!.beginParse().preloadUint(256);
    val (wc, addr) = source.getWorkchainAndHash();
    if (wc != MASTERCHAIN || addr != configAddr) {
        return false;
    }

    setGasLimitToMaximum();

    val code = body.loadRef();
    contract.setCodePostponed(code);

    if (!body.isEmpty()) {
        // Execute `afterCodeUpgrade` on a new code if there are some args
        // left in the message body.
        setTvmRegisterC3(transformSliceToContinuation(code.beginParse()));
        afterCodeUpgrade(source, body, queryId);
        throw 0;
    }

    return true;
}

/// Processes a response for a "install vset" request.
///
/// Only accepts messages from the blockchain config account.
///
/// If config rejected our request we reset `currentElection`
/// and unfreeze all its stakes.
///
/// Mutates persistent storage fields:
/// - currentElection
/// - pastElections
/// - credits
/// - grams
fun processConfigVsetResponse(source: address, queryId: int, success: bool) {
    val (wc, addr) = source.getWorkchainAndHash();
    val configAddr = blockchain.configParam(0)!.beginParse().preloadUint(256);

    var data = lazy Storage.load();
    if (wc != MASTERCHAIN || addr != configAddr || data.currentElection == null) {
        return;
    }

    var election = lazy Election.fromCell(data.currentElection);
    if (election.electAt != queryId || !election.finished) {
        // Some other election, or it has not finished yet.
        return;
    }

    setGasLimitToMaximum();

    if (!success) {
        // Cancel current election, return stakes.
        data.unfreezeAll(election.electAt);
        data.currentElection = null;
        data.save();
    }
}

fun sendMessageBack(dest: address, functionTag: uint32, queryId: uint64, body: uint32) {
    val message = createMessage({
        bounce: false,
        value: 0,
        dest,
        body: (functionTag, queryId, body),
    });
    message.send(SEND_MODE_CARRY_ALL_REMAINING_MESSAGE_VALUE);
}

@inline_ref
fun Storage.unfreezeAll(mutate self, electionId: int) {
    var updated = unfreezeAll(self.credits, self.pastElections, electionId);
    self.credits = updated.credits;
    self.pastElections = updated.pastElections;
    self.grams += updated.recovered;
}

struct UnfreezeAllResult {
    credits: dict
    pastElections: dict
    recovered: coins
}

@inline
fun unfreezeAll(credits: dict, pastElections: dict, electionId: int): UnfreezeAllResult {
    var (cs, found) = pastElections.uDictDeleteAndGet(32, electionId);
    if (!found) {
        // No election with this id.
        return UnfreezeAllResult {
            credits,
            pastElections,
            recovered: 0,
        };
    }

    val pe = lazy PastElection.fromSlice(cs!);
    val recovered = pe.bonuses > 0
        ? unfreezeWithBonuses(mutate credits, pe.frozenDict, pe.totalStake, pe.bonuses)
        : unfreezeWithoutBonuses(mutate credits, pe.frozenDict, pe.totalStake);

    return UnfreezeAllResult {
        credits,
        pastElections,
        recovered,
    };
}

fun unfreezeWithBonuses(mutate credits: dict, frozenDict: dict, totalStake: coins, bonuses: coins): coins {
    var total = 0;
    var recovered = 0;
    var returnedBonuses = 0;
    var iterNext = -1;
    do {
        var (pubkey, cs, found) = frozenDict.uDictGetNext(256, iterNext);
        if (found) {
            iterNext = pubkey!;
            var item = lazy FrozenStake.fromSlice(cs!);

            if (item.banned) {
                recovered += item.stake;
            } else {
                var bonus = mulDivFloor(bonuses, item.stake, totalStake);
                returnedBonuses += bonus;
                creditTo(mutate credits, item.addr, item.stake + bonus);
            }
            total += item.stake;
        }
    } while (found);

    assert ((total == totalStake) && (returnedBonuses <= bonuses), 59);

    return recovered + bonuses - returnedBonuses;
}

fun unfreezeWithoutBonuses(mutate credits: dict, frozenDict: dict, totalStake: coins): coins {
    var total = 0;
    var recovered = 0;
    var iterNext = -1;
    do {
        var (pubkey, cs, found) = frozenDict.uDictGetNext(256, iterNext);
        if (found) {
            iterNext = pubkey!;
            var item = lazy FrozenStake.fromSlice(cs!);

            if (item.banned) {
                recovered += item.stake;
            } else {
                creditTo(mutate credits, item.addr, item.stake);
            }
            total += item.stake;
        }
    } while (found);

    assert (total == totalStake, 59);

    return recovered;
}

fun creditTo(mutate credits: dict, addr: int, amount: coins) {
    var (cs, found) = credits.uDictGet(256, addr);
    if (found) {
        amount += cs!.loadCoins();
    }
    credits.uDictSetBuilder(256, addr, beginCell().storeCoins(amount));
}
