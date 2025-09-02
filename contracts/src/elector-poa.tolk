import "@stdlib/tvm-dicts"
import "elector-base"
import "interface/elector"
import "lib/config-params"
import "lib/util"

tolk 1.0

const STAKE_ERR_NOT_IN_WHITELIST = 100

const ELECTOR_POA_OP_ADD_ADDRESS = 0x6e764f6f
const ELECTOR_POA_OP_REMOVE_ADDRESS = 0x56efd52d

const ANSWER_TAG_POA_WHITELIST_UPDATED = 0xbc06677e

const WHITELIST_KEY_LEN = 256

struct PoAData {
    /// List of validator addresses which can participate in elections.
    ///
    /// uint256 (validator address) => `Unit`.
    whitelist: dict
}

//
// === Tick-Tock transaction logic ===
//
/// Sync state at the beginning and end of the masterchain block.
fun onRunTickTock(_isTock: bool) {
    var data = lazy Storage<PoAData>.load();
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
        processSimpleTransfer<PoAData>(in.senderAddress, in.valueCoins);
        return;
    }

    val op = body.loadUint(32);
    if (op == 0) {
        // Simple transfer with comment.
        processSimpleTransfer<PoAData>(in.senderAddress, in.valueCoins);
        return;
    }

    val queryId = body.loadUint(64);
    match (op) {
        ELECTOR_OP_NEW_STAKE => {
            if (canParticipateInElection(in.senderAddress)) {
                processNewStake<PoAData>(in.senderAddress, in.valueCoins, body, queryId);
            } else {
                returnStake(in.senderAddress, queryId, STAKE_ERR_NOT_IN_WHITELIST);
            }
        }
        ELECTOR_OP_RECOVER_STAKE => {
            recoverStake<PoAData>(in.senderAddress, queryId);
        }
        ELECTOR_OP_UPGRADE_CODE => {
            val ok = upgradeCode(in.senderAddress, body, queryId);
            val tag = ok ? ANSWER_TAG_CODE_ACCEPTED : ANSWER_TAG_ERROR;
            sendMessageBack(in.senderAddress, tag, queryId, op);
        }
        ELECTOR_OP_CONFIRM_VSET => {
            processConfigVsetResponse<PoAData>(in.senderAddress, queryId, true);
        }
        ELECTOR_OP_REJECT_VSET => {
            processConfigVsetResponse<PoAData>(in.senderAddress, queryId, false);
        }
        ELECTOR_POA_OP_ADD_ADDRESS => {
            processManageWhitelist(in.senderAddress, queryId, body, true);
        }
        ELECTOR_POA_OP_REMOVE_ADDRESS => {
            processManageWhitelist(in.senderAddress, queryId, body, false);
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
    var oldData = contract.getData().beginParse().getFirstAny<Storage<()>>();

    var newData = beginCell() //
        .storeSlice(oldData)
        .storeAny(PoAData { whitelist: null })
        .endCell();
    contract.setData(newData);

    sendMessageBack(source, ANSWER_TAG_CODE_ACCEPTED, queryId, ELECTOR_OP_UPGRADE_CODE);
}

//
// === Logic ===
//
fun canParticipateInElection(source: address): bool {
    val data = lazy Storage<PoAData>.load();
    if (data.customData.whitelist == null) {
        return true;
    }

    val (wc, addr) = source.getWorkchainAndHash();
    var (_, found) = data.customData.whitelist.uDictGet(WHITELIST_KEY_LEN, addr);
    return wc == MASTERCHAIN && found;
}

fun processManageWhitelist(source: address, queryId: int, body: slice, add: bool) {
    var answerTag = manageWhitelist(source, body, add)
         ? ANSWER_TAG_POA_WHITELIST_UPDATED
         : ANSWER_TAG_ERROR;

    sendMessageBack(
        source,
        answerTag,
        queryId,
        add ? ELECTOR_POA_OP_ADD_ADDRESS : ELECTOR_POA_OP_REMOVE_ADDRESS
    );
}

fun manageWhitelist(source: address, body: slice, add: bool): bool {
    val (wc, sourceAddr) = source.getWorkchainAndHash();
    val managerAddrParam = blockchain.configParam(PARAM_IDX_ELECTOR_POA_MANAGER);
    val managerAddr = managerAddrParam == null ? -1 : managerAddrParam.beginParse().loadUint(256);
    if (wc != MASTERCHAIN || sourceAddr != managerAddr) {
        return false;
    }

    val whitelistedAddr = body.loadBits(256);
    body.assertEnd();

    var data = lazy Storage<PoAData>.load();
    if (add) {
        data.customData.whitelist.sDictSet(WHITELIST_KEY_LEN, whitelistedAddr, createEmptySlice());
    } else {
        data.customData.whitelist.sDictDelete(WHITELIST_KEY_LEN, whitelistedAddr);
    }
    return true;
}
