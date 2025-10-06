import "elector-base"
import "interface/elector"

tolk 1.0

struct NoData {}

//
// === Tick-Tock transaction logic ===
//
/// Sync state at the beginning and end of the masterchain block.
fun onRunTickTock(_isTock: bool) {
    if (Storage<NoData>.hasCurrentElection()) {
        // Try to form a new vset.
        if (conductElection<NoData>()) {
            // New vset formed. Wait for a new transaction.
            return;
        }

        // Check that `currentElection` vset was installed.
        if (syncElectionWithVset<NoData>()) {
            // Validator set installed. Wait for a new transaction.
            return;
        }
    } else {
        // Otherwise try to start a new election.
        if (announceNewElection<NoData>()) {
            // New election started. Wait for a new transaction.
            return;
        }
    }

    // Sync current validator set info.
    if (updateActiveVsetId<NoData>()) {
        return; // active validator set id updated, exit
    }

    // Unfreeze stakes if any.
    checkUnfreeze<NoData>();
}

//
// === Internal messages logic ===
//
/// Handles on-chain queries and answers.
fun onInternalMessage(in: InMessage) {
    var body = in.body;

    if (body.isEmpty()) {
        // Inbound message has empty body.
        processSimpleTransfer<NoData>(in.senderAddress, in.valueCoins);
        return;
    }

    val op = body.loadUint(32);
    if (op == 0) {
        // Simple transfer with comment.
        processSimpleTransfer<NoData>(in.senderAddress, in.valueCoins);
        return;
    }

    val queryId = body.loadUint(64);
    match (op) {
        ELECTOR_OP_NEW_STAKE => {
            processNewStake<NoData>(in.senderAddress, in.valueCoins, body, queryId);
        }
        ELECTOR_OP_RECOVER_STAKE => {
            recoverStake<NoData>(in.senderAddress, queryId);
        }
        ELECTOR_OP_UPGRADE_CODE => {
            val ok = upgradeCode(in.senderAddress, body, queryId);
            val tag = ok ? ANSWER_TAG_CODE_ACCEPTED : ANSWER_TAG_ERROR;
            sendMessageBack(in.senderAddress, tag, queryId, op);
        }
        ELECTOR_OP_CONFIRM_VSET => {
            processConfigVsetResponse<NoData>(in.senderAddress, queryId, true);
        }
        ELECTOR_OP_REJECT_VSET => {
            processConfigVsetResponse<NoData>(in.senderAddress, queryId, false);
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
    sendMessageBack(source, ANSWER_TAG_CODE_ACCEPTED, queryId, ELECTOR_OP_UPGRADE_CODE);
}
