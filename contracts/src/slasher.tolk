import "@stdlib/gas-payments"
import "@stdlib/common"
import "@stdlib/lisp-lists"
import "@stdlib/tvm-dicts"

import "slasher-base"
import "interface/slasher"


fun onInternalMessage(in: InMessage) {
    var body = in.body;

    val (wc, sender) = in.senderAddress.getWorkchainAndHash();
    if (wc != MASTERCHAIN || body.isEmpty()) {
        // Only masterchain addresses can execute any slasher logic.
        return;
    }

    val elector = blockchain.configParam(1)!.beginParse().preloadUint(256);
    if (sender != elector) {
        return; // todo: maybe some other logic?
    }

    val op = body.loadUint(32);
    val queryId = body.loadUint(32);
    body.assertEnd();

    match (op) {
        SLASHER_AGGREGATE_STATS_OP => {
            var slasher = lazy Slasher.load();
            val validatorsToPunish = slasher.runVoteAggregation();
            slasher.clearVotes();
            sendMessageBack(
                in.senderAddress,
                SLASHER_AGGREGATED_VALIDATORS_TAG,
                queryId,
                validatorsToPunish.toCell().beginParse() //todo: fix?
            );
        }
    }
}

fun onExternalMessage(inMsg: slice) {
    acceptExternalMessage();
    var storage = lazy Slasher.load();
    storage.processValidatorStatistics(inMsg);
}


/// Sends message back to the query source.
///
/// Attaches all remaining message value.
fun sendMessageBack(dest: address, answerTag: uint32, queryId: uint64, body: slice) {
    val message = createMessage({
        bounce: false,
        value: 0,
        dest,
        body: (answerTag, queryId, body),
    });
    message.send(SEND_MODE_CARRY_ALL_REMAINING_MESSAGE_VALUE);
}

/// GETTERS
get fun getPunishedValidators(): tuple {
    val data = lazy Slasher.load();
    if (data.punishedValidators == null) {
        return createEmptyList();
    }

    var punishedValidators = createEmptyList();
    val punished = data.punishedValidators!.load();

    var iterNext = -1;
    do {
        val (validatorId, votes, found) = data.votes.uDictGetNext(32, iterNext);
        if (found) {
            iterNext = validatorId!;
            listPrepend(validatorId!, punishedValidators);
        }
    } while (found);

    return [punished.blockSeqno, punishedValidators] as tuple;
}

get fun getCurrentVotes(): tuple {
    val data = lazy Slasher.load();

    var votersList = createEmptyList();
    var iterNext = -1;
    do {
        val (validatorId, votes, found) = data.votes.uDictGetNext(32, iterNext);
        if (found) {
            iterNext = validatorId!;
            listPrepend([validatorId!, votes], votersList);
        }
    } while (found);

    return votersList;
}
