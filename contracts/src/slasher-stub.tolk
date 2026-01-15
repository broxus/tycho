import "@stdlib/gas-payments"
import "@stdlib/tvm-dicts"
import "lib/config-params"

const ERROR_INVALID_SIGNATURE = 40
const ERROR_VALIDATOR_NOT_FOUND = 50
const ERROR_REPLAY_PROTECTION = 52
const ERROR_MESSAGE_EXPIRED = 57
const ERROR_INVALID_BLOCKS_BATCH = 100

const REPLAY_OFFSET_MS = 5000
const FUTURE_OFFSET_SEC = 60

const BATCH_SIZE = 10

struct Storage {
    updatedAtMs: uint64
}

fun Storage.load(): Storage {
    return Storage.fromCell(contract.getData());
}

fun Storage.save(self) {
    contract.setData(self.toCell());
}

//
// === Getters ===
//
get fun get_blocks_batch_size(): int {
    return BATCH_SIZE;
}

get fun is_blocks_batch_valid(batch: cell): bool {
    return validateBlocksBatch(batch.beginParse());
}

//
// === Logic ===
//
fun onInternalMessage(_in: InMessage) {}

fun onExternalMessage(inMsg: slice) {
    val signature = inMsg.loadBits(512);
    val signedBody = inMsg;
    val createdAtMs = inMsg.loadUint(64);
    val expireAtSec = inMsg.loadUint(32);
    val validatorIdx = inMsg.loadUint(16);
    val batch = inMsg.loadRef();
    inMsg.assertEnd();
    assert(blockchain.now() <= expireAtSec, ERROR_MESSAGE_EXPIRED);
    assert(validateBlocksBatch(batch.beginParse()), ERROR_INVALID_BLOCKS_BATCH);

    var data = Storage.load();
    assert(createdAtMs > (data.updatedAtMs - REPLAY_OFFSET_MS) &&
    createdAtMs <= (blockchain.now() + FUTURE_OFFSET_SEC) * 1000, ERROR_REPLAY_PROTECTION);

    var validatorCs = CurrentVset.getValidatorDescription(validatorIdx);
    assert(validatorCs != null, ERROR_VALIDATOR_NOT_FOUND);
    val validator = ValidatorDescr.readFromSlice(mutate validatorCs);

    val toSign = beginCell().storeSlice(signedBody).endCell();
    assert(isSignatureValid(toSign.hash(), signature, validator.pubkey), ERROR_INVALID_SIGNATURE);

    data.updatedAtMs = max(createdAtMs, data.updatedAtMs);
    data.save();

    acceptExternalMessage();
}

fun validateBlocksBatch(batch: slice): bool {
    // TODO: Assert that start seqno is recent enough and not from the future.
    val _startSeqno = batch.loadUint(32);
    batch.skipBits(BATCH_SIZE);
    val history = batch.loadRef() as dict;
    if (!batch.isEmpty()) {
        return false;
    }

    var iterNext = -1;
    do {
        val (validatorIdx, cs, found) = history.uDictGetNext(16, iterNext);
        if (found) {
            iterNext = validatorIdx!;

            // TODO: Check that validator idx is in the mc validators range.
            val (csBits, csRefs) = cs!.remainingBitsAndRefsCount();
            if (csBits != BATCH_SIZE * 2 || csRefs != 0) {
                return false;
            }
        }
    } while (found);

    return true;
}
