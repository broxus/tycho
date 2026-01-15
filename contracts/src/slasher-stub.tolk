import "@stdlib/gas-payments"
import "@stdlib/tvm-dicts"
import "lib/config-params"

const ERROR_INVALID_SIGNATURE = 40
const ERROR_VALIDATOR_NOT_FOUND = 50
const ERROR_REPLAY_PROTECTION = 52
const ERROR_MESSAGE_EXPIRED = 57
const ERROR_INVALID_BLOCKS_BATCH = 100
const ERROR_NO_SLASHER_CONFIG = 101

const REPLAY_OFFSET_MS = 5000
const FUTURE_OFFSET_SEC = 60

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
// === Slasher Config param ===
//
const PARAM_IDX_SLASHER_PARAMS = 666

struct (0x01) SlasherParams {
    address: bits256
    blocks_batch_size: uint8
}

//
// === Getters ===
//
get fun is_blocks_batch_valid(batch: cell): bool {
    val params = loadSlasherParams();
    return validateBlocksBatch(batch.beginParse(), params.blocks_batch_size);
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
    val params = loadSlasherParams();
    assert(validateBlocksBatch(batch.beginParse(), params.blocks_batch_size), ERROR_INVALID_BLOCKS_BATCH);

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

fun validateBlocksBatch(batch: slice, batch_size: int): bool {
    // TODO: Assert that start seqno is recent enough and not from the future.
    val _startSeqno = batch.loadUint(32);
    batch.skipBits(batch_size);
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
            if (csBits != batch_size * 2 || csRefs != 0) {
                return false;
            }
        }
    } while (found);

    return true;
}

fun loadSlasherParams(): SlasherParams {
    val param = blockchain.configParam(PARAM_IDX_SLASHER_PARAMS);
    assert(param != null, ERROR_NO_SLASHER_CONFIG);
    return SlasherParams.fromCell(param);
}
