import "@stdlib/gas-payments"
import "lib/config-params"

const ERROR_INVALID_SIGNATURE = 40
const ERROR_VALIDATOR_NOT_FOUND = 50
const ERROR_REPLAY_PROTECTION = 52
const ERROR_MESSAGE_EXPIRED = 57

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

fun onInternalMessage(_in: InMessage) {}

fun onExternalMessage(inMsg: slice) {
    val signature = inMsg.loadBits(512);
    val signedBody = inMsg;
    val createdAtMs = inMsg.loadUint(64);
    val expireAtSec = inMsg.loadUint(32);
    val validatorIdx = inMsg.loadUint(16);
    val _batch = inMsg.loadRef();
    inMsg.assertEnd();
    assert(blockchain.now() <= expireAtSec, ERROR_MESSAGE_EXPIRED);

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
