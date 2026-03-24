import "@stdlib/gas-payments"
import "@stdlib/tvm-dicts"
import "lib/config-params"

const ERROR_INVALID_SIGNATURE = 40
const ERROR_VALIDATOR_NOT_FOUND = 50
const ERROR_REPLAY_PROTECTION = 52
const ERROR_MESSAGE_EXPIRED = 57
const ERROR_INVALID_BLOCKS_BATCH = 100
const ERROR_NO_SLASHER_CONFIG = 101
const ERROR_NO_PREV_BLOCK_ID = 102

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
    blocksBatchSize: uint8
}

//
// === Getters ===
//
get fun is_blocks_batch_valid(batch: cell, mcSeqno: int): bool {
    val params = loadSlasherParams();
    val vset = lazy ValidatorSet.fromCell(blockchain.configParam(PARAM_IDX_CURRENT_VSET)!);
    val validatorCount = min(vset.total, vset.main);
    return validateBlocksBatch(
        batch.beginParse(),
        {
            batchSize: params.blocksBatchSize,
            mcSeqno,
            validatorCount,
        }
    );
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

    val toSign = beginCell().storeSlice(signedBody).endCell();
    val vset = lazy ValidatorSet.fromCell(blockchain.configParam(PARAM_IDX_CURRENT_VSET)!);
    var (validatorCs, validatorFound) = vset.list.uDictGet(16, validatorIdx);
    assert(validatorFound, ERROR_VALIDATOR_NOT_FOUND);
    val validatorPubkey = ValidatorDescr.readPubkeyOnly(validatorCs!);
    assert(isSignatureValid(toSign.hash(), signature, validatorPubkey), ERROR_INVALID_SIGNATURE);

    val validatorCount = min(vset.total, vset.main);

    val batchSize = loadSlasherParams().blocksBatchSize;
    val mcSeqno = blockchain.prevMcSeqno() + 1;
    assert(validateBlocksBatch(
        batch.beginParse(),
        {
            batchSize,
            validatorCount,
            mcSeqno,
        }
    ), ERROR_INVALID_BLOCKS_BATCH);

    var data = Storage.load();
    assert(createdAtMs > (data.updatedAtMs - REPLAY_OFFSET_MS) &&
    createdAtMs <= (blockchain.now() + FUTURE_OFFSET_SEC) * 1000, ERROR_REPLAY_PROTECTION);

    data.updatedAtMs = max(createdAtMs, data.updatedAtMs);
    data.save();

    acceptExternalMessage();
}

struct ValidateBlocksBatchParams {
    batchSize: int
    mcSeqno: int
    validatorCount: int
}

fun validateBlocksBatch(batch: slice, params: ValidateBlocksBatchParams): bool {
    val startSeqno = batch.loadUint(32);
    if (startSeqno + params.batchSize >= params.mcSeqno) {
        // Batch contains blocks that were not produced yet.
        return false;
    }
    if (startSeqno + params.batchSize * 2 < params.mcSeqno) {
        // Batch contains too old blocks.
        return false;
    }

    batch.skipBits(params.batchSize);
    val history = batch.loadRef() as dict;
    if (!batch.isEmpty()) {
        return false;
    }

    var iterNext = -1;
    do {
        val (validatorIdx, cs, found) = history.uDictGetNext(16, iterNext);
        if (found) {
            iterNext = validatorIdx!;

            if (validatorIdx! >= params.validatorCount) {
                return false;
            }

            val (csBits, csRefs) = cs!.remainingBitsAndRefsCount();
            if (csBits != params.batchSize * 2 || csRefs != 0) {
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

fun blockchain.prevMcSeqno(): int {
    val prevBlocks = blockchain.prevMcBlocks();
    assert(prevBlocks != null, ERROR_NO_PREV_BLOCK_ID);
    // Item at index 0 is the latest mc seqno.
    // Inner item at index 2 is a seqno of the block id.
    return prevBlocks.0.2;
}

fun ValidatorDescr.readPubkeyOnly(s: slice): int {
    val tag = s.loadUint(8);
    assert((tag & ~0x20) == VALIDATOR_DESCR_TAG_SIMPLE, ERROR_INVALID_VALIDATOR_DESCR);
    assert(s.loadUint(32) == PUBKEY_TAG_ED25519, ERROR_INVALID_VALIDATOR_DESCR);
    return s.loadUint(256);
}

@pure
fun blockchain.prevMcBlocks(): [BlockId] | null
    asm "PREVMCBLOCKS"

type BlockId = [int, int, int, int, int]
