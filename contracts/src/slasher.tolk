import "@stdlib/gas-payments"
import "@stdlib/tvm-dicts"
import "lib/config-params"

const SLASHER_OP_SEND_BLOCKS_BATCH = stringCrc32("op::slasher_send_blocks_batch")
const SLASHER_OP_VOTE = stringCrc32("op::slasher_vote")

const ERROR_INVALID_SIGNATURE = 40
const ERROR_VALIDATOR_NOT_FOUND = 50
const ERROR_MESSAGE_EXPIRED = 57
const ERROR_UNKNOWN_OP = 60
const ERROR_INVALID_BLOCKS_BATCH = 100
const ERROR_NO_SLASHER_CONFIG = 101
const ERROR_NO_PREV_BLOCK_ID = 102

struct Storage {
    currentVsetHash: uint256
    validatorCount: uint16
    /// Per-participant replay protection.
    ///
    /// uint16 (validatorIdx) => `ValidatorState`
    sentBatches: dict,
}

fun Storage.load(): Storage {
    return Storage.fromCell(contract.getData());
}

fun Storage.save(self) {
    contract.setData(self.toCell());
}

struct ValidatorState {
    pubkey: uint256
    minSeqno: uint32
}

/// Create a new builder with this value.
fun ValidatorState.toBuilder(self): builder {
    return beginCell().storeAny(self);
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
    val nextSeqno = validateBlocksBatch(
        batch.beginParse(),
        {
            minSeqno: 0,
            batchSize: params.blocksBatchSize,
            mcSeqno,
            validatorCount,
        }
    );
    return nextSeqno != null;
}

//
// === Logic ===
//
fun onRunTickTock(_isTock: bool) {
    val vsetRaw = blockchain.configParam(PARAM_IDX_CURRENT_VSET)!;
    val data = lazy Storage.load();
    val currentVsetHash = vsetRaw.hash();
    if (data.currentVsetHash == currentVsetHash) {
        return;
    }

    val vset = lazy ValidatorSet.fromCell(vsetRaw);
    val validatorCount = min(vset.total, vset.main);
    val lastValidatorIdx = validatorCount - 1;

    var sentBatches = createEmptyDict();

    var iterNext = -1;
    do {
        val (validatorIdx, cs, found) = vset.list.uDictGetNext(16, iterNext);
        if (found) {
            iterNext = validatorIdx!;
            val state = ValidatorState {
                pubkey: ValidatorDescr.readPubkeyOnly(cs!),
                minSeqno: 0,
            };
            sentBatches.uDictSetBuilder(16, validatorIdx!, state.toBuilder());
        }
    } while (found && validatorIdx! < lastValidatorIdx);

    Storage {
        currentVsetHash,
        validatorCount,
        sentBatches,
    }.save();
}

fun onInternalMessage(_in: InMessage) {
}

fun onExternalMessage(inMsg: slice) {
    val signature = inMsg.loadBits(512);
    val signedBody = inMsg;
    val expireAtSec = inMsg.loadUint(32);

    val op = inMsg.loadUint(32);
    match (op) {
        SLASHER_OP_SEND_BLOCKS_BATCH => {
            handleBlocksBatchMessage(
                signature,
                signedBody,
                expireAtSec,
                inMsg,
            )
        }
        // TODO: Handle `SLASHER_OP_VOTE`
        else => {
            throw(ERROR_UNKNOWN_OP)
        }
    }
}

fun handleBlocksBatchMessage(
    signature: slice,
    signedBody: slice,
    expireAtSec: int,
    rest: slice
) {
    val vsetHash = rest.loadUint(256);
    val validatorIdx = rest.loadUint(16);
    val batch = rest.loadRef();
    rest.assertEnd();
    assert(blockchain.now() <= expireAtSec, ERROR_MESSAGE_EXPIRED);

    val toSign = beginCell().storeSlice(signedBody).endCell();

    var data = Storage.load();
    assert(vsetHash == data.currentVsetHash, ERROR_VALIDATOR_NOT_FOUND);

    val (validatorCs, validatorFound) = data.sentBatches.uDictGet(16, validatorIdx);
    assert(validatorFound, ERROR_VALIDATOR_NOT_FOUND);
    var validatorState = ValidatorState.fromSlice(validatorCs!);
    assert(isSignatureValid(toSign.hash(), signature, validatorState.pubkey), ERROR_INVALID_SIGNATURE);

    val batchSize = loadSlasherParams().blocksBatchSize;
    val mcSeqno = blockchain.prevMcSeqno() + 1;
    val nextSeqno = validateBlocksBatch(
        batch.beginParse(),
        {
            minSeqno: validatorState.minSeqno,
            batchSize,
            validatorCount: data.validatorCount,
            mcSeqno,
        }
    );
    assert(nextSeqno != null, ERROR_INVALID_BLOCKS_BATCH);

    validatorState.minSeqno = nextSeqno;
    data.sentBatches.uDictSetBuilder(16, validatorIdx, validatorState.toBuilder());
    data.save();

    acceptExternalMessage();
}

struct ValidateBlocksBatchParams {
    minSeqno: int
    batchSize: int
    mcSeqno: int
    validatorCount: int
}

fun validateBlocksBatch(batch: slice, params: ValidateBlocksBatchParams): int? {
    val startSeqno = batch.loadUint(32);
    if (
        // Repeated batch.
        startSeqno < params.minSeqno
        // Batch contains blocks that were not produced yet.
        || startSeqno + params.batchSize > params.mcSeqno
        // Batch contains too old blocks.
        || startSeqno + params.batchSize * 2 < params.mcSeqno
    ) {
        return null;
    }

    val nextSeqno = startSeqno + params.batchSize;

    batch.skipBits(params.batchSize);
    val history = batch.loadRef() as dict;
    if (!batch.isEmpty()) {
        return null;
    }

    var iterNext = -1;
    do {
        val (validatorIdx, cs, found) = history.uDictGetNext(16, iterNext);
        if (found) {
            iterNext = validatorIdx!;

            if (validatorIdx! >= params.validatorCount) {
                return null;
            }

            val (csBits, csRefs) = cs!.remainingBitsAndRefsCount();
            if (csBits != params.batchSize * 2 || csRefs != 0) {
                return null;
            }
        }
    } while (found);

    return nextSeqno;
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
