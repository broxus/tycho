import "@stdlib/common"
import "@stdlib/lisp-lists"
import "@stdlib/tvm-dicts"
import "interface/slasher"
import "lib/config-params"
import "lib/util"

tolk 1.0

const BLOCKS_TO_TRACK : uint8 = 100

struct Slasher {
    votes: dict
    punishedValidators: Cell<PunishedValidators>?
}

struct PunishedValidators {
    blockSeqno: uint32
    validators: dict
}

struct SignedValidatorInfo {
    validatorId: uint16
    startBlockSeqno: uint64
    validationMask: slice
    validatorSignaturesInfo: dict
}

fun Slasher.load(): Slasher {
    return Slasher.fromCell(contract.getData());
}

fun Slasher.save(self) {
    contract.setData(self.toCell());
}

fun Slasher.getCurrentMcBLock(): uint32 {
    // val prevMcBlocks = getPreviousMcBlocks();
    // debug.print(prevMcBlocks);
    // val prevBlocks = prevMcBlocks.first() as tuple;
    // val seqno = prevBlocks.get<uint32>(3);
    return 100;
}

fun Slasher.processValidatorStatistics(mutate self, body: slice): int {
    val validatorId = body.loadUint(16);
    val startBlockSeqno = body.loadUint(64);
    val validationMask = body.loadBits(BLOCKS_TO_TRACK);
    var validatorSignaturesInfo = body.loadDict();

    val currentBlockSeqno = Slasher.getCurrentMcBLock();

    if (startBlockSeqno + BLOCKS_TO_TRACK > currentBlockSeqno) {
        return SLASHER_INVALID_DATA_ERROR;
    }

    if (body.remainingBitsCount() != 512 || body.remainingRefsCount() != 0) {
        // Invalid signature slice.
        return SLASHER_INVALID_SIGNATURE_ERROR;
    }

    val signature = body.loadBits(512);
    body.assertEnd();

    val signedData = SignedValidatorInfo {
        validatorId,
        startBlockSeqno,
        validationMask,
        validatorSignaturesInfo,
    }.toCell();

    var validatorCs = CurrentVset.getValidatorDescription(validatorId);
    if (validatorCs == null) {
        return SLASHER_UNKNOWN_VALIDATOR_ERROR;
    }

    val description = ValidatorDescr.readFromSlice(mutate validatorCs!);

    if (!isSignatureValid(signedData.hash(), signature, description.pubkey)) {
        return SLASHER_INVALID_SIGNATURE_ERROR;
    }

    var iterNext = maxInt();
    do {
        var (id, cs, found) = validatorSignaturesInfo.uDictGetPrev(16, iterNext);
        var weight: uint16 = 0;
        if (found) {
            iterNext = id!;
            var valMask = validationMask;
            var data = cs!.loadRef().beginParse();
            repeat (BLOCKS_TO_TRACK) {
                val validated = valMask.loadBool();
                if (validated) {
                    val gotValidSignature = data.loadBool();
                    val gotInvalidSignature = data.loadBool();
                    if (gotInvalidSignature || (!gotInvalidSignature && !gotValidSignature)) {
                        weight += 1;
                    }
                }
            }

            var (votes, found) = self.votes.uDictGet(16, validatorId);
            var total = 0;
            if (found) {
                val prevVotes = votes!.loadUint(32);
                total = prevVotes + weight;
            } else {
                total = weight;
            }

            val slice = beginCell().storeInt(weight, 32).endCell().beginParse();
            self.votes.uDictSet(16, validatorId, slice);
        }
    } while (found);

    self.save();
    return 0; //TODO: code
}

fun Slasher.runVoteAggregation(mutate self): cell {
    var iterNext = maxInt();

    var punishedValidators = createEmptyDict();
    var punishedValidatorsList = createEmptyList();

    do {
        var (validatorId, totalVotesCs, found) = self.votes.uDictGetPrev(16, iterNext);
        if (found) {
            iterNext = validatorId!;
            var score = totalVotesCs!.loadUint(32);
            if (score > BLOCKS_TO_TRACK / 2) {
                punishedValidators.uDictSet(16, validatorId!, createEmptySlice()); //todo: fix?
                listPrepend(validatorId!, punishedValidatorsList);
            }
        }
    } while (found);

    val currentBlockSeqno = Slasher.getCurrentMcBLock();

    val pv = PunishedValidators { blockSeqno: currentBlockSeqno, validators: punishedValidators };

    self.punishedValidators = pv.toCell();
    self.save();

    return self.punishedValidators;
}

fun Slasher.clearVotes(mutate self) {
    self.votes = null;
    self.save();
}
