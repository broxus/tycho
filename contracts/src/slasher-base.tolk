import "@stdlib/common"
import "@stdlib/tvm-dicts"
import "@stdlib/lisp-lists"

import "interface/slasher"

import "lib/config-params"
import "lib/util"


tolk 1.0

const BLOCKS_TO_TRACK: uint8 = 100;


struct Slasher {
    votes: dict;
    punishedValidators: Cell<PunishedValidators>?;

}

struct PunishedValidators {
    blockSeqno: uint32,
    validators: dict,
}

struct SignedValidatorInfo {
    validatorId: uint32,
    startBlockSeqno: uint64,
    validationMask: slice,
    validatorSignatureInfo: slice
}


fun Slasher.load(): Slasher {
    return Slasher.fromCell(contract.getData())
}

fun Slasher.save(self) {
    contract.setData(self.toCell());
}

fun Slasher.getCurrentMcBLock(): uint32 {
    val prevMcBlocks = getPreviousMcBlocks();
    val prevBlocks = prevMcBlocks.first() as tuple;
    val seqno = prevBlocks.get<uint32>(3);
    return seqno;
}

fun Slasher.processValidatorStatistics(mutate self, body: slice): int {
    val validatorId = body.loadUint(32);
    val startBlockSeqno = body.loadUint(64);
    var validationMask = body.loadBits(BLOCKS_TO_TRACK);
    var validatorSignatureInfo = body.loadBits(BLOCKS_TO_TRACK * 2);

    val currentBlockSeqno = Slasher.getCurrentMcBLock();


    if (startBlockSeqno + BLOCKS_TO_TRACK > currentBlockSeqno) {
        return SLASHER_INVALID_DATA_ERROR;
    }



    val signature = body.loadRef().beginParse();

    if (signature.remainingBitsCount() != 512 || signature.remainingRefsCount() != 0) {
        // Invalid signature slice.
        return SLASHER_INVALID_SIGNATURE_ERROR;
    }

    body.assertEnd();


    val signedData = SignedValidatorInfo {
        validatorId,
        startBlockSeqno,
        validationMask,
        validatorSignatureInfo

    }.toCell();

    var validator = CurrentVset.getValidatorDescription(validatorId);
    if (validator == null) {
        return SLASHER_UNKNOWN_VALIDATOR_ERROR;
    }
    val description = ValidatorDescr.readFromSlice(mutate validator!);

    if (!isSliceSignatureValid(signedData.beginParse(), signature, description.pubkey)) {
        return SLASHER_INVALID_SIGNATURE_ERROR;
    }


    var weight: uint16 = 0;

    repeat (BLOCKS_TO_TRACK) {
        val validated = validationMask.loadBool();


        if (validated) {
            val gotValidSignature = validatorSignatureInfo.loadBool();
            val gotInvalidSignature = validatorSignatureInfo.loadBool();
            if (gotInvalidSignature || (!gotInvalidSignature && !gotValidSignature)) {
                weight += 1;
            }
        }
    }

    var (votes, found) = self.votes.uDictGet(32, validatorId);
    if (found && weight > 0) {
        val prevVotes = votes!.loadUint(16);
        val total = prevVotes + weight;
        self.votes.uDictSet(32, validatorId, total.toCell().beginParse());
    }

    self.save();
    return 0; //todo: code
}


fun Slasher.runVoteAggregation(mutate self): tuple {
    var iterNext = -1;

    var punishedValidators = createEmptyDict();
    var punishedValidatorsList = createEmptyList();

    do {
        var (validatorId, totalVotesCs, found) = self.votes.uDictGetNext(32, iterNext);
        if (found) {
            iterNext = validatorId!;
            var score = totalVotesCs!.loadUint(32);
            if (score > BLOCKS_TO_TRACK / 2) {
                punishedValidators.uDictSet(32, validatorId!, createEmptyCell().beginParse()); //todo: fix?
                punishedValidatorsList.push(validatorId!);
            }
        }
    } while (found);

    val currentBlockSeqno = Slasher.getCurrentMcBLock();

    val pv = PunishedValidators {
        blockSeqno: currentBlockSeqno,
        validators: punishedValidators
    };

    self.punishedValidators = pv.toCell();
    self.save();

    return punishedValidatorsList;
}

fun Slasher.clearVotes(mutate self) {
    self.votes = null;
    self.save();
}
