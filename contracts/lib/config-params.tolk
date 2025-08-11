// === Validator set params ===
const PARAM_IDX_VSET_TIMINGS = 15

struct ValidatorSetTimingsConfig {
    electedFor: uint32
    electionsBeginBefore: uint32
    electionsEndBefore: uint32
    stakeHeldFor: uint32
}

fun ValidatorSetTimingsConfig.load(): ValidatorSetTimingsConfig {
    val param = blockchain.configParam(PARAM_IDX_VSET_TIMINGS);
    return ValidatorSetTimingsConfig.fromCell(param!);
}

// === Validator set count params ===
const PARAM_IDX_VSET_COUNT = 16

struct ValidatorSetSizeConfig {
    maxValidators: uint16
    maxMainValidators: uint16
    minValidators: uint16
}

fun ValidatorSetSizeConfig.load(): ValidatorSetSizeConfig {
    val param = blockchain.configParam(PARAM_IDX_VSET_COUNT);
    return ValidatorSetSizeConfig.fromCell(param!);
}

// === Validator set stake params ===
const PARAM_IDX_VSET_STAKE = 17;

struct ValidatorSetStakeConfig {
    minStake: coins
    maxStake: coins
    minTotalStake: coins
    maxStakeFactor: uint32
}

fun ValidatorSetStakeConfig.load(): ValidatorSetStakeConfig {
    val param = blockchain.configParam(PARAM_IDX_VSET_STAKE);
    return ValidatorSetStakeConfig.fromCell(param!);
}

// === Validator set ===
const PARAM_IDX_PREV_VSET = 32
const PARAM_IDX_CURRENT_VSET = 34
const PARAM_IDX_NEXT_VSET = 36

struct (0x12) ValidatorSet {
    utimeSince: uint32
    utimeUntil: uint32
    total: uint16
    main: uint16
    totalWeight: uint64
    /// u16 => ValidatorDescr
    list: dict
}

struct ValidatorDescr {
    pubkey: uint256
    weight: uint64
    adnlAddr: uint256
}

const VALIDATOR_DESCR_TAG_SIMPLE = 0x53
const VALIDATOR_DESCR_TAG_WITH_ADDR = 0x73

fun ValidatorDescr.packToBuilder(self, mutate b: builder) {
    val withAddr = self.adnlAddr != 0;
    b.storeUint(withAddr ? VALIDATOR_DESCR_TAG_WITH_ADDR : VALIDATOR_DESCR_TAG_SIMPLE, 8);
    b.storeUint(PUBKEY_TAG_ED25519, 32);
    b.storeUint(self.pubkey, 256);
    b.storeUint(self.weight, 64);
    if (withAddr) {
        b.storeUint(self.adnlAddr, 256);
    }
}

fun ValidatorDescr.unpackFromSlice(mutate s: slice): ValidatorDescr {
    val tag = s.loadUint(8);
    assert((tag & ~0x20) == VALIDATOR_DESCR_TAG_SIMPLE, ERROR_INVALID_VALIDATOR_DESCR);
    assert(s.loadUint(32) == PUBKEY_TAG_ED25519, ERROR_INVALID_VALIDATOR_DESCR);
    val pubkey = s.loadUint(256);
    val weight = s.loadUint(64);
    val adnlAddr = (tag & 0x20) != 0 ? s.loadUint(256) : 0;
    return ValidatorDescr {
        pubkey,
        weight,
        adnlAddr,
    };
}

const PUBKEY_TAG_ED25519 = 0x8e81278a

// TODO: Move into separate file?
const ERROR_INVALID_VALIDATOR_DESCR = 41
