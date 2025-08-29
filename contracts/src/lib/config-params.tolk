import "@stdlib/common"
import "@stdlib/tvm-dicts"

const PARAM_KEY_LEN = 32

const PARAM_IDX_ELECTOR_ADDRESS = 1
const PARAM_IDX_MANDATORY_PARAMS = 9
const PARAM_IDX_CRITICAL_PARAMS = 10

const PARAM_IDX_ELECTOR_POA_MANAGER = 101

// === Vote config ===
const PARAM_IDX_VOTE_CONFIG = 11

struct (0x91) ConfigVotingSetup {
    normalParams: Cell<ConfigProposalSetup>
    criticalParams: Cell<ConfigProposalSetup>
}

@inline
fun ConfigVotingSetup.load(): ConfigVotingSetup {
    val param = blockchain.configParam(PARAM_IDX_VOTE_CONFIG)!;
    return ConfigVotingSetup.fromCell(param!);
}

@inline
fun ConfigVotingSetup.getMinWins(self, critical: bool): int {
    var raw = null as Cell<ConfigProposalSetup>?;
    if (critical) {
        raw = self.criticalParams;
    } else {
        raw = self.normalParams;
    }
    var config = lazy raw.load();
    return config.minWins;
}

@inline
fun ConfigVotingSetup.getMaxLosses(self, critical: bool): int {
    var raw = null as Cell<ConfigProposalSetup>?;
    if (critical) {
        raw = self.criticalParams;
    } else {
        raw = self.normalParams;
    }
    var config = lazy raw.load();
    return config.maxLosses;
}

@inline
fun ConfigVotingSetup.get(self, critical: bool): ConfigProposalSetup {
    var raw = null as Cell<ConfigProposalSetup>?;
    if (critical) {
        raw = self.criticalParams;
    } else {
        raw = self.normalParams;
    }
    return ConfigProposalSetup.fromCell(raw);
}

struct (0x36) ConfigProposalSetup {
    minTotalRounds: uint8
    maxTotalRounds: uint8
    minWins: uint8
    maxLosses: uint8
    minStoreSec: uint32
    maxStoreSec: uint32
    bitPrice: uint32
    cellPrice: uint32
}

// === Validator set params ===
const PARAM_IDX_VSET_TIMINGS = 15

struct ValidatorSetTimingsConfig {
    electedFor: uint32
    electionsBeginBefore: uint32
    electionsEndBefore: uint32
    stakeHeldFor: uint32
}

@inline
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

@inline
fun ValidatorSetSizeConfig.load(): ValidatorSetSizeConfig {
    val param = blockchain.configParam(PARAM_IDX_VSET_COUNT);
    return ValidatorSetSizeConfig.fromCell(param!);
}

// === Validator set stake params ===
const PARAM_IDX_VSET_STAKE = 17

struct ValidatorSetStakeConfig {
    minStake: coins
    maxStake: coins
    minTotalStake: coins
    maxStakeFactor: uint32
}

@inline
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

struct CurrentVset {
    vset: Cell<ValidatorSet>
    totalWeight: uint64
    list: dict
}

@inline
fun CurrentVset.load() {
    val vset = blockchain.configParam(PARAM_IDX_CURRENT_VSET)! as Cell<ValidatorSet>;
    var parsed = lazy vset.load();
    return CurrentVset {
        vset,
        totalWeight: parsed.totalWeight,
        list: parsed.list,
    };
}

@inline
fun CurrentVset.getValidatorDescription(idx: int): slice? {
    val param = blockchain.configParam(PARAM_IDX_CURRENT_VSET);
    var vset = lazy ValidatorSet.fromCell(param!);
    val (validator, _) = vset.list.uDictGet(16, idx);
    return validator;
}

const PUBKEY_TAG_ED25519 = 0x8e81278a

// TODO: Move into separate file?
const ERROR_INVALID_VALIDATOR_DESCR = 41
