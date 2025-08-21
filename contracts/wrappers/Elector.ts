import {
  Address,
  beginCell,
  Builder,
  Cell,
  Contract,
  ContractProvider,
  Dictionary,
  Slice,
} from "@ton/core";

/// Contract state.
export type ElectorData = {
  /// Active election.
  currentElection: Election | null;
  /// Funds available to be recovered.
  ///
  /// uint256 (address) => coins
  credits: Dictionary<bigint, bigint>;
  /// Finished elections.
  ///
  /// uint32 (electAt) => `PastElection`
  pastElections: Dictionary<number, PastElection>;
  /// Tracked bounces balance.
  grams: bigint;
  /// Election id of the current validator set.
  activeId: number;
  /// Hash of the current validator set.
  activeHash: bigint;
};

export function loadElectorData(cs: Slice): ElectorData {
  const currentElection = cs.loadMaybeRef();

  return {
    currentElection:
      currentElection != null ? loadElection(currentElection.asSlice()) : null,
    credits: cs.loadDict(
      Dictionary.Keys.BigUint(256),
      Dictionary.Values.BigVarUint(4)
    ),
    pastElections: cs.loadDict(Dictionary.Keys.Uint(32), {
      serialize: (src, builder) => builder.store(storePastElection(src)),
      parse: (cs) => {
        let res = loadPastElection(cs);
        cs.endParse();
        return res;
      },
    }),
    grams: 0n,
    activeId: 0,
    activeHash: 0n,
  };
}

export function storeElectorData(src: ElectorData): (builder: Builder) => void {
  return (builder) => {
    builder
      .storeMaybeRef(
        src.currentElection != null
          ? beginCell().store(storeElection(src.currentElection))
          : null
      )
      .storeDict(src.credits)
      .storeDict(src.pastElections)
      .storeCoins(src.grams)
      .storeUint(src.activeId, 32)
      .storeUint(src.activeHash, 256);
  };
}

/// Active election state.
export type Election = {
  /// Unix timestamp when election starts.
  ///
  /// Used as an unique election id.
  electAt: number;
  /// Unix timestamp when election finishes.
  electClose: number;
  /// Minimal stake for participants.
  minStake: bigint;
  /// Minimal total stake for election to be considered successful.
  totalStake: bigint;
  /// Participants map.
  ///
  /// uint256 (pubkey) => `ElectionParticipant`
  participants: Dictionary<bigint, ElectionParticipant>;
  /// Whether the election finished without reaching required conditions.
  failed: boolean;
  /// Whether the election finished sucessfully.
  finished: boolean;
};

export function loadElection(cs: Slice): Election {
  return {
    electAt: cs.loadUint(32),
    electClose: cs.loadUint(32),
    minStake: cs.loadCoins(),
    totalStake: cs.loadCoins(),
    participants: cs.loadDict(Dictionary.Keys.BigUint(256), {
      serialize: (src, builder) => builder.store(storeElectionParticipant(src)),
      parse: (cs) => {
        let res = loadElectionParticipant(cs);
        cs.endParse();
        return res;
      },
    }),
    failed: cs.loadBit(),
    finished: cs.loadBit(),
  };
}

export function storeElection(src: Election): (builder: Builder) => void {
  return (builder) =>
    builder
      .storeUint(src.electAt, 32)
      .storeUint(src.electClose, 32)
      .storeCoins(src.minStake)
      .storeCoins(src.totalStake)
      .storeDict(src.participants)
      .storeBit(src.failed)
      .storeBit(src.finished);
}

/// Active election participant info.
export type ElectionParticipant = {
  /// Uncut stake.
  stake: bigint;
  /// Unix timestamp when this entry was registered.
  time: number;
  /// Max allowed factor for stake (encoded with 16-bit fixed point).
  maxFactor: number;
  /// Address (in masterchain) of the validator.
  addr: bigint;
  /// ADNL address of the validator.
  ///
  /// NOTE: Same as pubkey for Tycho-based networks.
  adnlAddr: bigint;
};

export function loadElectionParticipant(cs: Slice): ElectionParticipant {
  return {
    stake: cs.loadCoins(),
    time: cs.loadUint(32),
    maxFactor: cs.loadUint(32),
    addr: cs.loadUintBig(256),
    adnlAddr: cs.loadUintBig(256),
  };
}

export function storeElectionParticipant(
  src: ElectionParticipant
): (builder: Builder) => void {
  return (builder) =>
    builder
      .storeCoins(src.stake)
      .storeUint(src.time, 32)
      .storeUint(src.maxFactor, 32)
      .storeUint(src.addr, 256)
      .storeUint(src.adnlAddr, 256);
}

/// Finished election state.
export type PastElection = {
  /// Unix timestamp when participant stakes are unfrozen.
  unfreezeAt: number;
  /// A period in seconds for which validators' stakes are frozen.
  stakeHeld: number;
  /// Hash of the serialized validator set.
  vsetHash: bigint;
  // Frozen stakes.
  frozenDict: Dictionary<bigint, FrozenStake>;
  /// The sum of all stakes.
  totalStake: bigint;
  /// Accumulated fees for this validation round.
  bonuses: bigint;
  /// Unused for now
  complaints: Cell | null;
};

export function loadPastElection(cs: Slice): PastElection {
  return {
    unfreezeAt: cs.loadUint(32),
    stakeHeld: cs.loadUint(32),
    vsetHash: cs.loadUintBig(256),
    frozenDict: cs.loadDict(Dictionary.Keys.BigUint(256), {
      serialize: (src, builder) => builder.store(storeFrozenStake(src)),
      parse: (cs) => {
        let res = loadFrozenStake(cs);
        cs.endParse();
        return res;
      },
    }),
    totalStake: cs.loadCoins(),
    bonuses: cs.loadCoins(),
    complaints: cs.loadMaybeRef(),
  };
}

export function storePastElection(
  src: PastElection
): (builder: Builder) => void {
  return (builder) => {
    builder
      .storeUint(src.unfreezeAt, 32)
      .storeUint(src.stakeHeld, 32)
      .storeUint(src.vsetHash, 256)
      .storeDict(src.frozenDict)
      .storeCoins(src.totalStake)
      .storeCoins(src.bonuses)
      .storeMaybeRef(src.complaints);
  };
}

/// Description of frozen validator stake.
export type FrozenStake = {
  /// Address (in masterchain) of the validator.
  addr: bigint;
  /// Validator's weight in vset.
  weight: bigint;
  /// Frozen amount.
  stake: bigint;
  /// Whether validator was banned.
  ///
  /// NOTE: will be used by slashing, maybe should have two bits
  ///       (only disallow bonuses / full stake seizure).
  banned: boolean;
};

export function loadFrozenStake(cs: Slice): FrozenStake {
  return {
    addr: cs.loadUintBig(256),
    weight: cs.loadUintBig(64),
    stake: cs.loadCoins(),
    banned: cs.loadBit(),
  };
}

export function storeFrozenStake(src: FrozenStake): (builder: Builder) => void {
  return (builder) => {
    builder
      .storeUint(src.addr, 256)
      .storeUint(src.weight, 64)
      .storeCoins(src.stake)
      .storeBit(src.banned);
  };
}

export class Elector implements Contract {
  constructor(
    readonly address: Address,
    readonly init?: { code: Cell; data: Cell }
  ) {}

  static createFromAddress(address: Address) {
    return new Elector(address);
  }

  async getData(provider: ContractProvider): Promise<ElectorData | null> {
    const state = await provider.getState();
    if (state.state.type == "active") {
      if (state.state.data != null) {
        return loadElectorData(Cell.fromBoc(state.state.data)[0].asSlice());
      }
    }

    return null;
  }

  async getActiveElectionId(provider: ContractProvider) {
    const { stack } = await provider.get("active_election_id", []);
    return {
      electionId: stack.readBigNumber(),
    };
  }

  async getStake(provider: ContractProvider, address: Address) {
    const { stack } = await provider.get("compute_returned_stake", [
      { type: "int", value: BigInt("0x" + address.hash.toString("hex")) },
    ]);
    return {
      value: stack.readBigNumber(),
    };
  }
}
