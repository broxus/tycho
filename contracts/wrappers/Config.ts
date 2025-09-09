import {
  Address,
  beginCell,
  Builder,
  Cell,
  Contract,
  ContractProvider,
  Dictionary,
  Slice,
  TupleReader,
} from "@ton/core";
import assert from "assert";
import { ConfigParams, UnknownTagError } from "./util";

export const CONFIG_OP_SET_NEXT_VALIDATOR_SET = 0x4e565354;

/// Contract state.
export type ConfigData = {
  /// Root of the config params dictionary.
  configRoot: Dictionary<number, Cell>;
  /// Replay protection seqno.
  seqno: number;
  /// Master public key.
  publicKey: bigint;
  /// Votes for config update proposals.
  ///
  /// uint256 (hash) => `ConfigProposal`
  proposals: Dictionary<bigint, ConfigProposal>;
};

export function loadConfigData(cs: Slice): ConfigData {
  return {
    configRoot: Dictionary.loadDirect(
      Dictionary.Keys.Int(32),
      Dictionary.Values.Cell(),
      cs.loadRef()
    ),
    seqno: cs.loadUint(32),
    publicKey: cs.loadUintBig(256),
    proposals: cs.loadDict(Dictionary.Keys.BigUint(256), {
      parse: loadConfigProposal,
      serialize: storeConfigProposal,
    }),
  };
}

export function storeConfigData(src: ConfigData): (builder: Builder) => void {
  return (builder: Builder) =>
    builder
      .storeRef(beginCell().storeDictDirect(src.configRoot).endCell())
      .storeUint(src.seqno, 32)
      .storeUint(src.publicKey, 256)
      .storeDict(src.proposals);
}

/// Config proposal state.
export type ConfigProposal = {
  /// Unix timestamp when the proposal expires.
  expireAt: number;
  /// Info about the updated config parameter.
  content: ConfigProposalContent;
  /// Whether updated parameter was considered critical at the
  /// time of proposal creation.
  isCritical: boolean;
  /// Indices of participants who has voted.
  ///
  /// uint16 (vset index) => `ProposalVote`
  voters: Dictionary<number, ConfigProposalVote>;
  /// Remaining validators weight required for proposal to be accepted.
  weightRemaining: bigint;
  /// Id of the validator set which participants can vote.
  vsetId: bigint;
  /// For how many validator rounds this proposal is valid.
  roundsRemaining: number;
  /// How many times the quorum was achieved for this proposal.
  wins: number;
  /// How many rounds were skipped without a quorum.
  losses: number;
};

const CONFIG_PROPOSAL_TAG = 0xce;

export function loadConfigProposal(cs: Slice): ConfigProposal {
  const tag = cs.loadUint(8);
  if (tag != CONFIG_PROPOSAL_TAG) {
    throw new UnknownTagError({ tag, bits: 8 });
  }

  const expireAt = cs.loadUint(32);
  const contentCs = cs.loadRef().asSlice();
  const content = loadConfigProposalContent(contentCs);
  contentCs.endParse();
  return {
    expireAt,
    content,
    isCritical: cs.loadBoolean(),
    voters: cs.loadDict(Dictionary.Keys.Uint(16), {
      parse: loadConfigProposalVote,
      serialize: storeConfigProposalVote,
    }),
    weightRemaining: cs.loadIntBig(64),
    vsetId: cs.loadUintBig(256),
    roundsRemaining: cs.loadUint(8),
    wins: cs.loadUint(8),
    losses: cs.loadUint(8),
  };
}

export function storeConfigProposal(
  src: ConfigProposal
): (builder: Builder) => void {
  return (builder: Builder) => {
    builder
      .storeUint(CONFIG_PROPOSAL_TAG, 8)
      .storeUint(src.expireAt, 32)
      .storeRef(
        beginCell().store(storeConfigProposalContent(src.content)).endCell()
      )
      .storeBit(src.isCritical)
      .storeDict(src.voters)
      .storeInt(src.weightRemaining, 64)
      .storeUint(src.vsetId, 256)
      .storeUint(src.roundsRemaining, 8)
      .storeUint(src.wins, 8)
      .storeUint(src.losses, 8);
  };
}

/// Config proposal content to apply.
export type ConfigProposalContent = {
  /// Config param index.
  paramIdx: number;
  /// Config param value.
  ///
  /// `null` to remove value.
  paramValue: Cell | null;
  /// Expected hash of the previous value.
  /// Used as a "replay protection" for changes.
  prevParamValueHash: bigint | null;
};

const CONFIG_PROPOSAL_CONTENT_TAG = 0xf3;

export function loadConfigProposalContent(cs: Slice): ConfigProposalContent {
  const tag = cs.loadUint(8);
  if (tag != CONFIG_PROPOSAL_CONTENT_TAG) {
    throw new UnknownTagError({ tag, bits: 8 });
  }

  return {
    paramIdx: cs.loadInt(32),
    paramValue: cs.loadMaybeRef(),
    prevParamValueHash: cs.loadMaybeUintBig(256),
  };
}

export function storeConfigProposalContent(
  src: ConfigProposalContent
): (builder: Builder) => void {
  return (builder: Builder) =>
    builder
      .storeUint(CONFIG_PROPOSAL_CONTENT_TAG, 8)
      .storeInt(src.paramIdx, 32)
      .storeMaybeRef(src.paramValue)
      .storeMaybeUint(src.prevParamValueHash, 256);
}

/// Proposal votes dictionary entry.
export type ConfigProposalVote = {
  /// Unix timestamp when the vote was accepted.
  createdAt: number;
};

export function loadConfigProposalVote(cs: Slice): ConfigProposalVote {
  return {
    createdAt: cs.loadUint(32),
  };
}

export function storeConfigProposalVote(
  src: ConfigProposalVote
): (builder: Builder) => void {
  return (builder) => builder.storeUint(src.createdAt, 32);
}

export class Config implements Contract {
  constructor(
    readonly address: Address,
    readonly init?: { code: Cell; data: Cell }
  ) {}

  static createFromAddress(address: Address) {
    return new Config(address);
  }

  async getData(provider: ContractProvider): Promise<ConfigData> {
    const state = await provider.getState();
    assert(state.state.type === "active");
    assert(state.state.data != null);
    return loadConfigData(Cell.fromBoc(state.state.data)[0].asSlice());
  }

  async getParams(provider: ContractProvider): Promise<ConfigParams> {
    const state = await provider.getState();
    assert(state.state.type === "active");
    assert(state.state.data != null);
    let dictCell = Cell.fromBoc(state.state.data)[0].asSlice().loadRef();
    return new ConfigParams(dictCell);
  }

  async getParamsRaw(
    provider: ContractProvider
  ): Promise<Dictionary<number, Cell>> {
    const state = await provider.getState();
    assert(state.state.type === "active");
    assert(state.state.data != null);
    return Cell.fromBoc(state.state.data)[0]
      .asSlice()
      .loadRef()
      .asSlice()
      .loadDictDirect(Dictionary.Keys.Int(32), Dictionary.Values.Cell());
  }

  async getParamRaw(
    provider: ContractProvider,
    param: number
  ): Promise<Cell | undefined> {
    const params = await this.getParamsRaw(provider);
    return params.get(param);
  }

  async getCodeHash(provider: ContractProvider): Promise<Buffer | null> {
    const state = await provider.getState();
    if (state.state.type == "active" && state.state.code != null) {
      return Cell.fromBoc(state.state.code)[0].hash();
    }

    return null;
  }

  async seqno(provider: ContractProvider) {
    const { stack } = await provider.get("seqno", []);
    return {
      seqno: stack.readNumber(),
    };
  }

  async getProposal(provider: ContractProvider, proposalHash: bigint | Buffer) {
    if (typeof proposalHash != "bigint") {
      proposalHash = BigInt(`0x${proposalHash.toString("hex")}`);
    }

    const { stack } = await provider.get("get_proposal", [
      { type: "int", value: proposalHash },
    ]);
    const res = stack.readTupleOpt();
    if (res == null) {
      return null;
    }
    return readProposal(res);
  }

  async getProposals(provider: ContractProvider) {
    const { stack } = await provider.get("list_proposals", []);

    const proposals = new Map();
    for (let value of stack.readLispList()) {
      assert(value.type === "tuple");
      let [proposalHash, info] = value.items;
      assert(proposalHash.type === "int");
      assert(info.type === "tuple");
      proposals.set(
        proposalHash.value,
        readProposal(new TupleReader(info.items))
      );
    }

    return proposals;
  }

  async proposalStoragePrice(
    provider: ContractProvider,
    args: {
      critical: boolean;
      seconds: number;
      bits: bigint;
      refs: bigint;
    }
  ) {
    const { stack } = await provider.get("proposal_storage_price", [
      { type: "int", value: BigInt(args.critical ? -1 : 0) },
      { type: "int", value: BigInt(args.seconds) },
      { type: "int", value: args.bits },
      { type: "int", value: args.refs },
    ]);
    return {
      price: stack.readBigNumber(),
    };
  }
}

export type ParsedConfigProposal = {
  expireAt: number;
  isCritical: boolean;
  content: ConfigProposalContent;
  vsetId: bigint;
  voters: Set<number>;
  weightRemaining: bigint;
  roundsRemaining: number;
  losses: number;
  wins: number;
};

function readProposal(reader: TupleReader): ParsedConfigProposal {
  return {
    expireAt: reader.readNumber(),
    isCritical: reader.readBoolean(),
    content: {
      paramIdx: reader.readNumber(),
      paramValue: reader.readCellOpt(),
      prevParamValueHash: reader.readBigNumberOpt(),
    },
    vsetId: reader.readBigNumber(),
    voters: new Set(
      reader.readLispList().map((item) => {
        assert(item.type === "int");
        return Number(item.value);
      })
    ),
    weightRemaining: reader.readBigNumber(),
    roundsRemaining: reader.readNumber(),
    losses: reader.readNumber(),
    wins: reader.readNumber(),
  };
}
