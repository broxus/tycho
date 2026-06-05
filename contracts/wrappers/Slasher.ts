import {
  Address,
  Builder,
  Cell,
  Contract,
  ContractProvider,
  Dictionary,
  DictionaryValue,
  Slice,
} from "@ton/core";
import { UnknownTagError } from "./util";

export const SLASHER_OP_SEND_BLOCKS_BATCH = 0x60e2ac7f;

export const PARAM_IDX_SLASHER_PARAMS = 666;

const SLASHER_PARAMS_TAG = 0x01;
export type SlasherParams = {
  /// Slasher address in the masterchain.
  address: Buffer;
  blocksBatchSize: number;
};

export function loadSlasherParams(cs: Slice): SlasherParams {
  const tag = cs.loadUint(8);
  if (tag != SLASHER_PARAMS_TAG) {
    throw new UnknownTagError({ tag, bits: 8 });
  }
  return {
    address: cs.loadBuffer(32),
    blocksBatchSize: cs.loadUint(8),
  };
}

export function storeSlasherParams(
  s: SlasherParams,
): (builder: Builder) => void {
  return (builder) => {
    builder.storeUint(SLASHER_PARAMS_TAG, 8);
    builder.storeBuffer(s.address, 32);
    builder.storeUint(s.blocksBatchSize, 8);
  };
}

export type SlasherData = {
  currentVsetHash: Buffer;
  validatorCount: number;
  sentBatches: Dictionary<number, SlasherValidatorState>;
};

export function loadSlasherData(cs: Slice): SlasherData {
  return {
    currentVsetHash: cs.loadBuffer(32),
    validatorCount: cs.loadUint(16),
    sentBatches: cs.loadDict(
      Dictionary.Keys.Uint(16),
      SlasherValidatorStateValue,
    ),
  };
}

export function storeSlasherData(s: SlasherData): (builder: Builder) => void {
  return (builder) => {
    builder.storeBuffer(s.currentVsetHash, 32);
    builder.storeUint(s.validatorCount, 16);
    builder.storeDict(s.sentBatches);
  };
}

export const SlasherValidatorStateValue: DictionaryValue<SlasherValidatorState> =
  {
    serialize: (src, builder) => builder.store(storeSlasherValidatorState(src)),
    parse: (cs) => {
      const res = loadSlasherValidatorState(cs);
      cs.endParse();
      return res;
    },
  };

export type SlasherValidatorState = {
  pubkey: Buffer;
  minSeqno: number;
};

export function loadSlasherValidatorState(cs: Slice): SlasherValidatorState {
  return {
    pubkey: cs.loadBuffer(32),
    minSeqno: cs.loadUint(32),
  };
}

export function storeSlasherValidatorState(
  s: SlasherValidatorState,
): (builder: Builder) => void {
  return (builder) => {
    builder.storeBuffer(s.pubkey, 32);
    builder.storeUint(s.minSeqno, 32);
  };
}

export class Slasher implements Contract {
  constructor(
    readonly address: Address,
    readonly init?: { code: Cell; data: Cell },
  ) {}

  static createFromAddress(address: Address) {
    return new Slasher(address);
  }

  async isBlocksBatchValid(
    provider: ContractProvider,
    args: { blocksBatch: Cell; mcSeqno: number },
  ) {
    const { stack } = await provider.get("is_blocks_batch_valid", [
      {
        type: "cell",
        cell: args.blocksBatch,
      },
      {
        type: "int",
        value: BigInt(args.mcSeqno),
      },
    ]);
    return {
      isValid: stack.readBoolean(),
    };
  }
}
