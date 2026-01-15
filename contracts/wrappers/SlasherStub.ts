import {
  Address,
  Builder,
  Cell,
  Contract,
  ContractProvider,
  Slice,
} from "@ton/core";
import { UnknownTagError } from "./util";

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

export type SlasherStubData = {
  updatedAtMs: bigint;
};

export function loadSlasherStubData(cs: Slice): SlasherStubData {
  return {
    updatedAtMs: cs.loadUintBig(64),
  };
}

export function storeSlasherStubData(
  s: SlasherStubData,
): (builder: Builder) => void {
  return (builder) => {
    builder.storeUint(s.updatedAtMs, 64);
  };
}

export class SlasherStub implements Contract {
  constructor(
    readonly address: Address,
    readonly init?: { code: Cell; data: Cell },
  ) {}

  static createFromAddress(address: Address) {
    return new SlasherStub(address);
  }

  async isBlocksBatchValid(provider: ContractProvider, blocksBatch: Cell) {
    const { stack } = await provider.get("is_blocks_batch_valid", [
      {
        type: "cell",
        cell: blocksBatch,
      },
    ]);
    return {
      isValid: stack.readBoolean(),
    };
  }
}
