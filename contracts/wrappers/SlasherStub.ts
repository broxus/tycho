import {
  Address,
  Builder,
  Cell,
  Contract,
  ContractProvider,
  Slice,
} from "@ton/core";

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

  async getBlocksBatchSize(provider: ContractProvider) {
    const { stack } = await provider.get("get_blocks_batch_size", []);
    return {
      size: stack.readNumber(),
    };
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
