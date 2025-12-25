import { Address, Builder, Cell, Contract, Slice } from "@ton/core";

export type SlasherStubData = {
  updatedAtMs: bigint;
};

export function loadSlasherStubData(cs: Slice): SlasherStubData {
  return {
    updatedAtMs: cs.loadUintBig(64),
  };
}

export function storeSlasherStubData(
  s: SlasherStubData
): (builder: Builder) => void {
  return (builder) => {
    builder.storeUint(s.updatedAtMs, 64);
  };
}

export class SlasherStub implements Contract {
  constructor(
    readonly address: Address,
    readonly init?: { code: Cell; data: Cell }
  ) {}

  static createFromAddress(address: Address) {
    return new SlasherStub(address);
  }
}
