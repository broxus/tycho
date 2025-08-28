import {
  Address,
  BitString,
  Builder,
  Cell,
  Contract,
  ContractProvider,
  Dictionary,
  Slice,
} from "@ton/core";

import { ElectorData, loadElectorData, storeElectorData } from "./Elector";

export const ELECTOR_POA_OP_ADD_ADDRESS = 0x206491de;
export const ELECTOR_POA_OP_REMOVE_ADDRESS = 0x56efd52d;

export const ANSWER_TAG_POA_WHITELIST_UPDATED = 0xbc06677e;

export const STAKE_ERR_NOT_IN_WHITELIST = 100;

/// PoA elector data.
export type ElectorPoAData = ElectorData & {
  /// Whitelist.
  whitelist: Dictionary<bigint, BitString>;
};

export function loadElectorPoAData(cs: Slice): ElectorPoAData {
  return {
    ...loadElectorData(cs),
    whitelist: cs.loadDict(
      Dictionary.Keys.BigUint(256),
      Dictionary.Values.BitString(0)
    ),
  };
}

export function storeElectorPoAData(
  src: ElectorPoAData
): (builder: Builder) => void {
  return (builder) => {
    builder.store(storeElectorData(src)).storeDict(src.whitelist);
  };
}

export class ElectorPoA implements Contract {
  constructor(
    readonly address: Address,
    readonly init?: { code: Cell; data: Cell }
  ) {}

  static createFromAddress(address: Address) {
    return new ElectorPoA(address);
  }

  async getData(provider: ContractProvider): Promise<ElectorPoAData | null> {
    const state = await provider.getState();
    if (state.state.type == "active") {
      if (state.state.data != null) {
        return loadElectorPoAData(Cell.fromBoc(state.state.data)[0].asSlice());
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
