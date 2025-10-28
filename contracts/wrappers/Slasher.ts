import {Address, beginCell, Builder, Cell, Contract, ContractProvider, Dictionary, Slice} from "@ton/core";
import assert from "assert";


/// Contract state.
export type SlasherData = {
    /// Total punish votes FOR specified validator
    votes: Dictionary<number, number>;
    /// Validators punished for specified master block (as a timestamp)
    punishedValidators: PunishedValidators | null,
};

export function loadSlasherData(cs: Slice): SlasherData {
    return {
        votes: Dictionary.loadDirect(
            Dictionary.Keys.Int(32),
            Dictionary.Values.Int(32),
            cs.loadRef()
        ),
        punishedValidators: loadPunishedValidators(cs)

    };
}

export function storeSlasherData(src: SlasherData): (builder: Builder) => void {
    return (builder: Builder) =>
        builder
            .storeRef(beginCell().storeDictDirect(src.votes).endCell())
            .storeRef(beginCell().store(storePunishedValidators(src.punishedValidators)).endCell())

}

/// Validators punished for specified master block (as a timestamp)
export type PunishedValidators = {
    mc_seqno: number;
    punishedValidators: Dictionary<number, Cell>;
}

export function loadPunishedValidators(cs: Slice): PunishedValidators {

    let mc_seqno = cs.loadInt(32);
    let punishedValidators = cs.loadDict(Dictionary.Keys.Int(32), Dictionary.Values.Cell());
    return {
        mc_seqno,
        punishedValidators
    }
}

export function storePunishedValidators(pv: PunishedValidators | null): (builder: Builder) => void {
    return (builder: Builder) => {
        if (pv != null) {
            builder
                .storeUint(pv.mc_seqno, 32)
                .storeRef(beginCell().storeDictDirect(pv.punishedValidators).endCell())
        }
        return builder;
    }
}

export class Slasher implements Contract {
    constructor(
        readonly address: Address,
        readonly init?: { code: Cell; data: Cell }
    ) {
    }

    static createFromAddress(address: Address) {
        return new Slasher(address);
    }

    async getData(provider: ContractProvider): Promise<SlasherData> {
        const state = await provider.getState();
        assert(state.state.type === "active");
        assert(state.state.data != null);
        return loadSlasherData(Cell.fromBoc(state.state.data)[0].asSlice());
    }
}