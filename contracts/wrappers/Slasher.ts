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
        votes: Dictionary.load(
            Dictionary.Keys.Int(32),
            Dictionary.Values.Int(32),
            cs
        ),
        punishedValidators: loadPunishedValidators(cs)

    };
}

export function storeSlasherData(src: SlasherData): (builder: Builder) => void {
    return (builder: Builder) =>
        builder
            .storeDict(src.votes)
            .storeMaybeRef(
                src.punishedValidators != null
                    ? beginCell().store(storePunishedValidators(src.punishedValidators))
                    : null
            )

}

/// Validators punished for specified master block (as a timestamp)
export type PunishedValidators = {
    mc_seqno: number;
    punishedValidators: Dictionary<number, Cell>;
}

export function loadPunishedValidators(cs: Slice): PunishedValidators | null {
    if (cs.remainingBits === 0) {
        return null;
    }

    let ref = cs.loadMaybeRef();
    if (ref != null) {
        let mc_seqno = cs.loadInt(32);
        let punishedValidators = cs.loadDict(Dictionary.Keys.Int(32), Dictionary.Values.Cell());
        return {
            mc_seqno,
            punishedValidators
        }
    }

    return null;

}

export function storePunishedValidators(pv: PunishedValidators): (builder: Builder) => void {
    return (builder: Builder) =>
        builder
            .storeUint(pv.mc_seqno, 32)
            .storeDict(pv.punishedValidators)

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