import {Cell, Dictionary,} from "@ton/core";

/// Contract state.
export type ConfigData = {
    cfgDict: Cell
    storedSeqno: number,
    publicKey: bigint,
    voteDict: Dictionary<bigint, Cell>
};