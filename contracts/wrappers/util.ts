import {address, Address, beginCell, Builder, Cell, Dictionary, Message, Slice, toNano,} from "@ton/core";
import {getSecureRandomBytes, KeyPair, keyPairFromSeed} from "@ton/crypto";

import {ELECTOR_OP_NEW_STAKE} from "./Elector";

export class ConfigParams {
    dict: Dictionary<number, Cell>;

    constructor(root: Cell | string) {
        this.dict = (typeof root === "string" ? Cell.fromBase64(root) : root)
            .beginParse()
            .loadDictDirect(Dictionary.Keys.Int(32), Dictionary.Values.Cell());
    }

    toCell(): Cell {
        return beginCell().store(this.store()).endCell();
    }

    store(): (builder: Builder) => void {
        return (builder: Builder) => {
            builder.storeDictDirect(this.dict);
        };
    }

    getVsetTimings(): VsetTimings {
        const cell = this.dict.get(15)!;
        const cs = cell.beginParse();

        return {
            electedFor: cs.loadUint(32),
            electionsBeginBefore: cs.loadUint(32),
            electionsEndBefore: cs.loadUint(32),
            stakeHeldFor: cs.loadUint(32),
        };
    }

    setVsetSize(args: {
        maxValidators: number;
        maxMainValidators: number;
        minValidators: number;
    }) {
        const param = beginCell()
            .storeInt(args.maxValidators, 16)
            .storeInt(args.maxMainValidators, 16)
            .storeInt(args.minValidators, 16)
            .endCell();
        this.dict.set(16, param);
    }

    setVsetStakeConfig(args: {
        minStake: bigint;
        maxStake: bigint;
        minTotalStake: bigint;
        maxStakeFactor?: number;
    }) {
        const param = beginCell()
            .storeCoins(args.minStake)
            .storeCoins(args.maxStake)
            .storeCoins(args.minTotalStake)
            .storeUint(args.maxStakeFactor || 3 << 16, 32)
            .endCell();
        this.dict.set(17, param);
    }

    getGlobalId(): number {
        const cell = this.dict.get(19);
        return cell!.asSlice().loadUint(32);
    }

    getVsetInfo(): VsetInfo {
        const prev = this.dict.get(32);
        const current = this.dict.get(34);
        const next = this.dict.get(36);
        return {
            prevHash: prev ? bufferToBigInt(prev.hash()) : undefined,
            currentHash: current ? bufferToBigInt(current.hash()) : undefined,
            nextHash: next ? bufferToBigInt(next.hash()) : undefined,
        };
    }

    getCurrentVset(): ValidatorSet | null {
        return this.getVsetParam(34);
    }

    setCurrentVset(vset: ValidatorSet | Cell) {
        this.setVsetParam(34, vset);
    }

    getNextVset(): ValidatorSet | null {
        return this.getVsetParam(36);
    }

    setNextVset(vset: ValidatorSet | Cell | null) {
        this.setVsetParam(36, vset);
    }

    private getVsetParam(idx: number): ValidatorSet | null {
        const param = this.dict.get(idx);
        if (param == null) {
            return null;
        }
        return loadValidatorSet(param.asSlice());
    }

    private setVsetParam(idx: number, vset: ValidatorSet | Cell | null) {
        if (vset == null) {
            this.dict.delete(idx);
            return;
        }
        if (!(vset instanceof Cell)) {
            vset = beginCell().store(storeValidatorSet(vset)).endCell();
        }
        this.dict.set(idx, vset);
    }
}

const VALIDATOR_SET_TAG = 0x12;

export type ValidatorSet = {
    utimeSince: number;
    utimeUntil: number;
    total: number,
    main: number;
    totalWeight: bigint;
    validators: Dictionary<number, ValidatorDescr>;
};

export async function makeStubValidatorSet(args: {
    utimeSince: number;
    utimeUntil: number;
    validatorCount: number;
}): Promise<ValidatorSet> {
    let validators = Dictionary.empty(Dictionary.Keys.Uint(16), {
        parse: loadValidatorDescr,
        serialize: uncurry(storeValidatorDescr),
    });

    for (let i = 0; i < args.validatorCount; i++) {
        validators = validators.set(i, {
            pubkey: await getSecureRandomBytes(32).then(bufferToBigInt),
            weight: 1n,
            adnlAddr: null,
        });
    }

    return {
        utimeSince: args.utimeSince,
        utimeUntil: args.utimeUntil,
        total: args.validatorCount,
        main: args.validatorCount,
        totalWeight: BigInt(args.validatorCount),
        validators,
    };
}

export function makeValidatorSet(args: {
    utimeSince: number,
    utimeUntil: number,
    accounts: ValidatorAccount[]
}): ValidatorSet {
    let validators = Dictionary.empty(Dictionary.Keys.Uint(16), {
        parse: loadValidatorDescr,
        serialize: uncurry(storeValidatorDescr),
    });

    for (const account of args.accounts) {
        validators = validators.set(account.validatorId, {
            pubkey: bufferToBigInt(account.keyPair.publicKey),
            weight: 1n,
            adnlAddr: null,
        });
    }

    return {
        utimeSince: args.utimeSince,
        utimeUntil: args.utimeUntil,
        total: args.accounts.length,
        main: args.accounts.length,
        totalWeight: BigInt(args.accounts.length),
        validators,
    };
}

export function loadValidatorSet(cs: Slice): ValidatorSet {
    const tag = cs.loadUint(8);
    if (tag != VALIDATOR_SET_TAG) {
        throw new UnknownTagError({tag, bits: 8});
    }

    return {
        utimeSince: cs.loadUint(32),
        utimeUntil: cs.loadUint(32),
        total: cs.loadUint(16),
        main: cs.loadUint(16),
        totalWeight: cs.loadUintBig(64),
        validators: cs.loadDict(Dictionary.Keys.Uint(16), {
            parse: loadValidatorDescr,
            serialize: uncurry(storeValidatorDescr),
        }),
    };
}

export function storeValidatorSet(
    vset: ValidatorSet
): (builder: Builder) => void {
    return (builder: Builder) => {
        builder.storeUint(VALIDATOR_SET_TAG, 8);
        builder.storeUint(vset.utimeSince, 32);
        builder.storeUint(vset.utimeUntil, 32);
        builder.storeUint(vset.validators.size, 16);
        builder.storeUint(vset.main, 16);
        builder.storeUint(vset.totalWeight, 64);
        builder.storeDict(vset.validators);
    };
}

const VALIDATOR_DESCR_TAG_SIMPLE = 0x53;
const VALIDATOR_DESCR_TAG_WITH_ADDR = 0x73;
const PUBKEY_TAG_ED25519 = 0x8e81278a;

export type ValidatorDescr = {
    pubkey: bigint;
    weight: bigint;
    adnlAddr: bigint | null;
};

export function loadValidatorDescr(cs: Slice): ValidatorDescr {
    const tag = cs.loadUint(8);
    switch (tag) {
        case VALIDATOR_DESCR_TAG_SIMPLE:
        case VALIDATOR_DESCR_TAG_WITH_ADDR:
            break;
        default:
            throw new UnknownTagError({tag, bits: 8});
    }

    const keyTag = cs.loadUint(32);
    if (keyTag !== PUBKEY_TAG_ED25519) {
        throw new Error(`Unsupported pubkey tag: ${keyTag}`);
    }

    const withAddr = tag === VALIDATOR_DESCR_TAG_WITH_ADDR;
    return {
        pubkey: cs.loadUintBig(256),
        weight: cs.loadUintBig(64),
        adnlAddr: withAddr ? cs.loadUintBig(256) : null,
    };
}

export function storeValidatorDescr(
    d: ValidatorDescr
): (builder: Builder) => void {
    return (builder: Builder) => {
        builder.storeUint(
            d.adnlAddr != null
                ? VALIDATOR_DESCR_TAG_WITH_ADDR
                : VALIDATOR_DESCR_TAG_SIMPLE,
            8
        );

        builder.storeUint(PUBKEY_TAG_ED25519, 32);
        builder.storeUint(d.pubkey, 256);
        builder.storeUint(d.weight, 64);
        if (d.adnlAddr != null) {
            builder.storeUint(d.adnlAddr!, 256);
        }
    };
}


export type VsetInfo = {
    prevHash: bigint | undefined;
    currentHash: bigint | undefined;
    nextHash: bigint | undefined;
};

export type VsetTimings = {
    electedFor: number;
    electionsBeginBefore: number;
    electionsEndBefore: number;
    stakeHeldFor: number;
};

export function loadConfigDict(configCellOrBase64: string | Cell) {
    return (
        typeof configCellOrBase64 === "string"
            ? Cell.fromBase64(configCellOrBase64)
            : configCellOrBase64
    )
        .beginParse()
        .loadDictDirect(Dictionary.Keys.Int(32), Dictionary.Values.Cell());
}

export class ValidatorAccount {
    public validatorId: number;
    public keyPair: KeyPair;
    public address: Address;

    public static makeStub(addr: Address | string, id: number): ValidatorAccount {
        if (!(addr instanceof Address)) {
            addr = address(addr);
        }

        let keyPair = keyPairFromSeed(addr.hash);
        return new ValidatorAccount(addr, keyPair, id);
    }


    private constructor(address: Address, keyPair: KeyPair, validatorId: number) {
        this.address = address;
        this.keyPair = keyPair;
        this.validatorId = validatorId;
    }

    public createStakeMessage(
        electorAddress: Address,
        args: ParticipateInElections
    ): Message {
        return simpleInternal({
            src: this.address,
            dest: electorAddress,
            bounce: true,
            value: args.stake + toNano(1),
            body: this.createStakePayload(args),
        });
    }

    public createVoteMessage(
        slasherAddress: Address,
        startBlockSeqno: number,
        validators: ValidatorData[],
        crypto: Crypto,
    ): Message {
        let builder = new Builder();
        builder.storeUint(this.validatorId, 16);
        builder.storeUint(startBlockSeqno, 64);
        for (let i = 0; i < 100; i++) {
            builder.storeBit(true);
        }


        let dict = Dictionary.empty(
            Dictionary.Keys.Uint(16),
            Dictionary.Values.Cell()
        );

        for (let validator of validators) {
            let b = new Builder();
            for (let signature of validator.signatureInfo) {
                if (signature.validSignatures) {
                    b.storeBit(true);
                } else {
                    b.storeBit(false);
                }

                if (signature.invalidSignature) {
                    b.storeBit(true);
                } else {
                    b.storeBit(false);
                }
            }

            dict = dict.set(validator.validatorId, b.asCell());
        }

        builder.storeDict(dict);
        let cell = builder.asCell();

        const signature = crypto.sign(cell.hash(), this.keyPair.secretKey);
        builder.storeBuffer(signature);


        return simpleExternal({
            dest: slasherAddress,
            body: builder.asCell()
        })
    }

    public createStakePayload(args: ParticipateInElections): Cell {
        const queryId = args.queryId || Date.now();
        const adnlAddr = this.keyPair.publicKey;
        const maxFactor = args.maxFactor || 3 << 16;

        const toSign = Buffer.alloc(4 + 4 + 4 + 32 + 32);
        toSign.writeUInt32BE(0x654c5074, 0);
        toSign.writeUint32BE(args.stakeAt, 4);
        toSign.writeUint32BE(maxFactor, 8);
        this.address.hash.copy(toSign, 12);
        adnlAddr.copy(toSign, 12 + 32);

        const signature = args.crypto.sign(toSign, this.keyPair.secretKey);

        return beginCell()
            .storeUint(ELECTOR_OP_NEW_STAKE, 32)
            .storeUint(queryId, 64)
            .storeBuffer(this.keyPair.publicKey)
            .storeUint(args.stakeAt, 32)
            .storeUint(maxFactor, 32)
            .storeBuffer(adnlAddr)
            .storeRef(beginCell().storeBuffer(signature).endCell())
            .endCell();
    }
}

export type ValidatorData = {
    validatorId: number,
    signatureInfo: SignatureInfo[]
}

export type SignatureInfo = {
    validSignatures: boolean,
    invalidSignature: boolean,
}


export type ParticipateInElections = {
    stake: bigint;
    stakeAt: number;
    maxFactor?: number;
    queryId?: number;
    crypto: Crypto;
};

export interface Crypto {
    sign(data: Buffer, secretKey: Buffer): Buffer;
}

export function bufferToBigInt(input: Buffer) {
    return BigInt("0x" + input.toString("hex"));
}

export function simpleInternal(m: {
    src: Address;
    dest: Address;
    bounce?: boolean;
    bounced?: boolean;
    value: bigint;
    body: Cell;
}): Message {
    return {
        info: {
            type: "internal",
            ihrDisabled: true,
            bounce: m.bounce || false,
            bounced: m.bounced || false,
            src: m.src,
            dest: m.dest,
            value: {
                coins: m.value,
                other: null,
            },
            ihrFee: 0n,
            forwardFee: 0n,
            createdLt: 0n,
            createdAt: 0,
        },
        body: m.body,
    };
}


export function simpleExternal(m: {
    dest: Address;
    body: Cell;
}): Message {
    return {
        info: {
            type: "external-in",
            dest: m.dest,
            importFee: BigInt("0"),
        },
        body: m.body,
    };
}


export class UnknownTagError extends Error {
    constructor(args: { tag: number; bits: number }) {
        super(
            `unknown tag 0x${args.tag
                .toString(16)
                .padStart(Math.ceil(args.bits / 8), "0")}`
        );
    }
}

export function uncurry<T>(f: (src: T) => (builder: Builder) => void) {
    return (src: T, builder: Builder) => f(src)(builder)
}
