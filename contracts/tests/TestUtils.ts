import {Address, beginCell, Builder, Cell, Dictionary, Message} from "@ton/core";
import {cryptoWithSignatureId} from "@tychosdk/emulator";
import {KeyPair} from "@ton/crypto";

const crypto = cryptoWithSignatureId(2000);
const {getSecureRandomBytes, keyPairFromSeed, sign} = crypto;

export function loadConfigDict(configCellOrBase64: string | Cell) {
    return (typeof configCellOrBase64 === 'string' ? Cell.fromBase64(configCellOrBase64) : configCellOrBase64)
        .beginParse()
        .loadDictDirect(Dictionary.Keys.Int(32), Dictionary.Values.Cell());
}

export async function randomBigInt256() {
    const bytes = await getSecureRandomBytes(32);
    return bufferToBigInt(bytes);
}

export async function generateRandomKeyPair() {
    const seed: Buffer = await getSecureRandomBytes(32);
    return keyPairFromSeed(seed);
}

export function bufferToCell(buffer: Buffer): Cell {
    return beginCell()
        .storeBuffer(buffer)
        .endCell();
}

export function bufferToBigInt(input: Buffer) {
    return BigInt('0x' + input.toString('hex'))
}

export async function createNewStakeMessage(src: Address, dst: Address, coins: bigint, keyPair: KeyPair, stakeAt: number): Promise<Message> {

    let maxFactor = 0x100000;
    let randomAdnlAddr = await randomBigInt256();

    let addr = bufferToBigInt(src.hash);
    let publicKey = bufferToBigInt(keyPair.publicKey);

    let stake = new Stake(
        stakeAt,
        maxFactor,
        addr,
        randomAdnlAddr
    );

    let signature = stake.sign(keyPair.secretKey);

    let body = new Builder();
    body.storeUint(0x4e73744b, 32);
    body.storeUint(1240, 64); // queryId
    body.storeUint(publicKey, 256)
    body.storeUint(stakeAt, 32);
    body.storeUint(maxFactor, 32);
    body.storeUint(randomAdnlAddr, 256);
    body.storeRef(bufferToCell(signature));


    let bodyCell = body.endCell();

    return {
        info: {
            type: 'internal',
            ihrDisabled: true,
            bounce: false,
            bounced: false,
            src: src,
            dest: dst,
            value: {
                coins: coins,
                other: undefined,
            },
            ihrFee: 0n,
            forwardFee: 0n,
            createdLt: 1000000n,
            createdAt: Math.floor(Date.now() / 1000)
        },
        init: null,
        body: bodyCell
    };
}


export class Stake {
    constructor(
        public stakeAt: number,
        public maxFactor: number,
        public addr: bigint,
        public adnlAddr: bigint
    ) {
    }

    sign(secretKey: Buffer): Buffer {
        let boc = this.toBytes();
        return sign(boc, secretKey);
    }

    toBytes() {
        let builder = new Builder();
        builder.storeUint(0x654c5074, 32);
        builder.storeUint(this.stakeAt, 32);
        builder.storeUint(this.maxFactor, 32);
        builder.storeUint(this.addr, 256);
        builder.storeUint(this.adnlAddr, 256);
        let cell = builder.endCell();
        return cell.asSlice().loadBuffer(4 + 4 + 4 + 32 + 32)
    }
}