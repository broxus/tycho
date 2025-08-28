import {
  address,
  Address,
  beginCell,
  Builder,
  Cell,
  Dictionary,
  Message,
  ShardAccount,
  toNano,
} from "@ton/core";
import { KeyPair, keyPairFromSeed } from "@ton/crypto";
import { createShardAccount } from "@ton/sandbox";

import { ELECTOR_OP_NEW_STAKE } from "./Elector";

export class CustomConfig {
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

  setCurrentVset(vset: Cell) {
    this.dict.set(34, vset);
  }

  setNextVset(vset: Cell) {
    this.dict.set(36, vset);
  }

  clearNextVset() {
    this.dict.delete(36);
  }
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
  public keyPair: KeyPair;
  public address: Address;

  public static makeStub(addr: Address | string) {
    if (!(addr instanceof Address)) {
      addr = address(addr);
    }

    let keyPair = keyPairFromSeed(addr.hash);
    return new ValidatorAccount(addr, keyPair);
  }

  private constructor(address: Address, keyPair: KeyPair) {
    this.address = address;
    this.keyPair = keyPair;
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
