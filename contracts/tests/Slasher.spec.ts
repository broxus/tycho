import assert from "assert";
import { compile } from "@ton/blueprint";
import {
  address,
  beginCell,
  BitString,
  Cell,
  Dictionary,
  toNano,
} from "@ton/core";
import { Blockchain, EmulationError, SmartContract } from "@ton/sandbox";
import {
  getSecureRandomBytes,
  KeyPair,
  keyPairFromSeed,
  sign,
} from "@ton/crypto";
import { TychoExecutor } from "@tychosdk/emulator";
import {
  PARAM_IDX_SLASHER_PARAMS,
  Slasher,
  storeSlasherParams,
  storeSlasherData,
  SLASHER_OP_SEND_BLOCKS_BATCH,
  loadSlasherData,
} from "../wrappers/Slasher";
import {
  bufferToBigInt,
  ConfigParams,
  makeStubValidatorSet,
} from "../wrappers/util";

const SLASHER_ADDR = address(
  "-1:6666666666666666666666666666666666666666666666666666666666666666",
);
const BLOCKS_BATCH_SIZE = 10;

const SAMPLE_BLOCKS_BATCH = Cell.fromBase64(
  "te6ccgEBCAEAMAABCwAAAObYYAECAswFAgIBIAQDAAfRCgDAAAdpRQBgAgEgBwYAB2UFAGAAB/SKAMA=",
);

const FIRST_MC_SEQNO = 241;

describe("Slasher", () => {
  let config: Cell;
  let code: Cell;
  let executor: TychoExecutor;
  let blockchain: Blockchain;
  let slasher: SmartContract;
  let keypair: KeyPair;
  let currentVsetHash: Buffer;

  beforeAll(async () => {
    keypair = await getSecureRandomBytes(32).then(keyPairFromSeed);

    const params = new ConfigParams(TychoExecutor.defaultConfig);
    params.setSignatureModifiers({
      signatureWithId: false,
      signatureDomain: false,
    });
    params.setRaw(
      PARAM_IDX_SLASHER_PARAMS,
      beginCell()
        .store(
          storeSlasherParams({
            address: SLASHER_ADDR.hash,
            blocksBatchSize: BLOCKS_BATCH_SIZE,
          }),
        )
        .endCell(),
    );

    const vset = await makeStubValidatorSet({
      utimeSince: 0,
      utimeUntil: 1 << 30,
      validatorCount: 13,
    });
    vset.validators.set(0, {
      pubkey: bufferToBigInt(keypair.publicKey),
      weight: 1n,
      adnlAddr: null,
    });
    params.setCurrentVset(vset);
    currentVsetHash = params.getCurrentVsetHash()!;

    const fundamentalAddresses = Dictionary.load(
      Dictionary.Keys.Buffer(32),
      Dictionary.Values.BitString(0),
      params.getRaw(31)!,
    );
    fundamentalAddresses.set(SLASHER_ADDR.hash, BitString.EMPTY);
    params.setRaw(31, beginCell().storeDict(fundamentalAddresses).endCell());

    config = params.toCell();

    code = await compile("Slasher", { debugInfo: true });
    executor = await TychoExecutor.create();
  });

  beforeEach(async () => {
    blockchain = await Blockchain.create({
      config,
      executor,
    });

    await blockchain.setShardAccount(SLASHER_ADDR, {
      account: {
        addr: SLASHER_ADDR,
        storage: {
          lastTransLt: 0n,
          balance: { coins: toNano(500) },
          state: {
            type: "active",
            state: {
              code,
              data: beginCell()
                .store(
                  storeSlasherData({
                    currentVsetHash: Buffer.alloc(32),
                    validatorCount: 0,
                    sentBatches: Dictionary.empty(),
                  }),
                )
                .endCell(),
              special: {
                tick: true,
                tock: false,
              },
            },
          },
        },
        storageStats: {
          used: {
            cells: 0n,
            bits: 0n,
          },
          lastPaid: 0,
          duePayment: null,
          storageExtra: null,
        },
      },
      lastTransactionLt: 0n,
      lastTransactionHash: 0n,
    });

    slasher = await blockchain.getContract(SLASHER_ADDR);
  });

  it("should accept valid blocks batch", async () => {
    const { isValid } = await getters(blockchain, slasher).isBlocksBatchValid({
      blocksBatch: SAMPLE_BLOCKS_BATCH,
      mcSeqno: FIRST_MC_SEQNO,
    });
    expect(isValid).toBe(true);
  });

  it("should accept valid messages", async () => {
    const now = 10000000;
    blockchain.now = now;

    const nowMs = now * 1000 + 500;
    const expireAt = ~~(nowMs / 1000) + 60;
    const validatorIdx = 0;

    const bodyToSign = beginCell()
      .storeUint(expireAt, 32)
      .storeUint(SLASHER_OP_SEND_BLOCKS_BATCH, 32)
      .storeBuffer(currentVsetHash, 32)
      .storeUint(validatorIdx, 16)
      .storeRef(SAMPLE_BLOCKS_BATCH)
      .endCell();
    const signature = sign(bodyToSign.hash(), keypair.secretKey);
    const body = beginCell()
      .storeBuffer(signature, 64)
      .storeSlice(bodyToSign.asSlice())
      .endCell();

    // slasher.setVerbosity({
    //   blockchainLogs: true,
    //   debugLogs: true,
    //   vmLogs: "vm_logs_full",
    // });

    blockchain.prevBlocks = {
      lastMcBlocks: [
        {
          workchain: -1,
          shard: 1n << 63n,
          seqno: FIRST_MC_SEQNO,
          rootHash: Buffer.alloc(32),
          fileHash: Buffer.alloc(32),
        },
      ],
      prevKeyBlock: {
        workchain: -1,
        shard: 1n << 63n,
        seqno: 0,
        rootHash: Buffer.alloc(32),
        fileHash: Buffer.alloc(32),
      },
    };

    await slasher.runTickTock("tick");

    // Check slasher state to have an updated vset
    {
      const state = slasher.accountState;
      assert(state?.type === "active");

      const slasherData = loadSlasherData(state.state.data!.asSlice());
      expect(slasherData.currentVsetHash).toEqual(currentVsetHash);
      expect(slasherData.validatorCount).toBe(13);
    }

    const tx = await slasher.receiveMessage({
      info: {
        type: "external-in",
        dest: SLASHER_ADDR,
        importFee: 0n,
      },
      body,
    });
    assert(tx.description.type === "generic");
    expect(tx.description.aborted).toBe(false);

    {
      const state = slasher.accountState;
      assert(state?.type === "active");

      const slasherData = loadSlasherData(state.state.data!.asSlice());
      expect(slasherData.currentVsetHash).toEqual(currentVsetHash);
      expect(slasherData.validatorCount).toBe(13);

      const ourState = slasherData.sentBatches.get(0)!;
      expect(ourState.pubkey).toEqual(keypair.publicKey);

      const firstSeqno = SAMPLE_BLOCKS_BATCH.asSlice().loadUint(32);
      expect(ourState.minSeqno).toBe(firstSeqno + BLOCKS_BATCH_SIZE);
    }

    try {
      await slasher.receiveMessage({
        info: {
          type: "external-in",
          dest: SLASHER_ADDR,
          importFee: 0n,
        },
        body,
      });
      assert(false, "tx must fail");
    } catch (error: any) {
      assert(error instanceof EmulationError);
      expect(error.exitCode).toBe(100);
    }
  });
});

function getters(blockchain: Blockchain, slasher: SmartContract) {
  return blockchain.openContract(Slasher.createFromAddress(slasher.address));
}
