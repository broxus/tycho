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
import { Blockchain, createShardAccount, SmartContract } from "@ton/sandbox";
import {
  getSecureRandomBytes,
  KeyPair,
  keyPairFromSeed,
  sign,
} from "@ton/crypto";
import { TychoExecutor } from "@tychosdk/emulator";
import {
  PARAM_IDX_SLASHER_PARAMS,
  SlasherStub,
  storeSlasherParams,
  storeSlasherStubData,
} from "../wrappers/SlasherStub";
import {
  bufferToBigInt,
  ConfigParams,
  makeStubValidatorSet,
  ValidatorDescrValue,
} from "../wrappers/util";

const SLASHER_ADDR = address(
  "-1:6666666666666666666666666666666666666666666666666666666666666666",
);
const BLOCKS_BATCH_SIZE = 10;

const SAMPLE_BLOCKS_BATCH = Cell.fromBase64(
  "te6ccgEBCAEAMAABCwAAAObYYAECAswFAgIBIAQDAAfRCgDAAAdpRQBgAgEgBwYAB2UFAGAAB/SKAMA=",
);

describe("Slasher", () => {
  let config: Cell;
  let code: Cell;
  let executor: TychoExecutor;
  let blockchain: Blockchain;
  let slasher: SmartContract;
  let keypair: KeyPair;

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

    const fundamentalAddresses = Dictionary.load(
      Dictionary.Keys.Buffer(32),
      Dictionary.Values.BitString(0),
      params.getRaw(31)!,
    );
    fundamentalAddresses.set(SLASHER_ADDR.hash, BitString.EMPTY);
    params.setRaw(31, beginCell().storeDict(fundamentalAddresses).endCell());

    config = params.toCell();

    code = await compile("SlasherStub", { debugInfo: true });
    executor = await TychoExecutor.create();
  });

  beforeEach(async () => {
    blockchain = await Blockchain.create({
      config,
      executor,
    });

    await blockchain.setShardAccount(
      SLASHER_ADDR,
      createShardAccount({
        address: SLASHER_ADDR,
        balance: toNano(500),
        code,
        data: beginCell()
          .store(
            storeSlasherStubData({
              updatedAtMs: 0n,
            }),
          )
          .endCell(),
        workchain: -1,
      }),
    );

    slasher = await blockchain.getContract(SLASHER_ADDR);
  });

  it("should accept valid blocks batch", async () => {
    const { isValid } = await getters(blockchain, slasher).isBlocksBatchValid({
      blocksBatch: SAMPLE_BLOCKS_BATCH,
      mcSeqno: 241,
    });
    expect(isValid).toBe(true);
  });

  it("should accept valid messages", async () => {
    const now = 10000000;
    blockchain.now = now;

    const nowMs = now * 1000 + 500;
    const expireAt = ~~(nowMs / 1000) + 60;

    const bodyToSign = beginCell()
      .storeUint(nowMs, 64)
      .storeUint(expireAt, 32)
      .storeUint(0, 16)
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
          seqno: 241,
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
  });
});

function getters(blockchain: Blockchain, slasher: SmartContract) {
  return blockchain.openContract(
    SlasherStub.createFromAddress(slasher.address),
  );
}
