import { address, beginCell, Cell, Dictionary, toNano } from "@ton/core";
import {
  Blockchain,
  createShardAccount,
  SandboxContract,
  SmartContract,
} from "@ton/sandbox";
import "@ton/test-utils";
import { compile } from "@ton/blueprint";
import { getSecureRandomBytes, KeyPair, keyPairFromSeed } from "@ton/crypto";
import { TychoExecutor } from "@tychosdk/emulator";

import { ELECTOR_OP_CONFIRM_VSET } from "../wrappers/Elector";
import {
  CONFIG_OP_SET_NEXT_VALIDATOR_SET,
  Config,
  storeConfigData,
} from "../wrappers/Config";
import {
  bufferToBigInt,
  ConfigParams,
  simpleInternal,
  makeStubValidatorSet,
  storeValidatorSet,
} from "../wrappers/util";

const ELECTOR_ADDR = address(
  "-1:3333333333333333333333333333333333333333333333333333333333333333"
);
const CONFIG_ADDR = address(
  "-1:5555555555555555555555555555555555555555555555555555555555555555"
);

describe("Config", () => {
  let code: Cell;
  let executor: TychoExecutor;
  let masterKey: KeyPair;

  const params = new ConfigParams(TychoExecutor.defaultConfig);

  beforeAll(async () => {
    code = await compile("Config");
    executor = await TychoExecutor.create();
    masterKey = await getSecureRandomBytes(32).then(keyPairFromSeed);
  });

  let blockchain: Blockchain;
  let config: SmartContract;

  beforeEach(async () => {
    let currentVset = await makeStubValidatorSet({
      utimeSince: 1,
      utimeUntil: 10,
      validatorCount: 10,
    });
    let nextVset = await makeStubValidatorSet({
      utimeSince: 11,
      utimeUntil: 20,
      validatorCount: 10,
    });
    params.setCurrentVset(currentVset);
    params.setNextVset(nextVset);

    blockchain = await Blockchain.create({
      config: params.toCell(),
      executor,
    });

    await blockchain.setShardAccount(
      CONFIG_ADDR,
      createShardAccount({
        address: CONFIG_ADDR,
        balance: toNano(500),
        code,
        data: beginCell()
          .store(
            storeConfigData({
              configRoot: Dictionary.loadDirect(
                Dictionary.Keys.Int(32),
                Dictionary.Values.Cell(),
                params.toCell()
              ),
              seqno: 0,
              publicKey: bufferToBigInt(masterKey.publicKey),
              proposals: Dictionary.empty(),
            })
          )
          .endCell(),
      })
    );
    config = await blockchain.getContract(CONFIG_ADDR);
  });

  it("should parse data", async () => {
    const data = await getters(blockchain, config).getData();

    const configRoot = beginCell().storeDictDirect(data.configRoot).endCell();
    expect(configRoot).toEqualCell(params.toCell());
    expect(data.seqno).toEqual(0);
    expect(data.publicKey).toEqual(bufferToBigInt(masterKey.publicKey));
    expect(data.proposals.size).toEqual(0);
  });

  it("should update vset", async () => {
    let customConfig = await getters(blockchain, config).getParams();
    const oldVsetInfo = customConfig.getVsetInfo();
    blockchain.now = 11;
    await config.runTickTock("tick");
    customConfig = await getters(blockchain, config).getParams();
    const newVsetInfo = customConfig.getVsetInfo();

    expect(newVsetInfo.currentHash).toEqual(oldVsetInfo.nextHash);
    expect(newVsetInfo.prevHash).toEqual(oldVsetInfo.currentHash);
    expect(newVsetInfo.nextHash).toBeUndefined();
  });

  it("should set next vset", async () => {
    blockchain.now = 20;

    const newVset = await makeStubValidatorSet({
      utimeSince: 21,
      utimeUntil: 30,
      validatorCount: 10,
    }).then((vset) => beginCell().store(storeValidatorSet(vset)).endCell());

    let transaction = await config.receiveMessage(
      simpleInternal({
        src: ELECTOR_ADDR,
        dest: CONFIG_ADDR,
        bounce: false,
        bounced: false,
        value: toNano(1),
        body: beginCell()
          .storeUint(CONFIG_OP_SET_NEXT_VALIDATOR_SET, 32)
          .storeUint(0, 64)
          .storeRef(newVset)
          .endCell(),
      })
    );
    expect(transaction.outMessages.size).toEqual(1);
    let resultTag = transaction.outMessages
      .get(0)!!
      .body.beginParse()
      .loadUint(32);

    expect(resultTag).toEqual(ELECTOR_OP_CONFIRM_VSET);

    const customConfig = await getters(blockchain, config).getParams();
    let newData = customConfig.getVsetInfo();
    expect(newData.nextHash).toEqual(bufferToBigInt(newVset.hash()));
  });
});

function getters(
  blockchain: Blockchain,
  config: SmartContract
): SandboxContract<Config> {
  return blockchain.openContract(Config.createFromAddress(config.address));
}
