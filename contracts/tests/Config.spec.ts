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

import { Config, storeConfigData } from "../wrappers/Config";
import { bufferToBigInt } from "../wrappers/util";

const CONFIG_ADDR = address(
  "-1:5555555555555555555555555555555555555555555555555555555555555555"
);

describe("Config", () => {
  let code: Cell;
  let executor: TychoExecutor;
  let masterKey: KeyPair;

  beforeAll(async () => {
    code = await compile("Config");
    executor = await TychoExecutor.create();
    masterKey = await getSecureRandomBytes(32).then(keyPairFromSeed);
  });

  let blockchain: Blockchain;
  let config: SmartContract;

  beforeEach(async () => {
    blockchain = await Blockchain.create({
      config: TychoExecutor.defaultConfig,
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
                TychoExecutor.defaultConfig
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
    expect(configRoot).toEqualCell(TychoExecutor.defaultConfig);
    expect(data.seqno).toEqual(0);
    expect(data.publicKey).toEqual(bufferToBigInt(masterKey.publicKey));
    expect(data.proposals.size).toEqual(0);
  });
});

function getters(
  blockchain: Blockchain,
  config: SmartContract
): SandboxContract<Config> {
  return blockchain.openContract(Config.createFromAddress(config.address));
}
