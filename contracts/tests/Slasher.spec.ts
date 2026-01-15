import { compile } from "@ton/blueprint";
import { address, beginCell, Cell, OpenedContract, toNano } from "@ton/core";
import { Blockchain, createShardAccount, SmartContract } from "@ton/sandbox";
import { TychoExecutor } from "@tychosdk/emulator";
import { SlasherStub, storeSlasherStubData } from "../wrappers/SlasherStub";

const SLASHER_ADDR = address(
  "-1:6666666666666666666666666666666666666666666666666666666666666666",
);

describe("Slasher", () => {
  let code: Cell;
  let executor: TychoExecutor;
  let blockchain: Blockchain;
  let slasher: SmartContract;

  beforeAll(async () => {
    code = await compile("SlasherStub", { debugInfo: true });
    executor = await TychoExecutor.create();
  });

  beforeEach(async () => {
    blockchain = await Blockchain.create({
      config: TychoExecutor.defaultConfig,
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
    await blockchain.setVerbosityForAddress(slasher.address, {
      blockchainLogs: true,
      debugLogs: true,
      //   vmLogs: "vm_logs_full",
    });
  });

  it("should return blocks batch size", async () => {
    const { size } = await getters(blockchain, slasher).getBlocksBatchSize();
    expect(size).toBe(10);
  });

  it("should accept valid blocks batch", async () => {
    const { isValid } = await getters(blockchain, slasher).isBlocksBatchValid(
      Cell.fromBase64(
        "te6ccgEBCAEAMAABCwAAAObYYAECAswFAgIBIAQDAAfRCgDAAAdpRQBgAgEgBwYAB2UFAGAAB/SKAMA=",
      ),
    );
    expect(isValid).toBe(true);
  });
});

function getters(blockchain: Blockchain, slasher: SmartContract) {
  return blockchain.openContract(
    SlasherStub.createFromAddress(slasher.address),
  );
}
