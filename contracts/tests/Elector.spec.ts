import { Blockchain, createShardAccount, SandboxContract } from "@ton/sandbox";
import { address, beginCell, Cell, Dictionary, toNano } from "@ton/core";
import { TychoExecutor } from "@tychosdk/emulator";
import { Elector, storeElectorData } from "../wrappers/Elector";
import "@ton/test-utils";
import { compile } from "@ton/blueprint";

const ELECTOR_ADDR = address(
  "-1:3333333333333333333333333333333333333333333333333333333333333333"
);

describe("Elector", () => {
  let code: Cell;
  let executor: TychoExecutor;

  beforeAll(async () => {
    code = await compile("Elector");
    executor = await TychoExecutor.create();
  });

  let blockchain: Blockchain;
  let elector: SandboxContract<Elector>;

  beforeEach(async () => {
    blockchain = await Blockchain.create({
      config: TychoExecutor.defaultConfig,
      executor,
    });

    blockchain.setShardAccount(
      ELECTOR_ADDR,
      createShardAccount({
        address: ELECTOR_ADDR,
        balance: toNano(500),
        code,
        data: beginCell()
          .store(
            storeElectorData({
              currentElection: null,
              credits: Dictionary.empty(),
              pastElections: Dictionary.empty(),
              grams: 0n,
              activeId: 0,
              activeHash: 0n,
            })
          )
          .endCell(),
        workchain: -1,
      })
    );

    elector = blockchain.openContract(Elector.createFromAddress(ELECTOR_ADDR));
  });

  it("should parse data", async () => {
    const data = await elector.getData();
    console.log(data);
  });
});
