import { Blockchain, createShardAccount } from "@ton/sandbox";
import {
  address,
  beginCell,
  Cell,
  Dictionary,
  SendMode,
  toNano,
} from "@ton/core";
import { TychoExecutor } from "@tychosdk/emulator";
import { loadElectorData, storeElectorData } from "../wrappers/Elector";
import { compile } from "@ton/blueprint";
import "@ton/test-utils";

const ELECTOR_ADDR = address(
  "-1:3333333333333333333333333333333333333333333333333333333333333333"
);
const CONFIG_ADDR = address(
  "-1:5555555555555555555555555555555555555555555555555555555555555555"
);

describe("ElectorPoA", () => {
  let oldCode: Cell;
  let newCode: Cell;
  let executor: TychoExecutor;

  beforeAll(async () => {
    oldCode = await compile("Elector");
    newCode = await compile("ElectorPoA");
    executor = await TychoExecutor.create();
  });

  let blockchain: Blockchain;

  beforeEach(async () => {
    blockchain = await Blockchain.create({
      config: TychoExecutor.defaultConfig,
      executor,
    });
  });

  it("should migrate data on code update", async () => {
    blockchain.setShardAccount(
      ELECTOR_ADDR,
      createShardAccount({
        address: ELECTOR_ADDR,
        balance: toNano(500),
        code: oldCode,
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

    const queryId = Date.now();

    const elector = await blockchain.getContract(ELECTOR_ADDR);
    const tx = await elector.receiveMessage({
      info: {
        type: "internal",
        ihrDisabled: true,
        bounce: false,
        bounced: false,
        src: CONFIG_ADDR,
        dest: ELECTOR_ADDR,
        value: {
          coins: toNano(1),
          other: null,
        },
        ihrFee: 0n,
        forwardFee: 0n,
        createdLt: 0n,
        createdAt: 0,
      },
      body: beginCell()
        .storeUint(0x4e436f64, 32)
        .storeUint(queryId, 64)
        .storeUint(0, 32)
        .storeRef(newCode)
        .endCell(),
    });
    expect(Array.isArray(tx.outActions)).toBe(true);

    let updatedCode = undefined;
    let sentCallback = undefined;
    for (const outAction of tx.outActions || []) {
      switch (outAction.type) {
        case "setCode": {
          updatedCode = outAction.newCode;
          break;
        }
        case "sendMsg": {
          sentCallback = outAction.outMsg;
          expect(outAction.mode).toEqual(
            SendMode.CARRY_ALL_REMAINING_INCOMING_VALUE
          );
        }
      }
    }

    expect(updatedCode).toEqualCell(newCode);
    expect(sentCallback?.body).toEqualCell(
      beginCell()
        .storeUint(0xce436f64, 32)
        .storeUint(queryId, 64)
        .storeUint(0x4e436f64, 32)
        .endCell()
    );

    if (elector.accountState?.type != "active") {
      throw new Error("elector must be active");
    }
    const cs = elector.accountState.state.data!.asSlice();
    loadElectorData(cs);

    expect(cs.remainingBits).toEqual(1);

    const whitelist = cs.loadDict(
      Dictionary.Keys.BigUint(256),
      Dictionary.Values.BitString(0)
    );
    expect(whitelist.size).toEqual(0);
  });
});
