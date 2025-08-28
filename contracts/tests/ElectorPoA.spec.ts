import assert from "assert";
import "@ton/test-utils";
import {
  address,
  beginCell,
  BitString,
  Cell,
  Dictionary,
  SendMode,
  toNano,
} from "@ton/core";
import { compile } from "@ton/blueprint";
import { Blockchain, createShardAccount } from "@ton/sandbox";
import { TychoExecutor } from "@tychosdk/emulator";

import {
  ELECTOR_OP_NEW_STAKE,
  ELECTOR_OP_UPGRADE_CODE,
  ANSWER_TAG_STAKE_REJECTED,
  ANSWER_TAG_CODE_ACCEPTED,
  ANSWER_TAG_ERROR,
  loadElectorData,
  storeElectorData,
} from "../wrappers/Elector";
import {
  ANSWER_TAG_POA_WHITELIST_UPDATED,
  ELECTOR_POA_OP_ADD_ADDRESS,
  ELECTOR_POA_OP_REMOVE_ADDRESS,
  STAKE_ERR_NOT_IN_WHITELIST,
  loadElectorPoAData,
  storeElectorPoAData,
} from "../wrappers/ElectorPoA";
import { simpleInternal } from "../wrappers/util";

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

    await blockchain.setShardAccount(
      ELECTOR_ADDR,
      createShardAccount({
        address: ELECTOR_ADDR,
        balance: toNano(500),
        code: newCode,
        data: beginCell()
          .store(
            storeElectorPoAData({
              currentElection: null,
              credits: Dictionary.empty(),
              pastElections: Dictionary.empty(),
              grams: 0n,
              activeId: 0,
              activeHash: 0n,
              whitelist: Dictionary.empty(),
            })
          )
          .endCell(),
        workchain: -1,
      })
    );
  });

  it("should allow only manager to update whitelist", async () => {
    const MANAGER_ADDR = address(
      "-1:b0bab0bab0bab0bab0bab0bab0bab0bab0bab0bab0bab0bab0bab0bab0bab0ba"
    );

    // Update config to enable PoA manager
    const params = blockchain.config
      .asSlice()
      .loadDictDirect(Dictionary.Keys.Int(32), Dictionary.Values.Cell());
    params.set(101, beginCell().storeBuffer(MANAGER_ADDR.hash, 32).endCell());
    blockchain.setConfig(beginCell().storeDictDirect(params).endCell());

    //
    const elector = await blockchain.getContract(ELECTOR_ADDR);
    const queryId = Date.now();

    // Try to add/remove validator from an unknown address
    for (const addrStr of [
      // Just a random shardchain address
      "0:cafecafecafecafecafecafecafecafecafecafecafecafecafecafecafecafe",
      // Same hash as manager but in shardchain
      "0:b0bab0bab0bab0bab0bab0bab0bab0bab0bab0bab0bab0bab0bab0bab0bab0ba",
      // Masterchain address but different from the manager
      "-1:cafecafecafecafecafecafecafecafecafecafecafecafecafecafecafecafe",
    ]) {
      const addr = address(addrStr);
      for (const op of [
        ELECTOR_POA_OP_ADD_ADDRESS,
        ELECTOR_POA_OP_REMOVE_ADDRESS,
      ]) {
        let tx = await elector.receiveMessage(
          simpleInternal({
            bounce: true,
            src: addr,
            dest: ELECTOR_ADDR,
            value: toNano(1),
            body: beginCell()
              .storeUint(op, 32)
              .storeUint(queryId, 64)
              .storeUint(123, 256)
              .endCell(),
          })
        );
        expect(tx.outMessagesCount).toEqual(1);

        const callback = tx.outMessages.get(0)!;
        assert(callback.info.type === "internal");
        expect(callback.info.dest).toEqualAddress(addr);
        expect(callback.body).toEqualCell(
          beginCell()
            .storeUint(ANSWER_TAG_ERROR, 32)
            .storeUint(queryId, 64)
            .storeUint(op, 32)
            .endCell()
        );
      }
    }

    // Add/remove address as a manager
    const validator = 123n;
    for (const [op, check] of [
      [
        ELECTOR_POA_OP_ADD_ADDRESS,
        (whitelist: Dictionary<bigint, BitString>) => {
          expect(whitelist.size).toEqual(1);
          expect(whitelist.get(validator)).toBeDefined();
        },
      ] as const,
      [
        ELECTOR_POA_OP_REMOVE_ADDRESS,
        (whitelist: Dictionary<bigint, BitString>) => {
          expect(whitelist.size).toEqual(0);
        },
      ] as const,
    ]) {
      let tx = await elector.receiveMessage(
        simpleInternal({
          bounce: true,
          src: MANAGER_ADDR,
          dest: ELECTOR_ADDR,
          value: toNano(1),
          body: beginCell()
            .storeUint(op, 32)
            .storeUint(queryId, 64)
            .storeUint(validator, 256)
            .endCell(),
        })
      );
      expect(tx.outMessagesCount).toEqual(1);

      const callback = tx.outMessages.get(0)!;
      assert(callback.info.type === "internal");
      expect(callback.info.dest).toEqualAddress(MANAGER_ADDR);
      expect(callback.body).toEqualCell(
        beginCell()
          .storeUint(ANSWER_TAG_POA_WHITELIST_UPDATED, 32)
          .storeUint(queryId, 64)
          .storeUint(op, 32)
          .endCell()
      );

      assert(elector.accountState?.type === "active");
      const data = loadElectorPoAData(
        elector.accountState.state.data!.asSlice()
      );
      check(data.whitelist);
    }
  });

  it("should block accounts not from whitelist", async () => {
    const VALIDATOR_ADDR = address(
      "-1:abababababababababababababababababababababababababababababababab"
    );

    // Reset elector state with a custom whitelist
    const whitelist = Dictionary.empty(
      Dictionary.Keys.BigUint(256),
      Dictionary.Values.BitString(0)
    );
    whitelist.set(123n, BitString.EMPTY);

    await blockchain.setShardAccount(
      ELECTOR_ADDR,
      createShardAccount({
        address: ELECTOR_ADDR,
        balance: toNano(500),
        code: newCode,
        data: beginCell()
          .store(
            storeElectorPoAData({
              currentElection: null,
              credits: Dictionary.empty(),
              pastElections: Dictionary.empty(),
              grams: 0n,
              activeId: 0,
              activeHash: 0n,
              whitelist,
            })
          )
          .endCell(),
        workchain: -1,
      })
    );
    const elector = await blockchain.getContract(ELECTOR_ADDR);

    const queryId = Date.now();
    const tx = await elector.receiveMessage(
      simpleInternal({
        bounce: true,
        src: VALIDATOR_ADDR,
        dest: ELECTOR_ADDR,
        value: toNano(1000),

        body: beginCell()
          .storeUint(ELECTOR_OP_NEW_STAKE, 32)
          .storeUint(queryId, 64)
          .endCell(),
      })
    );
    expect(tx.outMessagesCount).toEqual(1);

    const callback = tx.outMessages.get(0)!;
    assert(callback.info.type === "internal");
    expect(callback.info.dest).toEqualAddress(VALIDATOR_ADDR);

    expect(callback.body).toEqualCell(
      beginCell()
        .storeUint(ANSWER_TAG_STAKE_REJECTED, 32)
        .storeUint(queryId, 64)
        .storeUint(STAKE_ERR_NOT_IN_WHITELIST, 32)
        .endCell()
    );
  });

  it("should migrate data on code update", async () => {
    // Reset elector state with an old contract
    await blockchain.setShardAccount(
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
    const elector = await blockchain.getContract(ELECTOR_ADDR);
    assert(elector.accountState?.type === "active");
    expect(elector.accountState.state.code).toEqualCell(oldCode);

    const queryId = Date.now();
    const tx = await elector.receiveMessage(
      simpleInternal({
        src: CONFIG_ADDR,
        dest: ELECTOR_ADDR,
        value: toNano(1),
        body: beginCell()
          .storeUint(ELECTOR_OP_UPGRADE_CODE, 32)
          .storeUint(queryId, 64)
          .storeUint(0, 32)
          .storeRef(newCode)
          .endCell(),
      })
    );
    expect(Array.isArray(tx.outActions)).toBe(true);

    let updatedCode = undefined;
    let callback = undefined;
    for (const outAction of tx.outActions || []) {
      switch (outAction.type) {
        case "setCode": {
          updatedCode = outAction.newCode;
          break;
        }
        case "sendMsg": {
          callback = outAction.outMsg;
          expect(outAction.mode).toEqual(
            SendMode.CARRY_ALL_REMAINING_INCOMING_VALUE
          );
        }
      }
    }

    expect(updatedCode).toEqualCell(newCode);
    expect(callback?.body).toEqualCell(
      beginCell()
        .storeUint(ANSWER_TAG_CODE_ACCEPTED, 32)
        .storeUint(queryId, 64)
        .storeUint(ELECTOR_OP_UPGRADE_CODE, 32)
        .endCell()
    );

    assert(elector.accountState?.type === "active");
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
