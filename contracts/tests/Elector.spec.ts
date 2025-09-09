import fs from "fs";
import path from "path";
import assert from "assert";
import "@ton/test-utils";
import {
  Address,
  address,
  beginCell,
  Cell,
  Dictionary,
  SendMode,
  toNano,
} from "@ton/core";
import { compile } from "@ton/blueprint";
import {
  Blockchain,
  createShardAccount,
  SandboxContract,
  SmartContract,
} from "@ton/sandbox";
import { TychoExecutor, cryptoWithSignatureId } from "@tychosdk/emulator";

import {
  ELECTOR_OP_UPGRADE_CODE,
  ANSWER_TAG_CODE_ACCEPTED,
  Elector,
  ElectorData,
  ParticipantListExtended,
  storeElectorData,
} from "../wrappers/Elector";
import {
  bufferToBigInt,
  Crypto,
  ConfigParams,
  ValidatorAccount,
  VsetTimings,
  simpleInternal,
} from "../wrappers/util";

const ELECTOR_ADDR = address(
  "-1:3333333333333333333333333333333333333333333333333333333333333333"
);
const CONFIG_ADDR = address(
  "-1:5555555555555555555555555555555555555555555555555555555555555555"
);

describe("Elector", () => {
  let code: Cell;
  let executor: TychoExecutor;
  let globalId: number;
  let crypto: ReturnType<typeof cryptoWithSignatureId>;
  let blockchain: Blockchain;
  let elector: SmartContract;
  let timings: VsetTimings;

  beforeAll(async () => {
    code = await compile("Elector", { debugInfo: true });
    executor = await TychoExecutor.create();
  });

  beforeEach(async () => {
    const config = new ConfigParams(TychoExecutor.defaultConfig);
    config.setVsetSize({
      maxValidators: 1000,
      maxMainValidators: 100,
      minValidators: 3,
    });
    config.setVsetStakeConfig({
      minStake: toNano(10000),
      maxStake: toNano(100000),
      minTotalStake: toNano(20000),
    });
    globalId = config.getGlobalId();
    timings = config.getVsetTimings();
    crypto = cryptoWithSignatureId(globalId);

    blockchain = await Blockchain.create({
      config: config.toCell(),
      executor,
    });

    await blockchain.setShardAccount(
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

    elector = await blockchain.getContract(ELECTOR_ADDR);
    await blockchain.setVerbosityForAddress(elector.address, {
      blockchainLogs: true,
      debugLogs: true,
      //vmLogs: 'vm_logs',
    });
  });

  it("should parse data", async () => {
    const data = await getters(blockchain, elector).getData();
    expect(data).not.toBeNull();
  });

  it("should run full election cycle", async () => {
    const validators = Array(4)
      .fill(null)
      .map((_, i) =>
        ValidatorAccount.makeStub(address(`-1:${`e${i}`.repeat(32)}`))
      );

    const stake = toNano(10000);

    // Start election
    blockchain.now = time(
      timings.electedFor - timings.electionsBeginBefore + 1
    );
    const firstElectionId = await startElection({ blockchain, elector });

    await sendStakes({
      blockchain,
      elector,
      stake,
      validators,
      electionId: firstElectionId,
      count: 3,
      crypto,
    });

    // Shift time to when the first election ends
    blockchain.now = time(timings.electedFor - timings.electionsEndBefore + 1);
    const firstVset = await finishElection({
      blockchain,
      elector,
      electionId: firstElectionId,
    });

    // Simulate config change
    blockchain.now = time(timings.electedFor + 1);
    await syncVset({
      blockchain,
      elector,
      vset: firstVset,
      electionId: firstElectionId,
    });

    // Start next election
    blockchain.now = time(
      2 * timings.electedFor - timings.electionsBeginBefore + 1
    );
    const nextElectionId = await startElection({ blockchain, elector });

    await sendStakes({
      blockchain,
      elector,
      stake,
      validators,
      electionId: nextElectionId,
      count: 3,
      crypto,
    });

    // Shift time to when the next election ends
    blockchain.now = time(
      2 * timings.electedFor - timings.electionsEndBefore + 1
    );
    const nextVset = await finishElection({
      blockchain,
      elector,
      electionId: nextElectionId,
    });

    // Simulate config change
    blockchain.now = time(2 * timings.electedFor + 1);
    await syncVset({
      blockchain,
      elector,
      vset: nextVset,
      electionId: nextElectionId,
    });

    // Start next election
    blockchain.now = time(
      3 * timings.electedFor - timings.electionsBeginBefore + 1
    );
    await startElection({ blockchain, elector });

    // Check unfreeze
    await elector.runTickTock("tock");

    const data = await getters(blockchain, elector).getData();
    assert(data != null);
    expect(data.credits.size).toEqual(3);
    for (const validator of validators.slice(0, 3)) {
      const addr = bufferToBigInt(validator.address.hash);
      expect(data.credits.get(addr)).toEqual(stake);
    }
  });

  it("can be upgraded from old code", async () => {
    const [oldCode] = Cell.fromBoc(
      fs.readFileSync(path.join(__dirname, "../res/old_elector_code.boc"))
    );

    const possibleData: ElectorData[] = [
      // Empty data
      {
        currentElection: null,
        credits: Dictionary.empty(),
        pastElections: Dictionary.empty(),
        grams: 0n,
        activeId: 0,
        activeHash: 0n,
      },
      // Non-empty elector state
      {
        currentElection: {
          electAt: 123,
          electClose: 123123,
          minStake: 345345n,
          totalStake: 234234n,
          failed: true,
          finished: false,
          participants: Dictionary.empty(),
        },
        credits: Dictionary.empty(),
        pastElections: Dictionary.empty(),
        grams: 123123n,
        activeId: 234234,
        activeHash: 456456n,
      },
    ];

    for (const electorData of possibleData) {
      const electorDataCell = beginCell()
        .store(storeElectorData(electorData))
        .endCell();

      await blockchain.setShardAccount(
        ELECTOR_ADDR,
        createShardAccount({
          address: ELECTOR_ADDR,
          balance: toNano(500),
          code: oldCode,
          data: electorDataCell,
          workchain: -1,
        })
      );
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
            .storeRef(code)
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

      expect(updatedCode).toEqualCell(code);
      expect(callback?.body).toEqualCell(
        beginCell()
          .storeUint(ANSWER_TAG_CODE_ACCEPTED, 32)
          .storeUint(queryId, 64)
          .storeUint(ELECTOR_OP_UPGRADE_CODE, 32)
          .endCell()
      );

      assert(elector.accountState?.type === "active");
      expect(elector.accountState.state.data).toEqualCell(electorDataCell);
    }
  });
});

async function startElection(args: {
  blockchain: Blockchain;
  elector: SmartContract;
}): Promise<number> {
  await args.elector.runTickTock("tick");

  const elector = getters(args.blockchain, args.elector);

  const activeElectionId = await checkElectionId(args.blockchain, elector);
  let data = await elector.getData();
  assert(data != null);
  assert(data.currentElection != null);

  const electAt = data.currentElection.electAt;
  expect(electAt).toEqual(activeElectionId);

  return activeElectionId;
}

async function sendStakes(args: {
  blockchain: Blockchain;
  elector: SmartContract;
  stake: bigint;
  validators: ValidatorAccount[];
  electionId: number;
  count: number;
  crypto: Crypto;
}) {
  // Send stakes from validators
  for (const validator of args.validators.slice(0, args.count)) {
    const stakeMessage = validator.createStakeMessage(args.elector.address, {
      stake: args.stake,
      stakeAt: args.electionId,
      crypto: args.crypto,
    });
    await args.elector.receiveMessage(stakeMessage);
  }

  const elector = getters(args.blockchain, args.elector);
  await checkStakeEquals(elector, args.stake * 3n);

  let participants = await elector.getParticipantListExtended();
  for (const validator of args.validators.slice(0, args.count)) {
    expect(hasParticipant(participants, validator.address)).toBeTruthy();
  }
  for (const notValidator of args.validators.slice(args.count)) {
    expect(hasParticipant(participants, notValidator.address)).toBeFalsy();
  }
}

async function finishElection(args: {
  blockchain: Blockchain;
  elector: SmartContract;
  electionId: number;
}): Promise<Cell> {
  let tx = await args.elector.runTickTock("tick");

  const data = await getters(args.blockchain, args.elector).getData();
  assert(data != null);

  // Check whether elections have passed
  let pastElection = data.pastElections.get(args.electionId);
  assert(pastElection != null);

  // Elector must send message to config with updated vset
  expect(tx.outMessages.size).toEqual(1);

  const message = tx.outMessages.get(0)!;
  assert(message.info.type === "internal");
  expect(message.info.dest).toEqualAddress(CONFIG_ADDR);

  const vset = message.body.asSlice().loadRef();
  expect(bufferToBigInt(vset.hash())).toEqual(pastElection.vsetHash);

  return vset;
}

async function syncVset(args: {
  blockchain: Blockchain;
  elector: SmartContract;
  vset: Cell;
  electionId: number;
}) {
  let config = new ConfigParams(args.blockchain.config);
  config.setCurrentVset(args.vset);
  config.setNextVset(null);
  args.blockchain.setConfig(config.toCell());

  await args.elector.runTickTock("tick");
  const data = await getters(args.blockchain, args.elector).getData();
  assert(data != null);
  expect(data.currentElection).toBeNull();
  expect(data.activeId).toEqual(args.electionId);
  expect(data.activeHash).toEqual(bufferToBigInt(args.vset.hash()));
}

async function checkElectionId(
  blockchain: Blockchain,
  elector: SandboxContract<Elector>
): Promise<number> {
  let { electionId } = await elector.getActiveElectionId();
  let timings = new ConfigParams(blockchain.config).getVsetTimings();
  expect(electionId).toEqual(blockchain.now! + timings.electionsBeginBefore);
  return electionId;
}

async function checkStakeEquals(
  elector: SandboxContract<Elector>,
  stake: bigint
) {
  let data = await elector.getData();
  assert(data != null);
  assert(data.currentElection != null);
  expect(data.currentElection.totalStake).toEqual(stake);
}

function hasParticipant(
  participants: ParticipantListExtended,
  address: Address
) {
  const x = bufferToBigInt(address.hash);
  return (
    address.workChain == -1 &&
    participants.participants.find((participant) => participant.address == x)
  );
}

function getters(blockchain: Blockchain, elector: SmartContract) {
  return blockchain.openContract(Elector.createFromAddress(elector.address));
}

function time(offset: number): number {
  return START + offset;
}

const START = 2000000000;
