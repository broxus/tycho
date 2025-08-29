import {Blockchain, createShardAccount, SandboxContract} from "@ton/sandbox";
import {Address, address, beginCell, Builder, Cell, Dictionary, Message, toNano} from "@ton/core";
import {cryptoWithSignatureId, TychoExecutor} from "@tychosdk/emulator";
import {Elector, storeElectorData} from "../wrappers/Elector";
import {compile} from "@ton/blueprint";
import {KeyPair} from "@ton/crypto";

import "@ton/test-utils";
import {
    Account,
    createNewStakeMessage,
    generateRandomKeyPair,
    loadConfigDict,
} from "./TestUtils";
import path from "path";
import fs from "fs";


const ELECTOR_ADDR = address(
    "-1:3333333333333333333333333333333333333333333333333333333333333333"
);

const CONFIG_ADDR = address(
    "-1:5555555555555555555555555555555555555555555555555555555555555555"
);

describe("Elector", () => {
    let code: Cell;
    let executor: TychoExecutor;

    const timePivot = 2000000000;

    beforeAll(async () => {
        code = await compile("Elector", {debugInfo: true});
        executor = await TychoExecutor.create();
    });

    let blockchain: Blockchain;
    let elector: SandboxContract<Elector>;

    beforeEach(async () => {

        let updated = setValidatorCount(TychoExecutor.defaultConfig, 3);
        blockchain = await Blockchain.create({
            config: updated,
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


        elector = blockchain.openContract(Elector.createFromAddress(ELECTOR_ADDR));
        await blockchain.setVerbosityForAddress(elector.address, {
            blockchainLogs: true,
            debugLogs: true,
            //vmLogs: 'vm_logs',
        });
    });

    it("should parse data", async () => {
        const data = await elector.getData();
        expect(data).not.toBeNull();
    });

    describe("Validator submission", () => {

        let account1: Account;
        let account2: Account;
        let account3: Account;
        let account4: Account;

        beforeAll(async () => {
            account1 = await Account.generate('-1:c81b5e0e2654a79e7b5bcf0ecf4e395119832ca17f53260bbce113bc78907280', 200000);
            account2 = await Account.generate('-1:c74ff13d493f920d169fb263cfc79fb7b23a17c27908423efe865d8b82aac7aa', 200000);
            account3 = await Account.generate('-1:931c964105787ffcd9f2e2e5b2f0b91904e83505f564a2c7d82df40e71c301b4', 200000);
            account4 = await Account.generate('-1:b7d3c305b0dfd1fd3e1afc1d25a05deb4b9b281fd93e1862da340c2c90f26a88', 200000);

            await blockchain.setShardAccount(
                account1.address,
                account1.shardAccount
            );
            await blockchain.setShardAccount(
                account2.address,
                account2.shardAccount
            );
            await blockchain.setShardAccount(
                account3.address,
                account3.shardAccount
            );
            await blockchain.setShardAccount(
                account4.address,
                account4.shardAccount
            );


        });

        it("should have active election", async () => {
            await runElection(blockchain, elector, timePivot);
        });


        it('should have stake', async () => {
            blockchain.now = timePivot;
            let stakeValue = toNano(10001);
            await runElection(blockchain, elector, timePivot);
            let data = await elector.getData();

            let message1 = await createNewStakeMessage(
                account1.address,
                elector.address,
                stakeValue,
                account1.keyPair,
                data!!.currentElection!!.electAt
            );
            await blockchain.sendMessage(message1);

            let message2 = await createNewStakeMessage(
                account2.address,
                elector.address,
                stakeValue,
                account2.keyPair,
                data!!.currentElection!!.electAt
            );
            await blockchain.sendMessage(message2);

            let message3 = await createNewStakeMessage(
                account3.address,
                elector.address,
                stakeValue,
                account3.keyPair,
                data!!.currentElection!!.electAt
            );
            await blockchain.sendMessage(message3);


            await checkStakeEquals(elector, 10000 * 3);
        });


    })
});


describe("Old elector", () => {
    let oldElectorCode: Cell;
    let newElectorCode: Cell;
    let executor: TychoExecutor;

    const bocPath = path.join(__dirname, '..', 'tests/elector_code.boc');
    const timePivot = 2000000000;

    beforeAll(async () => {
        oldElectorCode = Cell.fromBoc(fs.readFileSync(bocPath))[0];
        newElectorCode = await compile("Elector", {debugInfo: true});
        executor = await TychoExecutor.create();
    });


    let blockchain: Blockchain;
    let elector: SandboxContract<Elector>;

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
                code: oldElectorCode,
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
        await blockchain.setVerbosityForAddress(elector.address, {
            blockchainLogs: true,
            debugLogs: true,
            //vmLogs: 'vm_logs',
        });
    });

    it("should parse data", async () => {
        const data = await elector.getData();
        expect(data).not.toBeNull();
    });


    describe("Elector code change", () => {
        it('should change code', async () => {
            await upgradeCode(blockchain, elector, newElectorCode);
        })
    });

    describe("Elector code change during active election", () => {
        let validator: Account;
        beforeAll(async () => {
            validator = await Account.generate("-1:9f6723605fab64ef5da7105943b26092ce0858ccbdffa10e77676f2a87674421", 20000);
            await blockchain.setShardAccount(
                validator.address,
                validator.shardAccount
            );

        });


        it('should run election, accept stake, change code and still have stake', async () => {
            await runElection(blockchain, elector, timePivot);

            let data = await elector.getData();

            const stakeAmount = 10000;

            const stake = toNano(stakeAmount) + toNano(1); //stake + gas fee
            let stakeMessage = await createNewStakeMessage(
                validator.address,
                elector.address,
                stake,
                validator.keyPair,
                data!!.currentElection!!.electAt
            );

            await blockchain.sendMessage(stakeMessage);

            //should have stake
            await checkStakeEquals(elector, stakeAmount);
            //upgrade code
            await upgradeCode(blockchain, elector, newElectorCode);
            //should still have stake
            await checkStakeEquals(elector, stakeAmount);
        });


        it('should run election, accept stake, change code and still have stake, change code back', async () => {
            await runElection(blockchain, elector, timePivot);

            let data = await elector.getData();

            const stakeAmount = 10000;

            const stake = toNano(stakeAmount) + toNano(1); //stake + gas fee
            let stakeMessage = await createNewStakeMessage(
                validator.address,
                elector.address,
                stake,
                validator.keyPair,
                data!!.currentElection!!.electAt
            );

            await blockchain.sendMessage(stakeMessage);

            //should have stake
            await checkStakeEquals(elector, stakeAmount);
            //upgrade code
            await upgradeCode(blockchain, elector, newElectorCode);
            //should still have stake
            await checkStakeEquals(elector, stakeAmount);

            //upgrade code
            await upgradeCode(blockchain, elector, oldElectorCode);
            //should still have stake
            await checkStakeEquals(elector, stakeAmount);
        });

    });
})

function setValidatorCount(cfg: Cell, count: number) {
    let config = loadConfigDict(cfg);
    let builder = new Builder();
    let param16 = builder.storeInt(count, 16)
        .storeInt(count, 16)
        .storeInt(count, 16)
        .endCell();
    config.set(16, param16);

    let cfgBuilder = new Builder();
    config.storeDirect(cfgBuilder, Dictionary.Keys.Int(32), Dictionary.Values.Cell());
    return cfgBuilder.endCell();
}

async function runElection(blockchain: Blockchain, elector: SandboxContract<Elector>, timePivot: number) {

    blockchain.now = timePivot;
    await blockchain.runTickTock(elector.address, 'tick');

    let activeElection = await elector.getActiveElectionId();

    let dict = loadConfigDict(TychoExecutor.defaultConfig);
    let cell = dict.get(15);
    expect(cell).not.toBe(undefined);
    let expectedElectAt = cell!!.beginParse().skip(32).loadInt(32);

    expect(activeElection.electionId).toEqual(BigInt(timePivot + expectedElectAt));
}

async function upgradeCode(blockchain: Blockchain, elector: SandboxContract<Elector>, newCode: Cell,) {
    let updateCodeMessage = createUpdateCodeMessage(CONFIG_ADDR, ELECTOR_ADDR, newCode, toNano(100));

    let oldCodeHash = await elector.getCodeHash();
    expect(oldCodeHash).not.toEqual(null);
    let result = await blockchain.sendMessage(updateCodeMessage);
    expect(result.transactions.length).toBeGreaterThan(1);
    expect(result.events.length).toEqual(1);
    let event = result.events[0];
    expect(event.type).toEqual('message_sent');
    if (event.type == "message_sent") {
        expect(event.from.hash).toEqual(ELECTOR_ADDR.hash);
        expect(event.to.hash).toEqual(CONFIG_ADDR.hash);
        let answerTag = event.body.beginParse().loadUint(32);
        expect(answerTag).toBe(0xce436f64);
    }


    let newCodeHash = await elector.getCodeHash();
    expect(newCodeHash).not.toEqual(null);
    expect(oldCodeHash).not.toEqual(newCodeHash);
}

async function checkStakeEquals(elector: SandboxContract<Elector>, stake: number) {
    let data = await elector.getData();
    expect(data).not.toBeNull();
    expect(data!!.currentElection).not.toBeNull();
    //should have value - gas fee
    expect(data!!.currentElection!.totalStake).toEqual(toNano(stake));
}


function createUpdateCodeMessage(src: Address, dst: Address, code: Cell, coins: bigint): Message {
    let body = new Builder();
    body.storeUint(0x4e436f64, 32);
    body.storeUint(1240, 64); // queryId
    body.storeRef(code);
    body.storeInt(0, 32); //additional arg to execute afterCodeUpgrade
    let bodyCell = body.endCell();

    return {
        info: {
            type: 'internal',
            ihrDisabled: true,
            bounce: false,
            bounced: false,
            src: src,
            dest: dst,
            value: {
                coins: coins,
                other: undefined,
            },
            ihrFee: 0n,
            forwardFee: 0n,
            createdLt: 1000000n,
            createdAt: Math.floor(Date.now() / 1000)
        },
        init: null,
        body: bodyCell
    };
}