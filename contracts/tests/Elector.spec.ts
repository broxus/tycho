import {Blockchain, createShardAccount, SandboxContract} from "@ton/sandbox";
import {Address, address, beginCell, Builder, Cell, Dictionary, Message, toNano} from "@ton/core";
import {cryptoWithSignatureId, TychoExecutor} from "@tychosdk/emulator";
import {Elector, storeElectorData} from "../wrappers/Elector";
import {compile} from "@ton/blueprint";
import {KeyPair} from "@ton/crypto";

import "@ton/test-utils";
import {
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

        blockchain = await Blockchain.create({
            config: TychoExecutor.defaultConfig,
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

        const validator1Address = address("-1:9f6723605fab64ef5da7105943b26092ce0858ccbdffa10e77676f2a87674421");
        let keypair: KeyPair;

        beforeAll(async () => {
            keypair = await generateRandomKeyPair();
            await blockchain.setShardAccount(
                validator1Address,
                createShardAccount({
                    address: validator1Address,
                    balance: toNano(200000),
                    code: Cell.fromBase64("te6ccgEBAQEABAAABPgA"),
                    data: beginCell()
                        .storeInt(1n, 32)
                        .endCell(),
                    workchain: -1,
                })
            );

        });

        it("should have active election", async () => {
            await runElection(blockchain, elector, timePivot);
        });


        it('should have stake', async () => {
            blockchain.now = timePivot;
            await runElection(blockchain, elector, timePivot);
            let data = await elector.getData();

            const stake = toNano(10001);
            let stakeMessage = await createNewStakeMessage(
                validator1Address,
                elector.address,
                stake,
                keypair,
                data!!.currentElection!!.electAt
            );

            await blockchain.sendMessage(stakeMessage);

            await checkStakeEquals(elector, 10000);
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

        const validator1Address = address("-1:9f6723605fab64ef5da7105943b26092ce0858ccbdffa10e77676f2a87674421");
        let keypair: KeyPair;

        beforeAll(async () => {
            keypair = await generateRandomKeyPair();
            await blockchain.setShardAccount(
                validator1Address,
                createShardAccount({
                    address: validator1Address,
                    balance: toNano(200000),
                    code: Cell.fromBase64("te6ccgEBAQEABAAABPgA"),
                    data: beginCell()
                        .storeInt(1n, 32)
                        .endCell(),
                    workchain: -1,
                })
            );

        });


        it('should run election, accept stake, change code and still have stake', async () => {
            await runElection(blockchain, elector, timePivot);

            let data = await elector.getData();

            const stakeAmount = 10000;

            const stake = toNano(stakeAmount) + toNano(1); //stake + gas fee
            let stakeMessage = await createNewStakeMessage(
                validator1Address,
                elector.address,
                stake,
                keypair,
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
                validator1Address,
                elector.address,
                stake,
                keypair,
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