import "@ton/test-utils";
import { address, beginCell, Cell, Dictionary, toNano } from "@ton/core";
import { cryptoWithSignatureId, TychoExecutor } from "@tychosdk/emulator";
import { ConfigParams, makeValidatorSet, simpleInternal, ValidatorAccount, ValidatorData } from "../wrappers/util";
import { compile } from "@ton/blueprint";
import { Blockchain, createShardAccount, SmartContract } from "@ton/sandbox";
import { Slasher, storeSlasherData } from "../wrappers/Slasher";

const SLASHER_ADDR = address(
    "-1:8888888888888888888888888888888888888888888888888888888888888888"
);

const ELECTOR_ADDR = address(
    "-1:3333333333333333333333333333333333333333333333333333333333333333"
);

const SLASHER_AGGREGATE_STATS_OP = 100;

describe("Slasher", () => {
    let code: Cell;
    let executor: TychoExecutor;

    let validators: ValidatorAccount[];
    let crypto: ReturnType<typeof cryptoWithSignatureId>;
    let globalId: number;

    const config = new ConfigParams(TychoExecutor.defaultConfig);


    beforeAll(async () => {
        code = await compile("Slasher");
        executor = await TychoExecutor.create();
    });

    let blockchain: Blockchain;
    let slasher: SmartContract;

    beforeEach(async () => {

        validators = Array(4)
            .fill(null)
            .map((_, i) =>
                ValidatorAccount.makeStub(address(`-1:${`e${i}`.repeat(32)}`), i)
            );

        let currentVset = makeValidatorSet({
            utimeSince: 1,
            utimeUntil: 10,
            accounts: validators,
        });

        config.setCurrentVset(currentVset);
        globalId = config.getGlobalId();
        crypto = cryptoWithSignatureId(globalId);

        blockchain = await Blockchain.create({
            config: config.toCell(),
            executor,
        });

        const hexString = BigInt(123).toString(16);
        const buffer = Buffer.from(hexString, 'hex');

        blockchain.prevBlocks = {
            lastMcBlocks: [{
                workchain: -1,
                seqno: 100,
                shard: BigInt("0"),
                fileHash: buffer,
                rootHash: buffer
            }],
            prevKeyBlock: {
                workchain: -1,
                seqno: 100,
                shard: BigInt("0"),
                fileHash: buffer,
                rootHash: buffer
            }
        }


        await blockchain.setShardAccount(
            SLASHER_ADDR,
            createShardAccount({
                address: SLASHER_ADDR,
                balance: toNano(500),
                code,
                data: beginCell()
                    .store(
                        storeSlasherData({
                            votes: Dictionary.empty(),
                            punishedValidators: null,
                        })
                    )
                    .endCell(),
                workchain: -1,
            })
        );

        slasher = await blockchain.getContract(SLASHER_ADDR);
        await blockchain.setVerbosityForAddress(slasher.address, {
            blockchainLogs: true,
            debugLogs: true,
            //vmLogs: 'vm_logs',
        });
    });

    it("should parse data", async () => {
        const data = await getters(blockchain, slasher).getData();
        expect(data).not.toBeNull();
    });

    it("should update votes", async () => {

        for (let validator of validators) {
            let accounts: ValidatorData[] = validators
                .filter(x => x.validatorId != validator.validatorId)
                .map(x => {
                    return {
                        validatorId: x.validatorId,
                        signatureInfo: Array(100).fill({ validSignatures: false, invalidSignatures: true })
                    }
                });

            await slasher.receiveMessage(validator.createVoteMessage(slasher.address, 0, accounts, crypto))
        }


        const data = await getters(blockchain, slasher).getData();

        expect(data.votes.size).toBe(4);
        for (let votes of data.votes.values()) {
            expect(votes).toBe(100);
        }

        let votes = await getters(blockchain, slasher).getVotes();
        expect(votes.length).toBe(4);
        for (let vote of votes) {
            expect(vote.votes).toBe(100);
        }
    });

    it("should select validators", async () => {
        for (let validator of validators) {
            let accounts: ValidatorData[] = validators
                .filter(x => x.validatorId != validator.validatorId)
                .map(x => {
                    return {
                        validatorId: x.validatorId,
                        signatureInfo: Array(100).fill({ validSignatures: false, invalidSignatures: true })
                    }
                });

            await slasher.receiveMessage(validator.createVoteMessage(slasher.address, 0, accounts, crypto))
        }


        let transaction = await slasher.receiveMessage(
            simpleInternal({
                src: ELECTOR_ADDR,
                dest: SLASHER_ADDR,
                bounce: false,
                bounced: false,
                value: toNano(1),
                body: beginCell()
                    .storeUint(SLASHER_AGGREGATE_STATS_OP, 32)
                    .storeUint(0, 32)
                    .endCell(),
            })
        );

        expect(transaction.outMessages.size).toBe(1);
        for (let m of transaction.outMessages.values()) {
            if (m.info.type == "internal") {
                expect(m.info.dest.toRawString()).toEqual(ELECTOR_ADDR.toRawString());
            }
        }

        const data = await getters(blockchain, slasher).getData();


        expect(data.punishedValidators).not.toBeNull();
        expect(data.punishedValidators!.list.size).toBe(4);

        let votes = await getters(blockchain, slasher).getVotes();
        expect(votes.length).toBe(0);
    });

});

function getters(blockchain: Blockchain, slasher: SmartContract) {
    return blockchain.openContract(Slasher.createFromAddress(slasher.address));
}
