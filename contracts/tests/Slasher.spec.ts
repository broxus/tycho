import "@ton/test-utils";
import {address, beginCell, Cell, Dictionary, toNano} from "@ton/core";
import {TychoExecutor} from "@tychosdk/emulator";
import {ConfigParams, makeValidatorSet, ValidatorAccount} from "../wrappers/util";
import {compile} from "@ton/blueprint";
import {Blockchain, createShardAccount, SmartContract} from "@ton/sandbox";
import {Slasher, storeSlasherData} from "../wrappers/Slasher";

const SLASHER_ADDR = address(
    "-1:8888888888888888888888888888888888888888888888888888888888888888"
);

describe("Slasher", () => {
    let code: Cell;
    let executor: TychoExecutor;

    let validators: ValidatorAccount[];

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
                ValidatorAccount.makeStub(address(`-1:${`e${i}`.repeat(32)}`))
            );

        let currentVset = makeValidatorSet({
            utimeSince: 1,
            utimeUntil: 10,
            accounts: validators,
        });
        config.setCurrentVset(currentVset)

        blockchain = await Blockchain.create({
            config: config.toCell(),
            executor,
        });

        blockchain.prevBlocks = {
            lastMcBlocks: [{
                workchain: -1,
                seqno: 1,
                shard: BigInt("0"),
                fileHash: Buffer.from([]),
                rootHash: Buffer.from([])
            }],
            prevKeyBlock: {
                workchain: -1,
                seqno: 1,
                shard: BigInt("0"),
                fileHash: Buffer.from([]),
                rootHash: Buffer.from([])
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

        for (let [index, validator] of validators.entries()) {
            await slasher.receiveMessage(validator.createVoteMessage(index, slasher.address, 0))
        }

        const data = await getters(blockchain, slasher).getData();
        expect(data.votes.size).toBe(4);
    })

});

function getters(blockchain: Blockchain, slasher: SmartContract) {
    return blockchain.openContract(Slasher.createFromAddress(slasher.address));
}