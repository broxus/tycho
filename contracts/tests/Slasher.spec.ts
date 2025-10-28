import "@ton/test-utils";
import {address, beginCell, Cell, Dictionary, toNano} from "@ton/core";
import {TychoExecutor} from "@tychosdk/emulator";
import {ConfigParams} from "../wrappers/util";
import {compile} from "@ton/blueprint";
import {Blockchain, createShardAccount, SmartContract} from "@ton/sandbox";
import {Slasher, storeSlasherData} from "../wrappers/Slasher";

const SLASHER_ADDR = address(
    "-1:8888888888888888888888888888888888888888888888888888888888888888"
);

describe("Slasher", () => {
    let code: Cell;
    let executor: TychoExecutor;

    const config = new ConfigParams(TychoExecutor.defaultConfig);

    beforeAll(async () => {
        code = await compile("Slasher");
        executor = await TychoExecutor.create();
        // masterKey = await getSecureRandomBytes(32).then(keyPairFromSeed);
    });

    let blockchain: Blockchain;
    let slasher: SmartContract;

    beforeEach(async () => {
        blockchain = await Blockchain.create({
            config: config.toCell(),
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

});

function getters(blockchain: Blockchain, slasher: SmartContract) {
    return blockchain.openContract(Slasher.createFromAddress(slasher.address));
}