import {Blockchain, createShardAccount, loadConfig, SandboxContract} from "@ton/sandbox";
import {Address, address, beginCell, Builder, Cell, Dictionary, Message, toNano} from "@ton/core";
import {TychoExecutor} from "@tychosdk/emulator";
import {Elector, storeElectorData} from "../wrappers/Elector";
import "@ton/test-utils";
import {compile} from "@ton/blueprint";
import {parseDict} from "@ton/core/dist/dict/parseDict";

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


        //let newHash = updatedConfig.hash();
        //console.log("Hashes", hash, newHash);

        let tychoConfig = TychoExecutor.defaultConfig;
        let dict = loadConfigDict(TychoExecutor.defaultConfig)

        blockchain = await Blockchain.create({
            config: updatedConfig,
            executor,
        });

        let cfg = loadConfig(blockchain.config);
        console.log("cfg", cfg);

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
    });

    it("should parse data", async () => {
        const data = await elector.getData();
        console.log(data);
    });

    describe("Validator submission", () => {
        const validator1Address = createValidatorAddress('-1:c81b5e0e2654a79e7b5bcf0ecf4e395119832ca17f53260bbce113bc78907280');

        beforeAll(async () => {
            await blockchain.setShardAccount(
                validator1Address,
                createShardAccount({
                    address: validator1Address,
                    balance: toNano(200000),
                    code,
                    data: beginCell()
                        .storeInt(1n, 32)
                        .endCell(),
                    workchain: -1,
                })
            );


            let result = await blockchain.runTickTock(elector.address, 'tick');
            console.log('tick tock result', result);
        })

        it("should have active election", async () => {
            let activeElection = await elector.getActiveElectionId();
            console.log("active election", activeElection);
            expect(activeElection).toBeGreaterThan(0)
        })


        it('should have stake', async () => {
            const stake = 1000000n;
            let stakeMessage = createNewStakeMessage(validator1Address, elector.address, stake);
            await blockchain.sendMessage(stakeMessage);
            let currentStake = await elector.getStake(validator1Address);
            console.log("stake", stakeMessage);

            expect(currentStake.value).toEqual(stake)
        })
    })
});

function createNewStakeMessage(src: Address, dst: Address, coins: bigint): Message {
    let body = new Builder();
    body.storeInt(0x4e73744b, 32);
    body.storeInt(1, 64); // queryId
    let bodyCell = body.endCell();
    return {
        info: {
            type: 'internal',
            ihrDisabled: true,
            bounce: true,
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

function createValidatorAddress(addr: string): Address {
    return address(addr);
}

function loadConfigDict(configCellOrBase64: string | Cell) {
    return (typeof configCellOrBase64 === 'string' ? Cell.fromBase64(configCellOrBase64) : configCellOrBase64)
        .beginParse()
        .loadDictDirect(Dictionary.Keys.Int(32), Dictionary.Values.Cell());
}


