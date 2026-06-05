import arg from "arg";
import {
  address,
  beginCell,
  Dictionary,
  storeAccount,
  toNano,
} from "@ton/core";
import { storeSlasherData } from "../wrappers/Slasher";
import { compile } from "@ton/blueprint";

async function main() {
  const args = arg({
    "--balance": String,
  });
  const balance = args["--balance"];
  if (balance == null) {
    throw new Error("`--balance` option is missing");
  }

  const code = await compile("Slasher");

  const account = beginCell()
    .storeBit(true)
    .store(
      storeAccount({
        addr: address(
          "-1:0000000000000000000000000000000000000000000000000000000000000000",
        ),
        storage: {
          balance: {
            coins: toNano(balance),
          },
          lastTransLt: 0n,
          state: {
            type: "active",
            state: {
              code,
              special: {
                tick: true,
                tock: false,
              },
              data: beginCell()
                .store(
                  storeSlasherData({
                    currentVsetHash: Buffer.alloc(32),
                    validatorCount: 0,
                    sentBatches: Dictionary.empty(),
                  }),
                )
                .endCell(),
            },
          },
        },
        storageStats: {
          used: {
            bits: 0n,
            cells: 0n,
          },
          lastPaid: 0,
          storageExtra: null,
        },
      }),
    )
    .endCell();
  console.log(account.toBoc().toString("base64"));
}

main().catch(console.error);
