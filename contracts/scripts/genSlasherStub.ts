import arg from "arg";
import { address, beginCell, storeAccount, toNano } from "@ton/core";
import { storeSlasherStubData } from "../wrappers/SlasherStub";
import { compile } from "@ton/blueprint";

async function main() {
  const args = arg({
    "--balance": String,
  });
  const balance = args["--balance"];
  if (balance == null) {
    throw new Error("`--balance` option is missing");
  }

  const code = await compile("SlasherStub");

  const account = beginCell()
    .storeBit(true)
    .store(
      storeAccount({
        addr: address(
          "-1:0000000000000000000000000000000000000000000000000000000000000000"
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
              data: beginCell()
                .store(
                  storeSlasherStubData({
                    updatedAtMs: 0n,
                  })
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
      })
    )
    .endCell();
  console.log(account.toBoc().toString("base64"));
}

main().catch(console.error);
