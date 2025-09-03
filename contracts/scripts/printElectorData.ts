import arg from "arg";
import util from "util";
import {
  TychoExecutor,
  TychoRemoteBlockchainStorage,
} from "@tychosdk/emulator";
import { Blockchain } from "@ton/sandbox";
import { Address, Cell, Dictionary } from "@ton/core";
import { Elector, loadElectorData } from "../wrappers/Elector";

const ELECTOR_ADDRESS = Address.parse(
  "-1:3333333333333333333333333333333333333333333333333333333333333333"
);

async function main() {
  const args = arg({
    "--rpc": String,
  });
  const rpcUrl = args["--rpc"];
  if (rpcUrl == null) {
    throw new Error("`--rpc` option is missing");
  }

  const storage = new TychoRemoteBlockchainStorage({
    url: rpcUrl,
  });
  const executor = await TychoExecutor.create();
  const blockchain = await Blockchain.create({
    executor,
    config: TychoExecutor.defaultConfig,
    storage,
  });

  const state = await storage.getContract(blockchain, ELECTOR_ADDRESS);
  if (state.account.account?.storage.state.type !== "active") {
    throw new Error("invalid elector state");
  }

  const data = state.account.account.storage.state.state.data || Cell.EMPTY;
  const cs = data.asSlice();

  const parsed = loadElectorData(cs);
  const poaData =
    cs.remainingBits != 0
      ? cs.loadDict(
          Dictionary.Keys.BigUint(256),
          Dictionary.Values.BitString(0)
        )
      : null;

  util.inspect.defaultOptions.depth = null;
  console.log("base_data", parsed);
  console.log("poa_data", poaData);
}

main().catch(console.error);
