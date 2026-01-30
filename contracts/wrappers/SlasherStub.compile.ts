import { CompilerConfig } from "@ton/blueprint";

export const compile: CompilerConfig = {
  lang: "tolk",
  entrypoint: "src/slasher-stub.tolk",
  withStackComments: true,
  withSrcLineComments: true,
  experimentalOptions: "",
};
