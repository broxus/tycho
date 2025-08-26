import { CompilerConfig } from "@ton/blueprint";

export const compile: CompilerConfig = {
  lang: "tolk",
  entrypoint: "src/config.tolk",
  withStackComments: true,
  withSrcLineComments: true,
  experimentalOptions: "",
};
