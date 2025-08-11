import { CompilerConfig } from "@ton/blueprint";

export const compile: CompilerConfig = {
  lang: "tolk",
  entrypoint: "src/elector.tolk",
  withStackComments: true,
  withSrcLineComments: true,
  experimentalOptions: "",
};
