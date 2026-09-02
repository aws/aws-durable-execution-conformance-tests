import commonJs from "@rollup/plugin-commonjs";
import json from "@rollup/plugin-json";
import nodeResolve from "@rollup/plugin-node-resolve";
import typescript from "@rollup/plugin-typescript";
import { readdirSync } from "node:fs";
import { resolve } from "node:path";
import { defineConfig } from "rollup";

const handlersRoot = "handlers";
const entryPoints = Object.fromEntries(
  readdirSync(handlersRoot)
    .filter((file) => /^insight_.*\.ts$/.test(file))
    .map((file) => [file.replace(/\.ts$/, ""), `${handlersRoot}/${file}`]),
);
const handlerPaths = Object.values(entryPoints).map((path) => resolve(path));

// Only the AWS SDK v3 clients (@aws-sdk/*) are provided by the Lambda nodejs
// runtime. @smithy/* and @aws-crypto/* are NOT resolvable at runtime and must
// be bundled (leaving them external fails with Runtime.ImportModuleError:
// "Cannot find module '@aws-crypto/sha256-js'"). Unused optional exporters
// (Redshift, Aurora, OpenSearch, etc.) stay out of the bundle because their
// @aws-sdk/* clients remain external and are never imported at runtime.
const externalPattern = /^@aws-sdk\//;

export default defineConfig({
  input: entryPoints,
  external: (id) => externalPattern.test(id),
  output: {
    dir: "dist",
    format: "cjs",
    sourcemap: true,
    sourcemapExcludeSources: true,
    chunkFileNames: "[name].js",
    manualChunks: (id) => (handlerPaths.includes(id) ? null : "vendors"),
  },
  plugins: [
    typescript({ tsconfig: "./tsconfig.json" }),
    nodeResolve({ preferBuiltins: true }),
    json(),
    commonJs(),
  ],
  onwarn(warning, warn) {
    if (
      warning.code === "CIRCULAR_DEPENDENCY" &&
      warning.message.includes("node_modules")
    ) {
      return;
    }
    warn(warning);
  },
});
