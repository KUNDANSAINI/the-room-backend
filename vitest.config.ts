import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    testTimeout: 20_000,
    hookTimeout: 20_000,
    // Integration suites share one Redis DB; keep files sequential for determinism.
    fileParallelism: false,
  },
});
