module.exports = {
  preset: "ts-jest",
  testEnvironment: "node",
  roots: ["./packages/shard", "./packages/orbit-db-bstores", "./packages/orbit-db-query-store", "./packages/bquery", "./packages/orbit-db-string", "./packages/orbit-db-bfeedstore", "./packages/orbit-db-types", "./packages/orbit-db-bdocstore", "./packages/orbit-db-bkvstore"],
  transform: {
    '^.+\\.ts?$': 'ts-jest',
  },
  transformIgnorePatterns: [],
  /*   extensionsToTreatAsEsm: [".ts"],
   */
  testRegex: "/__tests__/[A-Za-z0-9]+\\.integration\\.(test|spec)\\.ts$",
  moduleFileExtensions: ["ts", "tsx", "js", "jsx", "json", "node"],
  testTimeout: 600000,
  globals: {
    "ts-jest": {
      tsconfig: {
        // allow js in typescript
        allowJs: true,
      },
      /*  useESM: true, */
    },
  },
};
