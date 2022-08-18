module.exports = {
  preset: "ts-jest",
  testEnvironment: "node",
  roots: ["./packages/shard", "./packages/time", "./packages/identity", "./packages/ipfs-log", "./packages/ipfs-log-entry", "packages/orbit-db-keystore", "packages/orbit-db-access-controllers", "./packages/orbit-db", "./packages/orbit-db-cache", "./packages/orbit-db-pubsub", "./packages/orbit-db-io", "./packages/io-utils", "./packages/encryption-utils", "./packages/orbit-db-trust-web", "./packages/orbit-db-dynamic-access-controller", "./packages/orbit-db-identity-provider", "./packages/orbit-db-store", "./packages/orbit-db-bstores", "./packages/orbit-db-query-store", "./packages/bquery", "./packages/orbit-db-string", "./packages/orbit-db-bfeedstore", "./packages/orbit-db-types", "./packages/orbit-db-bdocstore", "./packages/orbit-db-bkvstore"],
  transform: {
    '^.+\\.ts?$': 'ts-jest',
  },
  transformIgnorePatterns: [],
  /*  extensionsToTreatAsEsm: [".ts"], */

  testRegex: "/__tests__/.*\\.(test|spec)\\.ts$",
  moduleFileExtensions: ["ts", "tsx", "js", "jsx", "json", "node"],
  testTimeout: 260000,
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
