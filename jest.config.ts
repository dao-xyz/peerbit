import type { JestConfigWithTsJest } from 'ts-jest'

const jestConfig: JestConfigWithTsJest = {
  preset: "ts-jest",
  testEnvironment: "node",
  roots: ["./packages/utils/time", "./packages/utils/crypto", "./packages/ipfs/ipfs-pubsub-direct-channel", "./packages/ipfs/ipfs-pubsub-peer-monitor", "./packages/ipfs/ipfs-log", "packages/utils/keystore", "packages/contract/orbit-db-ipfs-access-controller", "./packages/client", "./packages/utils/test-utils", "./packages/utils/cache", "./packages/utils/io-utils", "./packages/utils/borsh-utils", "./packages/contract/trusted-network", "./packages/contract/discovery", "./packages/contract/dynamic-access-controller",/*  "./packages/orbit-db-identity-provider", */ "./packages/store/dstore", "./packages/store/orbit-db-query-store", "./packages/store/query-protocol", "./packages/store/dstring", "./packages/store/ddoc"],
  transform: {
    '^.+\\.tsx?$': [
      'ts-jest',
      {
        useESM: true,
      },
    ],
  },
  extensionsToTreatAsEsm: ['.ts'],
  forceExit: true,
  moduleNameMapper: {
    '^(\\.{1,2}/.*)\\.js$': '$1',
  },
  transformIgnorePatterns: [],

  /*  useESM: true, */
  testRegex: "/__tests__/[A-Za-z0-9-/]+(\\.integration)?\\.(test|spec)\\.ts$",
  /*   moduleFileExtensions: ["ts", "tsx", "js", "jsx", "json", "node"], */
  testTimeout: 600000,
  setupFilesAfterEnv: ['jest-extended/all'],

}
export default jestConfig;
