import type { JestConfigWithTsJest } from 'ts-jest'

const jestConfig: JestConfigWithTsJest = {
  preset: "ts-jest",
  testEnvironment: "node",
  roots: ["./packages/utils/time", "./packages/utils/crypto", "./packages/ipfs/ipfs-pubsub-direct-channel", "./packages/ipfs/ipfs-pubsub-peer-monitor", "./packages/ipfs/ipfs-log", "packages/identity/keystore", "packages/acl/orbit-db-ipfs-access-controller", "./packages/client", "./packages/utils/test-utils", "./packages/utils/cache", "./packages/utils/io-utils", "./packages/utils/borsh-utils", "./packages/acl/trusted-network", "./packages/acl/dynamic-access-controller",/*  "./packages/orbit-db-identity-provider", */ "./packages/store/orbit-db-store", "./packages/store/orbit-db-query-store", "./packages/store/query-protocol", "./packages/store/orbit-db-string", "./packages/store/orbit-db-bdocstore"],
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
