module.exports = {
  preset: "ts-jest",
  testEnvironment: "node",
  /* roots: ["./packages/shard"], */
  extensionsToTreatAsEsm: ['.ts'],
  globals: {
    'ts-jest': {
      useESM: true,
    },
  },
  moduleNameMapper: {
    '^(\\.{1,2}/.*)\\.js$': '$1',
  },
};
