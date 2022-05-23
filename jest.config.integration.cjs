module.exports = {
  preset: "ts-jest",
  testEnvironment: "node",
  roots: ["./packages/node", "./packages/social", "./packages/social-interface", "./packages/social-client", "./packages/orbit-db-types", "./packages/orbit-db-bdocstore", "./packages/orbit-db-bkvstore"],
  transform: {
    '^.+\\.ts?$': 'ts-jest',
  },
  testRegex: "/__tests__/[A-Za-z0-9]+\\.integration\\.(test|spec)\\.ts$",
  moduleFileExtensions: ["ts", "tsx", "js", "jsx", "json", "node"],
  testTimeout: 600000,
  globals: {
    "ts-jest": {
      tsconfig: {
        // allow js in typescript
        allowJs: true,
      },
    },
  },
};
