import type { JestConfigWithTsJest } from "ts-jest";

const jestConfig: JestConfigWithTsJest = {
	preset: "ts-jest",
	workerThreads: true,
	testEnvironment: "node",
	roots: ["./packages/", "./docs/"],
	transform: {
		"^.+\\.tsx?$": [
			"ts-jest",
			{
				useESM: true,
			},
		],
	},
	fakeTimers: {},
	extensionsToTreatAsEsm: [".ts"],
	moduleNameMapper: {
		uuid: require.resolve("uuid"),
		"^(\\.{1,2}/.*)\\.js$": "$1",
	},
	transformIgnorePatterns: ["dns"],
	// forceExit: true,
	/*  useESM: true, */
	testRegex: [
		"[A-Za-z0-9-/]+(\\.integration)?\\.(test|spec)\\.ts$",
		"[A-Za-z0-9-/]+(\\.integration)?\\.(test|spec)\\.ts$",
	],
	moduleFileExtensions: ["ts", "tsx", "js", "jsx", "json", "node"],
	testTimeout: 60000,
	setupFilesAfterEnv: ["jest-extended/all"],
	/*   coverageReporters: ["lcov"] */
};
export default jestConfig;
