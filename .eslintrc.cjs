module.exports = {
	"root": true,
	"env": {
		"browser": true,
		"es2021": true,
		"node": true
	},
	"extends": [
		"eslint:recommended",
		"plugin:@typescript-eslint/recommended"
	],
	"parser": "@typescript-eslint/parser",
	"parserOptions": {
		"ecmaVersion": "latest",
		"sourceType": "module"
	},
	"plugins": [
		"@typescript-eslint"
	],
	"ignorePatterns": ["**/*.test.ts", "/**/lib/", "/**/dist/", "/**/test-utils/", ".release-please-manifest.json"],
	"rules": {
		"no-mixed-spaces-and-tabs": "off",
		"@typescript-eslint/no-explicit-any": "off",
		"@typescript-eslint/prefer-as-const": "off",
		"@typescript-eslint/no-empty-interface": "off",
		"no-useless-escape": "off",
		"no-return-await": "error",
		"@typescript-eslint/no-unsafe-declaration-mergin": "off",
		"@typescript-eslint/no-unused-vars": "off",
		"@typescript-eslint/no-unsafe-declaration-merging": "off"
		//"require-await": "error" TODO
	},
}
