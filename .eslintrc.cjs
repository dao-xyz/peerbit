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
	"ignorePatterns": ["**/*.test.ts", "/**/lib/", "/**/frontend/dist", "/**/test-utils/", ".release-please-manifest.json"],
	"rules": {
		"no-mixed-spaces-and-tabs": "off",
		"@typescript-eslint/no-explicit-any": "off",
		"@typescript-eslint/prefer-as-const": "off",
		"@typescript-eslint/no-empty-interface": "off",
		"no-useless-escape": "off",
		"no-return-await": "error",
		//"require-await": "error" TODO
	},
}
