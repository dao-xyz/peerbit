import js from "@eslint/js";
import stylistic from "@stylistic/eslint-plugin";
import tsPlugin from "@typescript-eslint/eslint-plugin";
import tsParser from "@typescript-eslint/parser";
import prettier from "eslint-config-prettier";
import jestPlugin from "eslint-plugin-jest";
import reactHooks from "eslint-plugin-react-hooks";

const ignores = [
	"**/node_modules/**",
	"**/dist/**",
	"docs/**",
	"**/*.d.ts",
	"**/packages/utils/rateless-iblt/**",
	"eslint.config.js",
	"packages/**/public/**",
];

export default [
	{ ignores },
	js.configs.recommended,
	{
		files: ["**/*.{ts,tsx,js,jsx,mjs,cjs}"],
		languageOptions: {
			parser: tsParser,
			parserOptions: {
				sourceType: "module",
				ecmaVersion: "latest",
			},
		},
		plugins: {
			"@typescript-eslint": tsPlugin,
			"@stylistic": stylistic,
			"react-hooks": reactHooks,
			jest: jestPlugin,
		},
		rules: {
			"@stylistic/indent": ["warn", "tab", { SwitchCase: 1 }],
			"@stylistic/no-tabs": "off",
			"@stylistic/quotes": "off",
			"@stylistic/semi": "off",
			"@stylistic/space-before-function-paren": "off",
			"@stylistic/comma-dangle": "off",
			"@stylistic/no-trailing-spaces": "off",
			"@stylistic/lines-between-class-members": "off",
			"@stylistic/array-bracket-spacing": "off",
			"@stylistic/no-multiple-empty-lines": "off",
			"@stylistic/brace-style": "off",
			"@stylistic/operator-linebreak": "off",
			"@stylistic/no-mixed-spaces-and-tabs": "off",
			"import/order": "off",
			"no-implicit-coercion": "off",
			"no-mixed-spaces-and-tabs": "off",
			"no-warning-comments": "off",
			"no-dupe-class-members": "off",
			"no-redeclare": "off",
			"no-cond-assign": "off",
			"no-empty": "off",
			"no-fallthrough": "off",
			"no-prototype-builtins": "off",
			complexity: "off",
			"max-depth": "off",
			"max-nested-callbacks": "off",
			"max-params": "off",
			"new-cap": "off",
			"object-shorthand": "off",
			"no-console": "off",
			"no-undef": "off",
			"no-unused-vars": "off",
			"@typescript-eslint/no-unused-vars": [
				"warn",
				{
					args: "none",
					varsIgnorePattern: "^_",
					caughtErrorsIgnorePattern: "^_",
					ignoreRestSiblings: true,
				},
			],
			"@typescript-eslint/explicit-function-return-type": "off",
			"@typescript-eslint/method-signature-style": "off",
			"@typescript-eslint/await-thenable": "off",
			"@typescript-eslint/consistent-type-imports": "off",
			"@typescript-eslint/consistent-type-exports": "off",
			"@typescript-eslint/no-useless-constructor": "off",
			"@typescript-eslint/no-base-to-string": "off",
			"@typescript-eslint/no-implied-eval": "off",
			"@typescript-eslint/no-throw-literal": "off",
			"@typescript-eslint/prefer-includes": "off",
			"@typescript-eslint/no-redeclare": "off",
			"@typescript-eslint/no-dupe-class-members": "off",
			"jsdoc/require-hyphen-before-param-description": "off",
			"jsdoc/tag-lines": "off",
			"no-constant-condition": "off",
			"no-useless-catch": "off",
			"jest/valid-expect-in-promise": "off",
			"react-hooks/exhaustive-deps": "off",
		},
	},
	prettier,
];
