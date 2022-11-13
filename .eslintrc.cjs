module.exports = {
    "env": {
        "browser": true,
        "es2021": true,
        "node": true
    },
    "extends": [
        "eslint:recommended",
        "plugin:@typescript-eslint/recommended"
    ],
    "overrides": [
    ],
    "parser": "@typescript-eslint/parser",
    "parserOptions": {
        "ecmaVersion": "latest",
        "sourceType": "module"
    },
    "plugins": [
        "@typescript-eslint"
    ],
    "ignorePatterns": ["**/*.test.ts", "/**/lib/", "/**/test-utils/"],
    "rules": {
        "@typescript-eslint/no-explicit-any": "off",
        "@typescript-eslint/prefer-as-const": "off"
    },
}
