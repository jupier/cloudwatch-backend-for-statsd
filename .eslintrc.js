module.exports = {
  env: {
    browser: true,
    commonjs: true,
  },
  extends: "eslint:recommended",
  parserOptions: {
    ecmaVersion: 5,
  },
  rules: {
    indent: ["error", 2],
    "linebreak-style": ["error", "unix"],
    quotes: ["error", "double"],
    semi: ["error", "always"],
  },
  globals: {
    describe: true,
    it: true,
  },
};
