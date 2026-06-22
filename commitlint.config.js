module.exports = {
  extends: ["@commitlint/config-conventional"],
  rules: {
    "type-enum": [
      2,
      "always",
      [
        "feat",
        "fix",
        "perf",
        "refactor",
        "style",
        "test",
        "build",
        "doc",
        "chore",
        "version",
        "dep",
        "security",
        "revert",
        "drop",
        "obs",
      ],
    ],
  },
};
