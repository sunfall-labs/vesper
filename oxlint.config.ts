import { recommended as effectRecommended } from '@effect/tsgo/oxlint-presets';
import { defineConfig } from 'oxlint';

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null;
const recommended: unknown = effectRecommended;
const recommendedRules =
  isRecord(recommended) && isRecord(recommended.rules) ? recommended.rules : {};
const strictEffectRules = Object.fromEntries(
  Object.keys(recommendedRules).map((rule) => [rule, 'error']),
);

export default defineConfig({
  extends: [effectRecommended],
  plugins: [
    'eslint',
    'typescript',
    'unicorn',
    'oxc',
    'import',
    'promise',
    'node',
    'vitest',
  ],
  categories: {
    correctness: 'error',
    suspicious: 'error',
    perf: 'error',
  },
  options: {
    denyWarnings: true,
    reportUnusedDisableDirectives: 'error',
  },
  rules: {
    ...strictEffectRules,
    'no-console': 'error',
    'no-throw-literal': 'error',
    'preserve-caught-error': 'error',
    'promise/catch-or-return': 'error',
    'promise/no-multiple-resolved': 'error',
    'import/no-cycle': 'error',
    'oxc/no-map-spread': 'error',
    'unicorn/prefer-set-has': 'error',
    'unicorn/prefer-array-find': 'error',
    'eslint/curly': 'error',
    'eslint/eqeqeq': 'error',
    'eslint/no-await-in-loop': 'off',
    'eslint/no-underscore-dangle': 'off',
    'typescript/await-thenable': 'error',
    'typescript/consistent-type-imports': 'error',
    'typescript/no-array-delete': 'error',
    'typescript/no-base-to-string': 'error',
    'typescript/no-confusing-void-expression': 'error',
    'typescript/no-deprecated': 'error',
    'typescript/no-duplicate-type-constituents': 'error',
    'typescript/no-empty-object-type': [
      'error',
      {
        // Public schema-derived interfaces are intentionally declaration-
        // mergeable. Bare empty interfaces and `{}` aliases remain rejected.
        allowInterfaces: 'with-single-extends',
      },
    ],
    'typescript/no-explicit-any': 'error',
    'typescript/no-floating-promises': 'error',
    'typescript/no-for-in-array': 'error',
    'typescript/no-implied-eval': 'error',
    'typescript/no-meaningless-void-operator': 'error',
    'typescript/no-misused-promises': 'error',
    'typescript/no-misused-spread': 'error',
    'typescript/no-mixed-enums': 'error',
    'typescript/no-non-null-assertion': 'error',
    'typescript/no-redundant-type-constituents': 'error',
    'typescript/no-unnecessary-boolean-literal-compare': 'error',
    'typescript/no-unnecessary-condition': 'error',
    'typescript/no-unnecessary-template-expression': 'error',
    'typescript/no-unnecessary-type-arguments': 'error',
    'typescript/no-unnecessary-type-assertion': 'error',
    'typescript/no-unnecessary-type-conversion': 'error',
    'typescript/no-unnecessary-type-parameters': 'error',
    'typescript/no-unsafe-argument': 'error',
    'typescript/no-unsafe-assignment': 'error',
    'typescript/no-unsafe-call': 'error',
    'typescript/no-unsafe-enum-comparison': 'error',
    'typescript/no-unsafe-member-access': 'error',
    'typescript/no-unsafe-return': 'error',
    'typescript/no-unsafe-type-assertion': 'error',
    'typescript/no-unsafe-unary-minus': 'error',
    'typescript/no-useless-default-assignment': 'error',
    'typescript/only-throw-error': 'error',
    'typescript/prefer-promise-reject-errors': 'error',
    'typescript/prefer-reduce-type-parameter': 'error',
    'typescript/prefer-return-this-type': 'error',
    'typescript/related-getter-setter-pairs': 'error',
    'typescript/require-array-sort-compare': 'error',
    'typescript/require-await': 'error',
    'typescript/restrict-plus-operands': [
      'error',
      {
        allowAny: false,
        allowBoolean: false,
        allowNullish: false,
        allowNumberAndString: false,
        allowRegExp: false,
      },
    ],
    'typescript/restrict-template-expressions': [
      'error',
      {
        allowAny: false,
        allowBoolean: false,
        allowNever: false,
        allowNullish: false,
        allowNumber: false,
        allowRegExp: false,
      },
    ],
    'typescript/return-await': ['error', 'error-handling-correctness-only'],
    'typescript/strict-boolean-expressions': 'error',
    'typescript/switch-exhaustiveness-check': 'error',
    'typescript/unbound-method': 'error',
    'typescript/use-unknown-in-catch-callback-variable': 'error',
    'vitest/no-disabled-tests': 'error',
    'vitest/no-focused-tests': 'error',
    'vitest/no-identical-title': 'error',
    'vitest/no-standalone-expect': 'off',
    'import/no-self-import': 'off',
    'eslint/no-unused-vars': [
      'error',
      {
        argsIgnorePattern: '^_',
        varsIgnorePattern: '^_',
      },
    ],
  },
  overrides: [
    {
      // These are executable entrypoints, benchmark harnesses, or build/test
      // configuration. They intentionally cross Node's Promise/process/module
      // boundary; rewriting them into Effect would obscure the boundary
      // without improving any published package runtime.
      files: [
        'scripts/**/*.mjs',
        'benchmarks/**/*.ts',
        'packages/agent/host/*.mjs',
        'tsdown.config.ts',
        'vitest.config.ts',
      ],
      rules: {
        'effecttsgo/async-function': 'off',
        'effecttsgo/new-promise': 'off',
        'effecttsgo/node-builtin-import': 'off',
        'effecttsgo/process-env': 'off',
      },
    },
    {
      files: ['**/*.test.ts', 'benchmarks/**/*.ts', 'examples/**/*.ts'],
      rules: {
        'no-console': 'off',
      },
    },
    {
      // A test body and an example program are application entrypoints, where
      // providing the complete layer is the intended ownership boundary.
      files: ['**/*.test.ts', 'examples/**/*.ts'],
      rules: {
        'effecttsgo/strict-effect-provide': 'off',
      },
    },
    {
      files: ['**/*.test.ts'],
      rules: {
        'unicorn/no-new-array': 'off',
        'unicorn/no-thenable': 'off',
        'oxc/no-map-spread': 'off',
        'eslint/no-unused-expressions': 'off',
      },
    },
  ],
});
