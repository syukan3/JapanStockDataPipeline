import js from '@eslint/js';
import tseslint from 'typescript-eslint';

export default tseslint.config(
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    ignores: ['node_modules/', '.next/', 'out/'],
  },
  {
    rules: {
      // 未使用の変数は警告（_で始まる変数は無視）
      '@typescript-eslint/no-unused-vars': [
        'warn',
        { argsIgnorePattern: '^_', varsIgnorePattern: '^_' },
      ],
      // anyの使用は警告
      '@typescript-eslint/no-explicit-any': 'warn',
    },
  }
);
