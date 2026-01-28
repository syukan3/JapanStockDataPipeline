import { describe, it, expect } from 'vitest';
import { POSTGREST_ERROR_CODES, isPostgrestError } from '@/lib/supabase/errors';

describe('errors.ts', () => {
  describe('POSTGREST_ERROR_CODES', () => {
    it('NO_ROWS_RETURNED が PGRST116 である', () => {
      expect(POSTGREST_ERROR_CODES.NO_ROWS_RETURNED).toBe('PGRST116');
    });

    it('MULTIPLE_ROWS_RETURNED が PGRST200 である', () => {
      expect(POSTGREST_ERROR_CODES.MULTIPLE_ROWS_RETURNED).toBe('PGRST200');
    });

    it('RANGE_NOT_SATISFIABLE が PGRST103 である', () => {
      expect(POSTGREST_ERROR_CODES.RANGE_NOT_SATISFIABLE).toBe('PGRST103');
    });
  });

  describe('isPostgrestError', () => {
    it('一致するcodeでtrueを返す', () => {
      const error = { code: 'PGRST116', message: 'No rows found' };
      expect(isPostgrestError(error, POSTGREST_ERROR_CODES.NO_ROWS_RETURNED)).toBe(true);
    });

    it('異なるcodeでfalseを返す', () => {
      const error = { code: 'PGRST200', message: 'Multiple rows' };
      expect(isPostgrestError(error, POSTGREST_ERROR_CODES.NO_ROWS_RETURNED)).toBe(false);
    });

    it('nullでfalseを返す', () => {
      expect(isPostgrestError(null, POSTGREST_ERROR_CODES.NO_ROWS_RETURNED)).toBe(false);
    });

    it('undefinedでfalseを返す', () => {
      expect(isPostgrestError(undefined, POSTGREST_ERROR_CODES.NO_ROWS_RETURNED)).toBe(false);
    });

    it('codeなしオブジェクトでfalseを返す', () => {
      const error = { message: 'Some error' };
      expect(isPostgrestError(error, POSTGREST_ERROR_CODES.NO_ROWS_RETURNED)).toBe(false);
    });

    it('プリミティブ値（文字列）でfalseを返す', () => {
      expect(isPostgrestError('PGRST116', POSTGREST_ERROR_CODES.NO_ROWS_RETURNED)).toBe(false);
    });

    it('プリミティブ値（数値）でfalseを返す', () => {
      expect(isPostgrestError(123, POSTGREST_ERROR_CODES.NO_ROWS_RETURNED)).toBe(false);
    });

    it('codeが数値型でfalseを返す', () => {
      const error = { code: 116, message: 'Numeric code' };
      expect(isPostgrestError(error, POSTGREST_ERROR_CODES.NO_ROWS_RETURNED)).toBe(false);
    });

    it('Supabase形式エラーでtrueを返す', () => {
      // Supabaseクライアントが返す典型的なエラー形式
      const error = {
        code: 'PGRST116',
        message: 'No rows found',
        details: null,
        hint: null,
      };
      expect(isPostgrestError(error, POSTGREST_ERROR_CODES.NO_ROWS_RETURNED)).toBe(true);
    });
  });
});
