-- 00022_fix_function_search_path.sql
-- set_updated_at 関数に search_path を設定
--
-- 問題: search_path が設定されていない関数は呼び出し元の search_path に依存し、
--       search_path injection 攻撃のリスクがある
-- 修正: SET search_path = '' を追加し、関数内の参照を明示的にする

-- ============================================
-- 1. scouter.set_updated_at() を再作成
-- ============================================

CREATE OR REPLACE FUNCTION scouter.set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = ''
AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$;

-- ============================================
-- 2. jquants_core.set_updated_at() を再作成
-- ============================================

CREATE OR REPLACE FUNCTION jquants_core.set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = ''
AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$;
