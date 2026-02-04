-- 00021_fix_security_definer_view.sql
-- equity_master_snapshot ビューを SECURITY INVOKER に変更
--
-- 問題: SECURITY DEFINER ビューはビュー作成者（postgres）の権限でRLSを評価するため、
--       クエリ実行者のRLSポリシーがバイパスされる
-- 修正: SECURITY INVOKER に変更し、クエリ実行者の権限でRLSを評価させる

-- ============================================
-- 1. equity_master_snapshot を SECURITY INVOKER に変更
-- ============================================

-- PostgreSQL 15+ では ALTER VIEW で直接変更可能
ALTER VIEW jquants_core.equity_master_snapshot
  SET (security_invoker = true);

COMMENT ON VIEW jquants_core.equity_master_snapshot IS '上場銘柄マスタ互換ビュー（現在有効レコードのみ）';
