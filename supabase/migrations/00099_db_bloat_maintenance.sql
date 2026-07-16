-- 00099_db_bloat_maintenance.sql
-- DB容量メンテナンス（Supabase Free継続戦略 §6-1）
--
-- 背景: 2026-07-16 実測で jquants_core.equity_bar_daily にデッドタプル約21万行
-- （live 140万行の約15%）が蓄積していた。分割再基準化(00093)等の大量UPDATE後、
-- 既定の autovacuum_vacuum_scale_factor 0.2（発火に約28万行必要）では
-- autovacuum がほぼ発火しないため（最終発火 2026-04-09）。

-- ============================================
-- equity_bar_daily の autovacuum 強化
-- ============================================
-- 0.02 = 約2.8万行のデッドタプルで発火（毎営業日の更新規模でも週内に回収される水準）

ALTER TABLE jquants_core.equity_bar_daily SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_analyze_scale_factor = 0.02
);

-- 補足1: 蓄積済みデッドタプルの物理回収 (VACUUM FULL) はトランザクション内で
-- 実行できないため本マイグレーションには含めない。適用後に手動で実行する:
--   VACUUM (FULL, ANALYZE) jquants_core.equity_bar_daily;
--
-- 補足2: 00010 が「1週間後に削除予定」として残した旧マスタ退避コピー
-- jquants_core.equity_master_snapshot_backup (約5.2MB) はコード参照なしを
-- 確認済みだが、削除はユーザー承認待ちのため本マイグレーションには含めない。
