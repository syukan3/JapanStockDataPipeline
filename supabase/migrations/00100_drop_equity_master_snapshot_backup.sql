-- 00100_drop_equity_master_snapshot_backup.sql
-- 旧マスタ退避コピーの削除（Supabase Free継続戦略 §6-1・2026-07-16ユーザー承認済み）
--
-- 00010 (SCD Type 2 移行) が「1週間後に削除予定」として残したバックアップテーブル
-- （約5.2MB・8874行）。データは equity_master へ移行済みで、全リポジトリで
-- コード参照が無いことを 2026-07-16 に確認済み。

DROP TABLE IF EXISTS jquants_core.equity_master_snapshot_backup;
