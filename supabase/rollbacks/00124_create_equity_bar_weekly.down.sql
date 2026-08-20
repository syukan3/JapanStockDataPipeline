-- 00124_create_equity_bar_weekly.down.sql
-- 00124（追跡銘柄限定の長期週足 + 分割再基準化RPC）のロールバック。
--
-- 注意: equity_bar_weekly は「J-Quants の10年ローリング窓から落ちた過去分」を含む
-- 自前の恒久系列になり得る（設計正本 §1 の副次価値）。DROP すると API から
-- 再取得できないデータが失われるため、実行前に必ずバックアップを取ること:
--   \copy (select * from analytics.equity_bar_weekly) to 'equity_bar_weekly.csv' csv header
--
-- 既存の他テーブル・00093 の日足 rebase には一切影響しない（新規オブジェクトのみを落とす）。

drop function if exists analytics.apply_weekly_rebase_event(text, date, numeric);
drop table if exists analytics.equity_bar_weekly_rebase_events;
drop table if exists analytics.equity_bar_weekly;
