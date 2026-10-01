-- 00133_jquants_alt_sources.down.sql
-- J-Quants 代替データの器を取り除く。
-- 先に Portfolio の fn_holdings_summary（v_equity_price_daily 参照版）と、ビューを読む
-- Scouter / Portfolio のコードを戻しておくこと（ビューを消すと読み出しが失敗する）。
-- 代替行（source<>'jquants'）が残っている場合は、先に jquants-restore で公式値へ置き換えること。

ALTER TABLE analytics.market_indicators DROP CONSTRAINT IF EXISTS market_indicators_breadth_source_chk;
ALTER TABLE analytics.market_indicators DROP COLUMN IF EXISTS breadth_source;

DROP VIEW IF EXISTS jquants_core.v_equity_price_daily;
DROP TABLE IF EXISTS jquants_core.alt_split_events;
DROP TABLE IF EXISTS jquants_core.alt_equity_bar_daily;

ALTER TABLE jquants_core.investor_type_trading DROP CONSTRAINT IF EXISTS investor_type_trading_source_chk;
ALTER TABLE jquants_core.investor_type_trading DROP COLUMN IF EXISTS source;
ALTER TABLE jquants_core.topix_bar_daily DROP CONSTRAINT IF EXISTS topix_bar_daily_source_chk;
ALTER TABLE jquants_core.topix_bar_daily DROP COLUMN IF EXISTS source;
ALTER TABLE jquants_core.trading_calendar DROP CONSTRAINT IF EXISTS trading_calendar_source_chk;
ALTER TABLE jquants_core.trading_calendar DROP COLUMN IF EXISTS source;
