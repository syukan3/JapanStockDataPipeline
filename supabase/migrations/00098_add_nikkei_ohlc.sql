-- 00098_add_nikkei_ohlc.sql
-- 日経平均の始値/高値/安値を market_indicators に追加。
-- 用途: Portfolio 側のレジーム判定（ADX14=Wilder平滑・ボリンジャーバンド幅20日2σ）の計算材料。
--
-- 書き込みは既存 yahoo グループ（Yahoo Finance ^N225 chart API・
-- scripts/cron/refresh-market-indicators.ts / scripts/seed/market-indicators.ts）が
-- 00068 の規約通り「担当カラムのみ」を列単位の同一shapeでupsertする。
-- Yahoo 応答の OHLC には null穴があり得るため、close とは独立に列ごとに埋める
-- （nikkei_close はあるが OHLC が null の日を許容。Portfolio 側は null を欠損として扱う）。
--
-- RLS/GRANT はテーブル単位で設定済み（00068）のため列追加に伴う作業は無し。

ALTER TABLE analytics.market_indicators
  ADD COLUMN IF NOT EXISTS nikkei_open numeric(12,2),
  ADD COLUMN IF NOT EXISTS nikkei_high numeric(12,2),
  ADD COLUMN IF NOT EXISTS nikkei_low  numeric(12,2);

COMMENT ON COLUMN analytics.market_indicators.nikkei_open IS
  '日経平均 始値（Yahoo Finance ^N225）。ソースにnull穴があり得る（終値のみの日はNULL）';
COMMENT ON COLUMN analytics.market_indicators.nikkei_high IS
  '日経平均 高値（Yahoo Finance ^N225）。ソースにnull穴があり得る';
COMMENT ON COLUMN analytics.market_indicators.nikkei_low IS
  '日経平均 安値（Yahoo Finance ^N225）。ソースにnull穴があり得る';
