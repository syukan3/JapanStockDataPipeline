-- 00090_extend_calibration_sources.sql
-- calibration_snapshots の source を3種追加:
--   macro_readiness  = investment_readiness(投資適性0-100)×TOPIXフォワードリターンの較正
--   market_internals = 市場内部指標スタンス(bullish/neutral/bearish)×TOPIX 5/21日リターンの較正
--   macro_sector     = セクターコール(sector_calls)×sector33等ウェイト超過リターンの検証
-- 詳細: docs/PLANS-macro-ai-calibration-2026-07.md

ALTER TABLE scouter.calibration_snapshots
  DROP CONSTRAINT IF EXISTS calibration_snapshots_source_check;

ALTER TABLE scouter.calibration_snapshots
  ADD CONSTRAINT calibration_snapshots_source_check
  CHECK (source IN ('hd_ai', 'macro_ai', 'macro_readiness', 'market_internals', 'macro_sector'));

COMMENT ON COLUMN scouter.calibration_snapshots.source IS
  'hd_ai=高配当AI評価 / macro_ai=マクロAI評価(confidence較正) / macro_readiness=investment_readiness較正 / market_internals=市場内部指標スタンス較正 / macro_sector=セクターコール検証';

COMMENT ON COLUMN scouter.calibration_snapshots.buckets IS
  '較正表(ソース別に形が異なる)。hd_ai/macro_ai=confidence 10刻みバケット配列 / macro_readiness={bands,spearman,nTotal} / market_internals=スタンス別配列 / macro_sector=セクター別配列';

COMMENT ON COLUMN scouter.calibration_snapshots.horizon_days IS
  '実現リターンの評価ホライズン(営業日)。hd_ai/macro_ai/macro_readiness=21,63 / market_internals=5,21 / macro_sector=21';
