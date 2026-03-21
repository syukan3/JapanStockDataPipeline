-- macro_ai_evaluations v2.0: 独自分析強化のための新カラム追加
-- 全て nullable で既存データに影響なし

ALTER TABLE scouter.macro_ai_evaluations
  ADD COLUMN IF NOT EXISTS contrarian_view JSONB,
  ADD COLUMN IF NOT EXISTS divergences JSONB,
  ADD COLUMN IF NOT EXISTS delta_from_previous TEXT,
  ADD COLUMN IF NOT EXISTS sector_calls JSONB;

COMMENT ON COLUMN scouter.macro_ai_evaluations.contrarian_view IS 'コントラリアン視点: {consensus, ourView, catalyst, probability}';
COMMENT ON COLUMN scouter.macro_ai_evaluations.divergences IS '指標間の乖離分析: [{indicatorPair, description, implication}]';
COMMENT ON COLUMN scouter.macro_ai_evaluations.delta_from_previous IS '前日評価からの変化点分析';
COMMENT ON COLUMN scouter.macro_ai_evaluations.sector_calls IS 'セクター別判断: [{sector, stance, reasoning, entryCondition}]';
