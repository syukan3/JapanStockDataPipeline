-- macro_ai_evaluations: 市場内部指標(analytics.market_indicators)+SQカレンダーを踏まえた
-- 総合評価を追加するための新カラム。全て nullable で既存データに影響なし（00041と同じパターン）。

ALTER TABLE scouter.macro_ai_evaluations
  ADD COLUMN IF NOT EXISTS input_market_internals JSONB,
  ADD COLUMN IF NOT EXISTS market_internals_assessment JSONB;

COMMENT ON COLUMN scouter.macro_ai_evaluations.input_market_internals IS
  '入力: analytics.market_indicators のスナップショット（日経PER/EPS・騰落レシオ・空売り比率・信用評価損益率・新高値/新安値・売買代金・NT倍率 + 7日/30日前値）+ SQカレンダー情報';
COMMENT ON COLUMN scouter.macro_ai_evaluations.market_internals_assessment IS
  '市場内部指標+SQを踏まえたAIの総合評価: {summary, stance, sqCaution}';
