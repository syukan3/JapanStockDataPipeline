-- マクロAI定性評価テーブル
-- Gemini 3 Flash + Search Grounding による市場全体の定性評価

CREATE TABLE scouter.macro_ai_evaluations (
  eval_date             DATE NOT NULL PRIMARY KEY,
  overall_sentiment     TEXT NOT NULL CHECK (overall_sentiment IN ('bullish','neutral','bearish')),
  investment_readiness  SMALLINT NOT NULL CHECK (investment_readiness BETWEEN 0 AND 100),
  key_factors           JSONB NOT NULL,       -- 主要判断要因（3-5項目）
  risks                 JSONB NOT NULL,       -- リスク要因（3-5項目）
  opportunities         JSONB NOT NULL,       -- 機会要因（1-3項目）
  news_summary          TEXT,                 -- Search Groundingで取得したニュース要約
  news_source           TEXT,                 -- ニュースソース情報
  recommendation        TEXT NOT NULL,        -- 投資スタンス推奨
  confidence            SMALLINT NOT NULL CHECK (confidence BETWEEN 0 AND 100),
  input_macro_regime    JSONB NOT NULL,       -- トレーサビリティ: 入力したmacro_regime
  input_indicators      JSONB NOT NULL,       -- トレーサビリティ: 入力した全26指標値
  macro_regime_date     DATE NOT NULL,        -- 参照したmacro_regimeの日付
  model_id              TEXT NOT NULL,        -- 使用したモデルID
  prompt_version        TEXT NOT NULL,        -- プロンプトバージョン
  created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- インデックス（PKのeval_dateは自動インデックス）
CREATE INDEX idx_macro_ai_sentiment ON scouter.macro_ai_evaluations (overall_sentiment);

-- RLS
ALTER TABLE scouter.macro_ai_evaluations ENABLE ROW LEVEL SECURITY;

-- authenticated ロールに読み取り権限
CREATE POLICY "Allow authenticated read macro_ai_evaluations"
  ON scouter.macro_ai_evaluations
  FOR SELECT
  TO authenticated
  USING (true);

-- service_role に全権限
CREATE POLICY "Allow service_role full access macro_ai_evaluations"
  ON scouter.macro_ai_evaluations
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);

COMMENT ON TABLE scouter.macro_ai_evaluations IS 'マクロAI定性評価（Gemini 3 Flash + Search Grounding）';
COMMENT ON COLUMN scouter.macro_ai_evaluations.overall_sentiment IS '市場センチメント: bullish/neutral/bearish';
COMMENT ON COLUMN scouter.macro_ai_evaluations.investment_readiness IS '投資適性スコア 0-100';
COMMENT ON COLUMN scouter.macro_ai_evaluations.key_factors IS '主要判断要因（JSON配列）';
COMMENT ON COLUMN scouter.macro_ai_evaluations.risks IS 'リスク要因（JSON配列）';
COMMENT ON COLUMN scouter.macro_ai_evaluations.opportunities IS '機会要因（JSON配列）';
COMMENT ON COLUMN scouter.macro_ai_evaluations.input_macro_regime IS '入力に使用したmacro_regime（トレーサビリティ）';
COMMENT ON COLUMN scouter.macro_ai_evaluations.input_indicators IS '入力に使用した全26指標（トレーサビリティ）';
