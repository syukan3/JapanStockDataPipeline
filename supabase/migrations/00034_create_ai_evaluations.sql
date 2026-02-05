-- AI定性評価結果テーブル
-- BUY銘柄に対するGemini Search Groundingによる定性評価を格納

-- テーブルが存在しない場合は作成
CREATE TABLE IF NOT EXISTS scouter.ai_evaluations (
  id                     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  run_date               DATE NOT NULL,
  local_code             TEXT NOT NULL,
  sentiment              TEXT NOT NULL CHECK (sentiment IN ('positive', 'neutral', 'negative')),
  news_priced_in         BOOLEAN NOT NULL,
  risk_flags             TEXT[] NOT NULL DEFAULT '{}',
  confidence             INTEGER NOT NULL CHECK (confidence BETWEEN 0 AND 100),
  summary                TEXT NOT NULL,
  key_news               TEXT[] NOT NULL DEFAULT '{}',
  model_id               TEXT NOT NULL,
  skipped                BOOLEAN NOT NULL DEFAULT FALSE,
  previous_run_date      DATE,
  created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (run_date, local_code)
);

-- 高配当投資特化の評価項目カラムを追加（存在しない場合のみ）
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'scouter' AND table_name = 'ai_evaluations' AND column_name = 'dividend_sustainability'
  ) THEN
    ALTER TABLE scouter.ai_evaluations ADD COLUMN dividend_sustainability TEXT;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'scouter' AND table_name = 'ai_evaluations' AND column_name = 'industry_trend'
  ) THEN
    ALTER TABLE scouter.ai_evaluations ADD COLUMN industry_trend TEXT;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'scouter' AND table_name = 'ai_evaluations' AND column_name = 'management_stance'
  ) THEN
    ALTER TABLE scouter.ai_evaluations ADD COLUMN management_stance TEXT;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'scouter' AND table_name = 'ai_evaluations' AND column_name = 'competitive_position'
  ) THEN
    ALTER TABLE scouter.ai_evaluations ADD COLUMN competitive_position TEXT;
  END IF;
END
$$;

-- インデックス（存在しない場合のみ作成）
CREATE INDEX IF NOT EXISTS idx_ai_evaluations_local_code_run_date
  ON scouter.ai_evaluations (local_code, run_date DESC);

CREATE INDEX IF NOT EXISTS idx_ai_evaluations_sentiment
  ON scouter.ai_evaluations (run_date, sentiment)
  WHERE sentiment = 'negative';

-- RLS（既に有効な場合はスキップ）
ALTER TABLE scouter.ai_evaluations ENABLE ROW LEVEL SECURITY;

-- ポリシー（存在しない場合のみ作成）
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies WHERE schemaname = 'scouter' AND tablename = 'ai_evaluations' AND policyname = 'authenticated_select'
  ) THEN
    CREATE POLICY "authenticated_select" ON scouter.ai_evaluations
      FOR SELECT TO authenticated USING (TRUE);
  END IF;
END
$$;

-- 権限付与
GRANT SELECT ON scouter.ai_evaluations TO authenticated;

-- コメント
COMMENT ON TABLE scouter.ai_evaluations IS 'BUY銘柄のAI定性評価結果';
COMMENT ON COLUMN scouter.ai_evaluations.sentiment IS '総合的な市場センチメント';
COMMENT ON COLUMN scouter.ai_evaluations.news_priced_in IS '直近のニュースが株価に織り込まれているか';
COMMENT ON COLUMN scouter.ai_evaluations.risk_flags IS 'リスク要因（最大5件）';
COMMENT ON COLUMN scouter.ai_evaluations.confidence IS '評価の信頼度（0-100）';
COMMENT ON COLUMN scouter.ai_evaluations.summary IS '評価サマリー';
COMMENT ON COLUMN scouter.ai_evaluations.key_news IS '重要ニュース（最大3件）';
COMMENT ON COLUMN scouter.ai_evaluations.dividend_sustainability IS '配当の持続性評価';
COMMENT ON COLUMN scouter.ai_evaluations.industry_trend IS '業界動向分析';
COMMENT ON COLUMN scouter.ai_evaluations.management_stance IS '経営者の発言・姿勢分析';
COMMENT ON COLUMN scouter.ai_evaluations.competitive_position IS '競合比較分析';
COMMENT ON COLUMN scouter.ai_evaluations.model_id IS '使用したLLMモデルID';
COMMENT ON COLUMN scouter.ai_evaluations.skipped IS '前回評価を再利用したか';
COMMENT ON COLUMN scouter.ai_evaluations.previous_run_date IS '前回評価の実行日（再利用時）';
