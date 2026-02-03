-- AI定性評価テーブル（Gemini Search Grounding）
CREATE TABLE scouter.ai_evaluations (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  run_date        DATE NOT NULL,
  local_code      TEXT NOT NULL,
  sentiment       TEXT NOT NULL CHECK (sentiment IN ('positive','neutral','negative')),
  news_priced_in  BOOLEAN NOT NULL,
  risk_flags      JSONB NOT NULL DEFAULT '[]',
  confidence      SMALLINT NOT NULL CHECK (confidence BETWEEN 0 AND 100),
  summary         TEXT NOT NULL,
  key_news        JSONB NOT NULL DEFAULT '[]',
  model_id        TEXT NOT NULL,
  skipped         BOOLEAN NOT NULL DEFAULT FALSE,
  previous_run_date DATE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (run_date, local_code)
);

CREATE INDEX idx_ai_eval_lookup
  ON scouter.ai_evaluations (local_code, run_date DESC);

-- RLS有効化（ポリシーなし = anon/authenticatedからアクセス不可、service_roleのみ）
ALTER TABLE scouter.ai_evaluations ENABLE ROW LEVEL SECURITY;

-- updated_at自動更新トリガー
CREATE TRIGGER trg_ai_eval_updated_at
  BEFORE UPDATE ON scouter.ai_evaluations
  FOR EACH ROW EXECUTE FUNCTION scouter.set_updated_at();
