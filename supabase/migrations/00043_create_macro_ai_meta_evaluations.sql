-- マクロAI メタ評価テーブル: 月2回の品質評価 + プロンプト改善指示

CREATE TABLE IF NOT EXISTS scouter.macro_ai_meta_evaluations (
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  eval_date     DATE NOT NULL UNIQUE,
  period_start  DATE NOT NULL,
  period_end    DATE NOT NULL,
  eval_count    INTEGER NOT NULL,

  -- 6次元品質スコア (0-100)
  repetition_score     INTEGER NOT NULL,
  specificity_score    INTEGER NOT NULL,
  divergence_score     INTEGER NOT NULL,
  contrarian_score     INTEGER NOT NULL,
  delta_score          INTEGER NOT NULL,
  recommendation_score INTEGER NOT NULL,
  overall_score        INTEGER NOT NULL,

  -- 詳細分析
  repetition_analysis     TEXT NOT NULL,
  specificity_analysis    TEXT NOT NULL,
  divergence_analysis     TEXT NOT NULL,
  contrarian_analysis     TEXT NOT NULL,
  delta_analysis          TEXT NOT NULL,
  recommendation_analysis TEXT NOT NULL,

  -- 日次プロンプトに注入する改善指示 (200-500文字)
  prompt_supplement  TEXT NOT NULL,

  -- 改善追跡
  previous_overall_score  INTEGER,
  score_delta             INTEGER,
  improvement_notes       TEXT,

  -- トレーサビリティ
  model_id        TEXT NOT NULL,
  prompt_version  TEXT NOT NULL,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_macro_ai_meta_evals_eval_date
  ON scouter.macro_ai_meta_evaluations (eval_date DESC);

COMMENT ON TABLE scouter.macro_ai_meta_evaluations IS 'マクロAI評価のメタ評価（月2回）: 品質スコア + プロンプト改善指示';
COMMENT ON COLUMN scouter.macro_ai_meta_evaluations.prompt_supplement IS '日次プロンプトに注入する改善指示（200-500文字）';
COMMENT ON COLUMN scouter.macro_ai_meta_evaluations.overall_score IS '6項目の加重平均品質スコア (0-100)';

GRANT SELECT, INSERT, UPDATE ON scouter.macro_ai_meta_evaluations TO service_role;
