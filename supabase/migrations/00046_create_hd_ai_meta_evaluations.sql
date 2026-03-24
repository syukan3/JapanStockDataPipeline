-- 高配当AI評価 メタ評価テーブル: 月次の予測精度分析 + プロンプト改善指示

CREATE TABLE IF NOT EXISTS scouter.hd_ai_meta_evaluations (
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  eval_date     DATE NOT NULL UNIQUE,
  period_start  DATE NOT NULL,
  period_end    DATE NOT NULL,
  signal_count  INTEGER NOT NULL,
  matched_count INTEGER NOT NULL,

  -- 5次元品質スコア (0-100)
  sentiment_accuracy_score       INTEGER NOT NULL,
  confidence_calibration_score   INTEGER NOT NULL,
  risk_detection_score           INTEGER NOT NULL,
  sector_quality_score           INTEGER NOT NULL,
  recommendation_precision_score INTEGER NOT NULL,
  overall_score                  INTEGER NOT NULL,

  -- 詳細分析
  sentiment_accuracy_analysis       TEXT NOT NULL,
  confidence_calibration_analysis   TEXT NOT NULL,
  risk_detection_analysis           TEXT NOT NULL,
  sector_quality_analysis           TEXT NOT NULL,
  recommendation_precision_analysis TEXT NOT NULL,

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

CREATE INDEX IF NOT EXISTS idx_hd_ai_meta_evals_eval_date
  ON scouter.hd_ai_meta_evaluations (eval_date DESC);

COMMENT ON TABLE scouter.hd_ai_meta_evaluations IS '高配当AI評価のメタ評価（月次）: AI予測精度 vs 実績リターンの分析 + プロンプト改善指示';
COMMENT ON COLUMN scouter.hd_ai_meta_evaluations.prompt_supplement IS '日次AI評価プロンプトに注入する改善指示（200-500文字）';
COMMENT ON COLUMN scouter.hd_ai_meta_evaluations.overall_score IS '5項目の加重平均品質スコア (0-100)';
COMMENT ON COLUMN scouter.hd_ai_meta_evaluations.matched_count IS 'signal_performance と ai_evaluations の両方にデータがあるレコード数';

GRANT SELECT, INSERT, UPDATE ON scouter.hd_ai_meta_evaluations TO service_role;
