-- 00070_create_calibration_snapshots.sql
-- LLM評価（AI定性評価）のconfidence定量較正スナップショット。
-- Brierスコア・確信度バケット別的中率をコードで計算し、メタ評価2種
-- （macro-ai-meta / signal-performance-meta）のプロンプトへ実測値として注入する。
-- 書き込みは Scouter（メタ評価の前段 + calibration:backfill CLI）。
-- 詳細: JapanStockScouter/PLANS-llm-self-improvement.md §3 Phase 1

CREATE TABLE scouter.calibration_snapshots (
  id              BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  eval_date       DATE NOT NULL,
  source          TEXT NOT NULL CHECK (source IN ('hd_ai', 'macro_ai')),
  horizon_days    SMALLINT NOT NULL,           -- 実現リターンのホライズン（21 / 63 営業日）
  sample_count    INTEGER NOT NULL,            -- 突合できた予測×実績のサンプル数（neutral・skipped除外後）
  brier_score     NUMERIC(6,4),                -- Brierスコア（小さいほど良い。サンプル0時はNULL）
  baseline_brier  NUMERIC(6,4),                -- 常に p=0.5 と予測した場合の参照値（=0.25）
  buckets         JSONB NOT NULL DEFAULT '[]', -- confidence 10刻みバケット別の件数・平均confidence・的中率
  mean_confidence NUMERIC(5,2),                -- サンプル全体の平均confidence（0-100）
  hit_rate        NUMERIC(5,4),                -- 方向的中率（0-1）
  model_id        TEXT,                        -- 予測を生成したモデルID（混在期はNULL）
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (eval_date, source, horizon_days)
);

COMMENT ON TABLE scouter.calibration_snapshots IS
  'AI評価confidenceの定量較正スナップショット（Brier・バケット別的中率）。メタ評価プロンプトへ実測値として注入する';
COMMENT ON COLUMN scouter.calibration_snapshots.source IS
  'hd_ai=高配当AI評価(ai_evaluations×signal_performance。BUY通過銘柄のみで選択バイアスあり) / macro_ai=マクロAI評価(macro_ai_evaluations×TOPIX実績)';
COMMENT ON COLUMN scouter.calibration_snapshots.horizon_days IS '実現リターンの評価ホライズン（営業日。21=約1ヶ月 / 63=約3ヶ月）';
COMMENT ON COLUMN scouter.calibration_snapshots.brier_score IS 'Brierスコア = mean((p - o)^2)。p=confidence由来の「超過リターン>0」確率、o=実現方向';
COMMENT ON COLUMN scouter.calibration_snapshots.baseline_brier IS '無情報ベースライン（常に p=0.5 と予測した場合）のBrierスコア';
COMMENT ON COLUMN scouter.calibration_snapshots.buckets IS
  'confidence帯別の較正表（reliability curve相当）。[{bucketLow, bucketHigh, count, meanConfidence, hitRate}, ...]';
COMMENT ON COLUMN scouter.calibration_snapshots.hit_rate IS '方向的中率。up予測は実現リターン>0、down予測は<0で的中（0はミス扱い）';

CREATE INDEX idx_calibration_snapshots_latest
  ON scouter.calibration_snapshots (source, eval_date DESC);

-- ============================================================
-- updated_at トリガー（既存 scouter.set_updated_at を再利用）
-- ============================================================
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname = 'trg_calibration_snapshots_updated_at'
      AND tgrelid = 'scouter.calibration_snapshots'::regclass
  ) THEN
    CREATE TRIGGER trg_calibration_snapshots_updated_at
      BEFORE UPDATE ON scouter.calibration_snapshots
      FOR EACH ROW EXECUTE FUNCTION scouter.set_updated_at();
  END IF;
END
$$;

-- ============================================================
-- 権限（プライベートRLSパターン: service_role のみ。00062/00064 と同方針）
-- ============================================================

ALTER TABLE scouter.calibration_snapshots ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'scouter' AND tablename = 'calibration_snapshots'
      AND policyname = 'service_role_all'
  ) THEN
    CREATE POLICY "service_role_all" ON scouter.calibration_snapshots
      FOR ALL TO service_role USING (true) WITH CHECK (true);
  END IF;
END
$$;

-- 00016 の「ALTER DEFAULT PRIVILEGES IN SCHEMA scouter GRANT SELECT ON TABLES TO authenticated」
-- により、本テーブル作成時点で authenticated へ自動的に SELECT が付与される。
-- 内部運用データのため、authenticated/anon には公開しないよう明示的に剥奪する
-- （Portfolio で表示する場合は service_role + cachedRef 経由で読む）。
REVOKE ALL ON scouter.calibration_snapshots FROM anon, authenticated;
