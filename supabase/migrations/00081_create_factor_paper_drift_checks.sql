-- ファクターポートフォリオ・ペーパートレードの自己監視（乖離検知）
-- PLANS-llm-self-improvement.md（JapanStockScouter）§3 Phase 5
--
-- factor-paper（value+lowvol+payout 四半期 0.1/0.4, 実資金なし）の実測が
-- バックテスト期待分布（PLANS-longonly-net.md §4 #6の合意値: ネット年率21.41%・
-- Sharpe1.79・実測片道回転率21.9%/期）から外れていないかを日次で記録する。
-- 判定（ok/warn/alert）はScouter側でコード・決定的に行い、alert時のみLLM原因分析を
-- llm_analysisに格納する。新規cronは作らず factor-paper:daily の末尾ステップから
-- upsert（onConflict=as_of_date）される想定。

CREATE TABLE IF NOT EXISTS scouter.factor_paper_drift_checks (
  id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  as_of_date        DATE NOT NULL,
  rebalance_date    DATE NOT NULL,
  days_elapsed      INTEGER NOT NULL,
  realized_return   NUMERIC(10,6) NOT NULL,
  expected_return   NUMERIC(10,6) NOT NULL,
  z_score           NUMERIC(8,4),
  realized_turnover NUMERIC(6,4),
  verdict           TEXT NOT NULL CHECK (verdict IN ('ok','warn','alert')),
  llm_analysis      TEXT,
  model_id          TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (as_of_date)
);

CREATE INDEX IF NOT EXISTS idx_fpdc_latest
  ON scouter.factor_paper_drift_checks (as_of_date DESC);
CREATE INDEX IF NOT EXISTS idx_fpdc_alerts
  ON scouter.factor_paper_drift_checks (as_of_date DESC) WHERE verdict = 'alert';

-- ============================================================
-- updated_at トリガー（既存 scouter.set_updated_at を再利用。日次upsertでの再実行に対応）
-- ============================================================
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname = 'trg_fpdc_updated_at'
      AND tgrelid = 'scouter.factor_paper_drift_checks'::regclass
  ) THEN
    CREATE TRIGGER trg_fpdc_updated_at
      BEFORE UPDATE ON scouter.factor_paper_drift_checks
      FOR EACH ROW EXECUTE FUNCTION scouter.set_updated_at();
  END IF;
END
$$;

-- ============================================================
-- RLS（プライベートパターン: service_role のみ。00070/00071/00072/00080 と同方針）
-- 00016 のデフォルト権限が authenticated へ自動SELECT付与するため REVOKE 必須。
-- ============================================================
ALTER TABLE scouter.factor_paper_drift_checks ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'scouter' AND tablename = 'factor_paper_drift_checks'
      AND policyname = 'service_role_all'
  ) THEN
    CREATE POLICY "service_role_all" ON scouter.factor_paper_drift_checks
      FOR ALL TO service_role USING (true) WITH CHECK (true);
  END IF;
END
$$;

REVOKE ALL ON scouter.factor_paper_drift_checks FROM anon, authenticated;

-- ============================================================
-- コメント
-- ============================================================
COMMENT ON TABLE scouter.factor_paper_drift_checks IS
  'factor-paper（ペーパートレード, 実資金なし）の実測 vs バックテスト期待の乖離監視ログ。alert時のみllm_analysisを格納';
COMMENT ON COLUMN scouter.factor_paper_drift_checks.rebalance_date IS
  '評価対象コホートのリバランス日（factor_portfolio_holdingsとのJOINキー）';
COMMENT ON COLUMN scouter.factor_paper_drift_checks.days_elapsed IS
  'rebalance_dateからas_of_dateまでの経過営業日数。10営業日未満はz値判定をスキップしokとする';
COMMENT ON COLUMN scouter.factor_paper_drift_checks.expected_return IS
  'バックテスト四半期リターン分布（PLANS-longonly-net.md §4 #6, ネット年率21.41%・Sharpe1.79）から経過日数按分した期待リターン';
COMMENT ON COLUMN scouter.factor_paper_drift_checks.z_score IS
  '(realized_return - expected_return) / 経過日数按分std。|z|>=2:alert, |z|>=1.5:warn';
COMMENT ON COLUMN scouter.factor_paper_drift_checks.realized_turnover IS
  'リバランス日のみ記録される実測片道回転率。baseline(21.9%/期)の2倍超は理由を問わず強制alert。非リバランス日はNULL';
COMMENT ON COLUMN scouter.factor_paper_drift_checks.verdict IS
  'ok/warn/alert。現行configがbaseline照合キーと不一致の場合は行自体を記録せずスキップする（Scouter側drift-check.tsの実装）';
COMMENT ON COLUMN scouter.factor_paper_drift_checks.llm_analysis IS
  'alert確定時のみGemini（既定, FACTOR_PAPER_DRIFT_MODEL envでclaude切替可）が生成する原因仮説。ok/warnはNULL';
