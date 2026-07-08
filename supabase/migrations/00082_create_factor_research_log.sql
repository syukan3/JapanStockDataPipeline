-- ファクター自動探索ループの試行ログ
-- PLANS-llm-self-improvement.md（JapanStockScouter）§3 Phase 6
--
-- 「仮説→バックテスト→t値判定」の研究サイクルをLLM主導で回すための試行記録。
-- 事前登録制（LLMが仮説+spec生成 → registeredでinsert → その後にバックテスト実行、
-- の順序をコードで強制）・ホールドアウト封印（直近12ヶ月除外）・多重検定ガード
-- （試行数に応じた採用閾値の引き上げ）はすべてScouter側のコードで強制する。
-- 本テーブルは結果の記録・監査証跡であり、判定ロジックそのものは持たない。

CREATE TABLE IF NOT EXISTS scouter.factor_research_log (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  trial_seq       INTEGER NOT NULL,
  hypothesis      TEXT NOT NULL,
  spec            JSONB NOT NULL,
  status          TEXT NOT NULL DEFAULT 'registered'
                    CHECK (status IN ('registered','tested','rejected','candidate','confirmed','failed_holdout')),
  train_tstat     NUMERIC(8,4),
  train_metrics   JSONB,
  holdout_tstat   NUMERIC(8,4),
  notes           TEXT,
  model_id        TEXT,
  registered_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  decided_at      TIMESTAMPTZ,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (trial_seq)
);

CREATE INDEX IF NOT EXISTS idx_frl_status ON scouter.factor_research_log (status);
CREATE INDEX IF NOT EXISTS idx_frl_trial_seq ON scouter.factor_research_log (trial_seq DESC);

-- ============================================================
-- RLS（プライベートパターン: service_role のみ。00070/00071/00072/00080/00081 と同方針）
-- 00016 のデフォルト権限が authenticated へ自動SELECT付与するため REVOKE 必須。
-- ============================================================
ALTER TABLE scouter.factor_research_log ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'scouter' AND tablename = 'factor_research_log'
      AND policyname = 'service_role_all'
  ) THEN
    CREATE POLICY "service_role_all" ON scouter.factor_research_log
      FOR ALL TO service_role USING (true) WITH CHECK (true);
  END IF;
END
$$;

REVOKE ALL ON scouter.factor_research_log FROM anon, authenticated;

-- ============================================================
-- コメント
-- ============================================================
COMMENT ON TABLE scouter.factor_research_log IS
  'ファクター自動探索ループ（手動CLI, npm run factor-research）の試行ログ。事前登録制・ホールドアウト封印・多重検定ガードで偽発見の量産を防ぐ';
COMMENT ON COLUMN scouter.factor_research_log.trial_seq IS '通し番号（1始まり）。多重検定ガードのtotal_trials算出にも使う';
COMMENT ON COLUMN scouter.factor_research_log.spec IS
  'CLI引数に写像可能なキーのみ許可: factors/freq/eval_freq/band_buy/band_hold/top_fraction/min_turnover/neutralize。weightsは禁止（重み付き合成は現行backtest:factorエンジンに未実装のため）';
COMMENT ON COLUMN scouter.factor_research_log.status IS
  'registered=LLM生成直後(バックテスト未実行) / tested=train実行済み・閾値未達 / rejected=spec不正で登録段階で却下 / candidate=train |t|>=required_t、ホールドアウト検証待ち / confirmed=ホールドアウト合格 / failed_holdout=ホールドアウト不合格';
COMMENT ON COLUMN scouter.factor_research_log.train_tstat IS 'trainバックテスト（直近12ヶ月を除外した期間）のexcessTStatNet';
COMMENT ON COLUMN scouter.factor_research_log.holdout_tstat IS 'candidateのみ算出。直近12ヶ月（ホールドアウト期間）単独のexcessTStatNet';
COMMENT ON COLUMN scouter.factor_research_log.notes IS 'reject理由・validation失敗理由・棄却済みシードの背景等';
