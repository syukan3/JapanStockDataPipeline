-- 00072_create_strategy_params.sql
-- 戦略パラメータのバージョン管理テーブル（Phase 3: tune クローズドループ完成）。
-- - backtest tune が out-of-sample（validation）基準を満たした提案を status='proposed' で永続化する
--   （従来は console 出力のみで揮発していた）。
-- - 人間承認（fn_approve_strategy_params）で active 化し、本番スクリーニング
--   （JapanStockScouter high-dividend screening）の getActiveParams() が参照する。
--   取得失敗・不在・検証不合格時は Scouter 側がハードコード既定値へフォールバックする。
-- - strategy ごとに active は常に1件以下（部分 UNIQUE インデックスで強制）。
-- - 承認/棄却は複数 UPDATE を伴うため、1トランザクションの DB 関数で行う
--   （Supabase JS からは複数 UPDATE をトランザクション化できない — 00071 と同方針）。
-- 詳細: JapanStockScouter/PLANS-llm-self-improvement.md §3 Phase 3

CREATE TABLE scouter.strategy_params (
  id           BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  strategy     TEXT NOT NULL CHECK (strategy IN ('high_dividend')),
  version      INTEGER NOT NULL,
  params       JSONB NOT NULL,
  status       TEXT NOT NULL DEFAULT 'proposed'
               CHECK (status IN ('proposed', 'active', 'rejected', 'retired')),
  evidence     JSONB,                            -- train/validation 双方の hit_rate・mean_return・分割情報等
  reasoning    TEXT,                             -- LLM の提案理由
  model_id     TEXT,                             -- 提案を生成した LLM モデルID
  proposed_at  TIMESTAMPTZ,
  decided_at   TIMESTAMPTZ,                      -- approve/reject/retire の決定日時
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (strategy, version)
);

COMMENT ON TABLE scouter.strategy_params IS
  '戦略パラメータのバージョン管理（tune の OOS 合格提案→人間承認→本番反映）。active は strategy ごとに常に1件以下';
COMMENT ON COLUMN scouter.strategy_params.status IS
  'proposed=tune が提案（承認待ち） / active=本番スクリーニングが使用中 / rejected=人間が棄却 / retired=後続バージョンに交代';
COMMENT ON COLUMN scouter.strategy_params.params IS
  'StrategyParams（min_dividend_yield 等6キー）。Scouter 側で validateParams 検証後に使用（不合格は既定値フォールバック）';
COMMENT ON COLUMN scouter.strategy_params.evidence IS
  'tune の根拠: split・train/validation 期間・baseline/proposed 双方の BUY 21日 hit_rate / mean_return 等';

-- strategy ごとに active は1件のみ（DBレベルで強制）
CREATE UNIQUE INDEX uq_strategy_params_single_active
  ON scouter.strategy_params (strategy)
  WHERE status = 'active';

CREATE INDEX idx_strategy_params_latest
  ON scouter.strategy_params (strategy, version DESC);

-- ============================================================
-- updated_at トリガー（既存 scouter.set_updated_at を再利用）
-- ============================================================
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname = 'trg_strategy_params_updated_at'
      AND tgrelid = 'scouter.strategy_params'::regclass
  ) THEN
    CREATE TRIGGER trg_strategy_params_updated_at
      BEFORE UPDATE ON scouter.strategy_params
      FOR EACH ROW EXECUTE FUNCTION scouter.set_updated_at();
  END IF;
END
$$;

-- ============================================================
-- 権限（プライベートRLSパターン: service_role のみ。00062/00064/00070 と同方針）
-- ============================================================

ALTER TABLE scouter.strategy_params ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'scouter' AND tablename = 'strategy_params'
      AND policyname = 'service_role_all'
  ) THEN
    CREATE POLICY "service_role_all" ON scouter.strategy_params
      FOR ALL TO service_role USING (true) WITH CHECK (true);
  END IF;
END
$$;

-- 00016 の「ALTER DEFAULT PRIVILEGES IN SCHEMA scouter GRANT SELECT ON TABLES TO authenticated」
-- により、本テーブル作成時点で authenticated へ自動的に SELECT が付与される。
-- 本番戦略パラメータのため、authenticated/anon には公開しないよう明示的に剥奪する。
REVOKE ALL ON scouter.strategy_params FROM anon, authenticated;

-- ============================================================
-- 承認/棄却の原子的トランザクション関数
-- approve: 旧 active 退役 → 対象 active 化 → decided_at 更新 を1トランザクションで実行。
-- SECURITY INVOKER（既定）。EXECUTE は service_role のみ（00053/00071 と同方針）。
-- ============================================================

CREATE OR REPLACE FUNCTION scouter.fn_approve_strategy_params(p_id BIGINT)
RETURNS VOID
LANGUAGE plpgsql
SET search_path = ''
AS $$
DECLARE
  v_strategy TEXT;
  v_status TEXT;
BEGIN
  SELECT strategy, status INTO v_strategy, v_status
  FROM scouter.strategy_params
  WHERE id = p_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'strategy_params id % not found', p_id;
  END IF;

  IF v_status <> 'proposed' THEN
    RAISE EXCEPTION 'strategy_params id % must be proposed to approve (got %)', p_id, v_status;
  END IF;

  -- 旧 active を退役（部分UNIQUEと順序でactive一意を維持）
  UPDATE scouter.strategy_params
  SET status = 'retired', decided_at = NOW()
  WHERE strategy = v_strategy AND status = 'active';

  -- 対象を active 化
  UPDATE scouter.strategy_params
  SET status = 'active', decided_at = NOW()
  WHERE id = p_id;
END
$$;

CREATE OR REPLACE FUNCTION scouter.fn_reject_strategy_params(p_id BIGINT)
RETURNS VOID
LANGUAGE plpgsql
SET search_path = ''
AS $$
DECLARE
  v_status TEXT;
BEGIN
  SELECT status INTO v_status
  FROM scouter.strategy_params
  WHERE id = p_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'strategy_params id % not found', p_id;
  END IF;

  IF v_status <> 'proposed' THEN
    RAISE EXCEPTION 'strategy_params id % must be proposed to reject (got %)', p_id, v_status;
  END IF;

  UPDATE scouter.strategy_params
  SET status = 'rejected', decided_at = NOW()
  WHERE id = p_id;
END
$$;

COMMENT ON FUNCTION scouter.fn_approve_strategy_params(BIGINT) IS
  '提案パラメータの承認（旧active退役→対象active化→decided_at）を1トランザクションで実行。active一意は uq_strategy_params_single_active が強制';
COMMENT ON FUNCTION scouter.fn_reject_strategy_params(BIGINT) IS
  '提案パラメータの棄却（proposed→rejected + decided_at）';

REVOKE ALL ON FUNCTION scouter.fn_approve_strategy_params(BIGINT) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION scouter.fn_reject_strategy_params(BIGINT) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION scouter.fn_approve_strategy_params(BIGINT) TO service_role;
GRANT EXECUTE ON FUNCTION scouter.fn_reject_strategy_params(BIGINT) TO service_role;
