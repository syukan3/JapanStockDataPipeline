-- ============================================================================
-- 外部トリガー基盤: Supabase pg_cron → GitHub workflow_dispatch
-- ----------------------------------------------------------------------------
-- 目的: GitHub Actions の schedule(cron) はベストエフォートで起動が最大数時間
--   遅延する。pg_cron は分単位で正確に発火するため、pg_net 経由で GitHub の
--   workflow_dispatch API を叩いて定刻起動させる。
--
-- このマイグレーションは「ディスパッチ基盤」のみを作る（Phase 1 前半）:
--   - 拡張 (pg_cron, pg_net)
--   - ops スキーマ / 監査ログ / ワークフロー manifest
--   - dispatch_github_workflow / dispatch_by_name 関数
--   - dispatch_status ビュー（pg_net 応答の手動確認用）
--
-- 監視・自動再ディスパッチ（reconcile / check_freshness / check_cron_health /
--   notify）と cron.schedule() の登録は別マイグレーション（Phase 5 / cutover）。
--
-- 秘密情報（GitHub PAT）は Vault に手動投入する。本ファイルには含めない:
--   select vault.create_secret('<PAT>', 'github_dispatch_pat',
--                              'fine-grained PAT for workflow_dispatch (Actions:RW, 2 repos)');
-- ============================================================================

-- 1. 拡張（Supabase では Dashboard > Database > Extensions でも有効化可）
CREATE EXTENSION IF NOT EXISTS pg_net;
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- 2. 専用スキーマ（公開しない / anon・authenticated には一切 grant しない）
CREATE SCHEMA IF NOT EXISTS ops;
REVOKE ALL ON SCHEMA ops FROM PUBLIC;

-- 3. ワークフロー manifest（実ファイル名・repo・predicate のソースオブトゥルース）
CREATE TABLE IF NOT EXISTS ops.expected_workflows (
  workflow_file  text PRIMARY KEY,                 -- 例: 'cron-a.yml'（dispatch API の {workflow_id}）
  repo           text NOT NULL,                    -- 'JapanStockDataPipeline' | 'JapanStockScouter'
  friendly_name  text NOT NULL,
  ref            text NOT NULL DEFAULT 'main',
  schedule_utc   text NOT NULL,                    -- cron 式（ドキュメント用。実 schedule は cron.schedule）
  kind           text NOT NULL CHECK (kind IN ('daily','weekday','monthly','weekly')),
  deadline_jst   text,                             -- 'HH:MM' freshness 判定の締切（Phase 5 で使用）
  job_name       text,                             -- 監視に使う job_runs.job_name（無い場合 NULL）
  enabled        boolean NOT NULL DEFAULT false,   -- pg_cron からの dispatch を許可するか（カナリア用トグル）
  notes          text
);
COMMENT ON TABLE ops.expected_workflows IS 'pg_cron 外部トリガー対象ワークフローの manifest（実ファイル名・predicate）';

-- 4. ディスパッチ監査ログ（秘密・ヘッダは記録しない）
CREATE TABLE IF NOT EXISTS ops.workflow_dispatch_log (
  id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  repo            text NOT NULL,
  workflow_file   text NOT NULL,
  dispatch_label  text,
  net_request_id  bigint,                          -- net.http_post の戻り値（応答突合キー）
  status_code     int,                             -- reconcile が後で埋める（204=成功）
  dispatched_at   timestamptz NOT NULL DEFAULT now(),
  reconciled_at   timestamptz
);
CREATE INDEX IF NOT EXISTS idx_workflow_dispatch_log_dispatched_at
  ON ops.workflow_dispatch_log (dispatched_at DESC);
CREATE INDEX IF NOT EXISTS idx_workflow_dispatch_log_net_request_id
  ON ops.workflow_dispatch_log (net_request_id);

-- ============================================================================
-- 5. dispatch_github_workflow: GitHub workflow_dispatch を1回叩く
--    SECURITY DEFINER + search_path='' + 完全修飾（search_path injection 防止）
-- ============================================================================
CREATE OR REPLACE FUNCTION ops.dispatch_github_workflow(
  p_repo          text,
  p_workflow_file text,
  p_ref           text  DEFAULT 'main',
  p_inputs        jsonb DEFAULT '{}'::jsonb,
  p_label         text  DEFAULT NULL
) RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_pat        text;
  v_inputs     jsonb;
  v_body       jsonb;
  v_request_id bigint;
BEGIN
  SELECT decrypted_secret INTO v_pat
  FROM vault.decrypted_secrets
  WHERE name = 'github_dispatch_pat';

  IF v_pat IS NULL THEN
    RAISE EXCEPTION 'github_dispatch_pat not found in vault';
  END IF;

  v_inputs := COALESCE(p_inputs, '{}'::jsonb);
  IF p_label IS NOT NULL THEN
    v_inputs := v_inputs || jsonb_build_object('dispatch_label', p_label);
  END IF;

  -- workflow_dispatch の inputs は workflow 側で宣言済みのキーのみ許可（未宣言は 422）。
  -- dispatch_label は P0-d で全 workflow に追加済み。
  v_body := jsonb_build_object('ref', p_ref, 'inputs', v_inputs);

  -- 成功時 GitHub は 204 No Content（body 無し）を返す。run_id は返らないため
  -- 相関は dispatch_label で取る。
  SELECT net.http_post(
    url     := 'https://api.github.com/repos/syukan3/' || p_repo
               || '/actions/workflows/' || p_workflow_file || '/dispatches',
    body    := v_body,
    headers := jsonb_build_object(
      'Authorization',        'Bearer ' || v_pat,
      'Accept',               'application/vnd.github+json',
      'X-GitHub-Api-Version', '2022-11-28',
      'User-Agent',           'japanstock-pgcron-dispatcher',  -- 無いと GitHub は 403
      'Content-Type',         'application/json'
    ),
    timeout_milliseconds := 10000
  ) INTO v_request_id;

  INSERT INTO ops.workflow_dispatch_log (repo, workflow_file, dispatch_label, net_request_id)
  VALUES (p_repo, p_workflow_file, p_label, v_request_id);

  RETURN v_request_id;
END;
$$;

-- ============================================================================
-- 6. dispatch_by_name: manifest を引いて当日 JST ラベル付きで dispatch
--    cron.schedule からはこれを呼ぶ: select ops.dispatch_by_name('cron-c.yml');
--    enabled=false のものは no-op（NULL を返す）= カナリア用の安全トグル
-- ============================================================================
CREATE OR REPLACE FUNCTION ops.dispatch_by_name(p_workflow_file text)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_repo    text;
  v_ref     text;
  v_enabled boolean;
  v_label   text;
BEGIN
  SELECT repo, ref, enabled INTO v_repo, v_ref, v_enabled
  FROM ops.expected_workflows
  WHERE workflow_file = p_workflow_file;

  IF v_repo IS NULL THEN
    RAISE EXCEPTION 'workflow % not registered in ops.expected_workflows', p_workflow_file;
  END IF;

  IF NOT v_enabled THEN
    RAISE NOTICE 'workflow % is disabled in ops.expected_workflows; skipping dispatch', p_workflow_file;
    RETURN NULL;
  END IF;

  v_label := to_char(timezone('Asia/Tokyo', now()), 'YYYY-MM-DD') || ':' || p_workflow_file;

  RETURN ops.dispatch_github_workflow(v_repo, p_workflow_file, v_ref, '{}'::jsonb, v_label);
END;
$$;

-- 7. dispatch_status: pg_net 応答の手動確認用（canary 検証で使う）
--    pg_net の応答は unlogged・約6時間 TTL のため、永続化は reconcile（Phase 5）で行う。
CREATE OR REPLACE VIEW ops.dispatch_status AS
SELECT
  l.id,
  l.repo,
  l.workflow_file,
  l.dispatch_label,
  l.net_request_id,
  l.dispatched_at,
  r.status_code,
  r.error_msg,
  r.timed_out
FROM ops.workflow_dispatch_log l
LEFT JOIN net._http_response r ON r.id = l.net_request_id
ORDER BY l.dispatched_at DESC;

-- ============================================================================
-- 8. 権限: 最小権限。
--    - 低レベルの dispatch_github_workflow は PUBLIC からも service_role からも
--      REVOKE（owner=postgres のみ実行可）。manifest 外の repo/workflow を
--      service_role 経由で dispatch する口を作らない。
--      pg_cron / dispatch_by_name は SECURITY DEFINER で内部から owner として
--      呼ぶため、呼び出し側に EXECUTE 権限は不要。
--    - 外部に公開するのは manifest を引く dispatch_by_name のみ。
--    - カットオーバーの enabled 切替（UPDATE ops.expected_workflows）と
--      Vault 投入は postgres ロール（supabase CLI / SQL Editor）で実施する前提。
--      service_role には SELECT のみ付与（誤操作・横展開を防ぐ）。
-- ============================================================================
REVOKE ALL ON FUNCTION ops.dispatch_github_workflow(text, text, text, jsonb, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION ops.dispatch_by_name(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ops.dispatch_by_name(text) TO service_role;
GRANT USAGE ON SCHEMA ops TO service_role;
GRANT SELECT ON ops.expected_workflows, ops.workflow_dispatch_log TO service_role;
GRANT SELECT ON ops.dispatch_status TO service_role;

-- ============================================================================
-- 9. manifest seed（enabled=false で投入。カナリア時に個別 true 化）
--    deadline_jst / job_name は Phase 5 の freshness 判定で使用。
-- ============================================================================
INSERT INTO ops.expected_workflows
  (workflow_file, repo, friendly_name, schedule_utc, kind, deadline_jst, job_name, notes)
VALUES
  ('cron-c.yml',                    'JapanStockDataPipeline', 'Cron C - Investor Types',   '10 3 * * *',     'daily',   '12:40', 'cron_c',                    'カナリア候補・メール無し'),
  ('cron-e.yml',                    'JapanStockDataPipeline', 'Cron E - Yutai Sync',       '30 7 * * *',     'daily',   '17:00', 'cron-e-yutai',              'メール有・target_date null 想定'),
  ('yutai-cross-screening.yml',     'JapanStockScouter',      'Yutai Cross Screening',     '0 8 * * *',      'daily',   '17:30', 'scouter-yutai-cross',       NULL),
  ('cron-a.yml',                    'JapanStockDataPipeline', 'Cron A - Daily Data Sync',  '40 9 * * *',     'daily',   '19:10', 'cron_a',                    'equity_bars 鮮度も確認'),
  ('cron-b.yml',                    'JapanStockDataPipeline', 'Cron B - Earnings Calendar','20 10 * * *',    'daily',   '19:40', 'cron_b',                    NULL),
  ('macro-regime.yml',              'JapanStockScouter',      'Macro Regime',              '45 10 * * 1-5',  'weekday', '19:55', 'scouter-macro-regime',      NULL),
  ('high-dividend-screening.yml',   'JapanStockScouter',      'High Dividend Screening',   '50 10 * * 1-5',  'weekday', '20:10', 'scouter-high-dividend',     'メール有'),
  ('macro-ai-evaluation.yml',       'JapanStockScouter',      'Macro AI Evaluation',       '50 10 * * 1-5',  'weekday', '20:10', 'scouter-macro-ai',          'メール有'),
  ('cron-d.yml',                    'JapanStockDataPipeline', 'Cron D - Macro Data',       '0 22 * * *',     'daily',   '07:30', 'cron-d-macro',              'target_date null 想定'),
  ('db-archival.yml',               'JapanStockDataPipeline', 'DB Archival',               '0 18 * * 6',     'weekly',  NULL,    'db-archival',               '閾値未満は正常 no-op（行なし）'),
  ('macro-ai-meta-evaluation.yml',  'JapanStockScouter',      'Macro AI Meta Evaluation',  '0 12 1,16 * *',  'monthly', NULL,    'scouter-macro-ai-meta',     '00056 で CHECK 制約追加済'),
  ('signal-performance-eval.yml',   'JapanStockScouter',      'Signal Performance Eval',   '0 1 1 * *',      'monthly', NULL,    'scouter-signal-performance',NULL)
ON CONFLICT (workflow_file) DO NOTHING;
