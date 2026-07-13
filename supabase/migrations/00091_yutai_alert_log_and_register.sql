-- 00091_yutai_alert_log_and_register.sql
-- 優待権利付き最終日アラート(yutai-alert)の実装に伴うDB変更:
--   1. scouter.yutai_alert_log        … (local_code, last_buy_date)ベースの通知dedupログ
--   2. job_runs の job_name CHECK制約 … 'scouter-yutai-alert' を追加
--   3. 外部トリガー登録               … ops.expected_workflows + pg_cron dispatch
-- 詳細: JapanStockScouter/PLANS-event-calendar.md §4.2

-- ============================================================
-- 1. 通知済みログ（保有銘柄コードを含む個人情報のため厳格な権限にする＝00064と同方針）
-- ============================================================

CREATE TABLE scouter.yutai_alert_log (
  id            bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  local_code    text NOT NULL,
  last_buy_date date NOT NULL,           -- 権利付き最終日 L（権利確定日の2営業日前・T+2決済）
  record_month  smallint,               -- 権利確定月（1-12）。参考情報
  status        text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sent', 'failed')),
  sent_at       timestamptz,
  error_message text,
  created_at    timestamptz NOT NULL DEFAULT now(),
  UNIQUE (local_code, last_buy_date)
);

COMMENT ON TABLE scouter.yutai_alert_log IS '優待権利付き最終日アラート(yutai-alert)の通知済みログ。(local_code+last_buy_date)ベースの重複排除に使用';
COMMENT ON COLUMN scouter.yutai_alert_log.last_buy_date IS '権利付き最終日 L（権利確定日の2営業日前・T+2決済）。dedupキー';
COMMENT ON COLUMN scouter.yutai_alert_log.status IS 'pending=DB記録済み未送信 / sent=送信成功 / failed=送信失敗';

CREATE INDEX idx_yutai_alert_log_status ON scouter.yutai_alert_log (status);

ALTER TABLE scouter.yutai_alert_log ENABLE ROW LEVEL SECURITY;

CREATE POLICY "service_role_all" ON scouter.yutai_alert_log
  FOR ALL TO service_role USING (true) WITH CHECK (true);

-- 00016 の「ALTER DEFAULT PRIVILEGES IN SCHEMA scouter GRANT SELECT ON TABLES TO authenticated」
-- により作成時点で authenticated へ自動付与される SELECT を、個人情報保護のため明示的に剥奪する。
REVOKE ALL ON scouter.yutai_alert_log FROM anon, authenticated;

-- ============================================================
-- 2. job_runs の job_name CHECK制約に 'scouter-yutai-alert' を追加
--    （既存慣習に倣い DROP → 全許可名を含めて ADD し直す。最新の全リストは
--     00086 を基準にし、末尾に新規名を足す。これを忘れると job_runs への insert が
--     失敗し、冪等ガード(alreadySucceededToday)と監視が機能しなくなる）
-- ============================================================

ALTER TABLE jquants_ingest.job_runs
  DROP CONSTRAINT IF EXISTS job_runs_job_name_check;

ALTER TABLE jquants_ingest.job_runs
  ADD CONSTRAINT job_runs_job_name_check
  CHECK (job_name IN (
    'cron_a', 'cron_b', 'cron_c',
    'scouter-high-dividend', 'cron-d-macro',
    'scouter-macro-regime', 'scouter-macro-ai',
    'cron-e-yutai', 'scouter-yutai-cross',
    'db-archival', 'scouter-signal-performance',
    'scouter-growth-signal', 'scouter-macro-ai-meta',
    'scouter-holdings-news', 'scouter-earnings-alert',
    'scouter-factor-paper',
    'scouter-price-alert',
    'scouter-earnings-surprise',
    'scouter-analyst-target-monitor',
    'scouter-yutai-alert'
  ));

-- ============================================================
-- 3. 外部トリガー登録（pg_cron → GitHub workflow_dispatch）
--    yutai-alert.yml は dispatch_label input・run-name・concurrency・冪等ガードを
--    実装済みのため、既存fleetと同じく直接 enabled=true とする（GitHub schedule は
--    フォールバックとして残置。00084 の end-state と同方針）。
--    同名cronが既にあれば他migrationの所有物を上書きしないよう fail closed にする。
-- ============================================================

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM cron.job j
    WHERE j.jobname = 'dispatch-yutai-alert'
  ) THEN
    RAISE EXCEPTION 'cron job dispatch-yutai-alert already exists';
  END IF;
END;
$$;

INSERT INTO ops.expected_workflows
  (workflow_file, repo, friendly_name, schedule_utc, kind, deadline_jst, job_name, enabled, notes)
VALUES
  ('yutai-alert.yml', 'JapanStockScouter', 'Yutai Rights Alert Check', '15 11 * * *', 'daily', '20:45', 'scouter-yutai-alert', true, 'メール有・Cron E(優待更新)後')
ON CONFLICT (workflow_file) DO NOTHING;

SELECT cron.schedule(
  'dispatch-yutai-alert',
  '15 11 * * *',
  $$ SELECT ops.dispatch_by_name('yutai-alert.yml') $$
);
