-- 00087_register_analyst_target_monitor.sql
-- Analyst Target Monitorを外部トリガーmanifestとpg_cronへ安全側で登録する。
-- GitHub scheduleで運用する間はenabled=falseのため、pg_cronはdispatchしない。
-- pg_cronへcutoverする場合だけ、先にGitHub scheduleを削除してからenabled=trueにする。
-- 同名cronが既に存在する場合は、他migrationの所有物を上書きしないようfail closedにする。

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM cron.job j
    WHERE j.jobname = 'dispatch-analyst-target-monitor'
  ) THEN
    RAISE EXCEPTION 'cron job dispatch-analyst-target-monitor already exists';
  END IF;
END;
$$;

INSERT INTO ops.expected_workflows (
  workflow_file,
  repo,
  friendly_name,
  schedule_utc,
  kind,
  deadline_jst,
  job_name,
  enabled,
  notes
)
VALUES (
  'analyst-target-monitor.yml',
  'JapanStockScouter',
  'Analyst Target Monitor',
  '25 12 * * 1-5',
  'weekday',
  '22:00',
  'scouter-analyst-target-monitor',
  false,
  'GitHub schedule運用中はfreshness対象外。cutover前にGitHub scheduleを削除してからenabled=trueにする。'
);

SELECT cron.schedule(
  'dispatch-analyst-target-monitor',
  '25 12 * * 1-5',
  $$ SELECT ops.dispatch_by_name('analyst-target-monitor.yml') $$
);
