-- 00123_create_macro_pair_rotation_reports.sql の手動ロールバック。
-- immutable report / outbox履歴を削除するため、本番ではmanifest停止・退避確認後だけ実行する。

BEGIN;

LOCK TABLE jquants_ingest.job_runs IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM jquants_ingest.job_runs jr
    WHERE jr.job_name = 'scouter-macro-pair-rotation-report'
      AND jr.status = 'running'
  ) THEN
    RAISE EXCEPTION 'macro pair rotation report is still running; drain it before rollback';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM scouter.macro_pair_rotation_email_outbox o
    WHERE o.status = 'sending'
  ) THEN
    RAISE EXCEPTION 'macro pair rotation email is still sending; reconcile or isolate it before rollback';
  END IF;
END;
$$;

-- Own cron entries only. Unknown owners/contracts are never overwritten or deleted.
DO $$
DECLARE
  v_schedule TEXT;
  v_command TEXT;
BEGIN
  SELECT j.schedule, j.command INTO v_schedule, v_command
  FROM cron.job j
  WHERE j.jobname = 'dispatch-macro-pair-rotation-report';

  IF FOUND THEN
    IF v_schedule <> '30 1 * * 6'
       OR btrim(v_command) <> 'SELECT ops.dispatch_by_name(''macro-pair-rotation-report.yml'')' THEN
      RAISE EXCEPTION 'dispatch-macro-pair-rotation-report cron is owned by a different contract';
    END IF;
    PERFORM cron.unschedule('dispatch-macro-pair-rotation-report');
  END IF;

  SELECT j.schedule, j.command INTO v_schedule, v_command
  FROM cron.job j
  WHERE j.jobname = 'ops-macro-pair-rotation-freshness';

  IF FOUND THEN
    IF v_schedule <> '15 2 * * 6'
       OR btrim(v_command) <> 'SELECT ops.check_macro_pair_rotation_weekly_freshness()' THEN
      RAISE EXCEPTION 'ops-macro-pair-rotation-freshness cron is owned by a different contract';
    END IF;
    PERFORM cron.unschedule('ops-macro-pair-rotation-freshness');
  END IF;
END;
$$;

DO $$
DECLARE
  w ops.expected_workflows%ROWTYPE;
BEGIN
  SELECT * INTO w
  FROM ops.expected_workflows e
  WHERE e.workflow_file = 'macro-pair-rotation-report.yml'
  FOR UPDATE;

  IF FOUND THEN
    IF w.repo <> 'JapanStockScouter'
       OR w.ref <> 'main'
       OR w.friendly_name <> 'Weekly Macro Pair Rotation Report'
       OR w.schedule_utc <> '30 1 * * 6'
       OR w.kind <> 'weekly'
       OR w.deadline_jst IS NOT NULL
       OR w.job_name <> 'scouter-macro-pair-rotation-report' THEN
      RAISE EXCEPTION 'macro-pair-rotation-report.yml manifest is owned by a different contract';
    END IF;

    UPDATE ops.expected_workflows
    SET enabled = false
    WHERE workflow_file = 'macro-pair-rotation-report.yml';

    DELETE FROM ops.expected_workflows
    WHERE workflow_file = 'macro-pair-rotation-report.yml';
  END IF;
END;
$$;

DELETE FROM ops.freshness_alert_log
WHERE workflow_file = 'macro-pair-rotation-report.yml';

DROP FUNCTION IF EXISTS ops.check_macro_pair_rotation_weekly_freshness();

DROP FUNCTION IF EXISTS scouter.recover_expired_macro_pair_rotation_emails();
DROP FUNCTION IF EXISTS scouter.reconcile_macro_pair_rotation_email(date, text, text);
DROP FUNCTION IF EXISTS scouter.retry_macro_pair_rotation_email(date, text, text, text, text, integer);
DROP FUNCTION IF EXISTS scouter.finalize_macro_pair_rotation_email(date, uuid, text, text, text);
DROP FUNCTION IF EXISTS scouter.claim_macro_pair_rotation_email(date, text, text, text, text, integer);
DROP FUNCTION IF EXISTS scouter.get_macro_pair_rotation_macro_snapshot(date, date);

DROP TRIGGER IF EXISTS trg_macro_pair_rotation_outbox_no_truncate
  ON scouter.macro_pair_rotation_email_outbox;
DROP TRIGGER IF EXISTS trg_macro_pair_rotation_outbox_no_delete
  ON scouter.macro_pair_rotation_email_outbox;
DROP TRIGGER IF EXISTS trg_macro_pair_rotation_report_no_truncate
  ON scouter.macro_pair_rotation_reports;
DROP TRIGGER IF EXISTS trg_macro_pair_rotation_report_no_update_delete
  ON scouter.macro_pair_rotation_reports;
DROP TRIGGER IF EXISTS trg_macro_pair_rotation_create_outbox
  ON scouter.macro_pair_rotation_reports;

DROP FUNCTION IF EXISTS scouter.reject_macro_pair_rotation_outbox_delete();
DROP FUNCTION IF EXISTS scouter.reject_macro_pair_rotation_report_mutation();
DROP FUNCTION IF EXISTS scouter.create_macro_pair_rotation_outbox();

DROP TABLE IF EXISTS scouter.macro_pair_rotation_email_outbox;
DROP TABLE IF EXISTS scouter.macro_pair_rotation_reports;

-- 00121時点のallow-listへ戻す。過去のjob_runs監査行を壊さないためNOT VALIDで追加する。
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
    'scouter-yutai-alert',
    'scouter-weekly-summary',
    'weekly-margin',
    'scouter-entry-timing-signal',
    'scouter-thesis-lens',
    'scouter-overheat'
  )) NOT VALID;

COMMIT;
