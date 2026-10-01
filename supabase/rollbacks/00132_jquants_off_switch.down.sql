-- 00132_jquants_off_switch.down.sql
-- J-Quants ON/OFF スイッチを取り除き、dispatch_by_name / check_freshness を 00057 / 00061 の定義へ戻す。
-- 先に `jquants-toggle.sh on` で ON に戻しておくこと（OFF のまま戻すと、止めていた
-- requires_jquants のワークフローが次の定刻から一斉に動き出す）。

DROP FUNCTION IF EXISTS jquants_ingest.set_data_source_mode(text, boolean, text, text);
DROP FUNCTION IF EXISTS jquants_ingest.get_data_source_mode(text);

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

CREATE OR REPLACE FUNCTION ops.check_freshness()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  w       record;
  v_today date := (timezone('Asia/Tokyo', now()))::date;
  v_time  time := (timezone('Asia/Tokyo', now()))::time;
  v_dow   int  := extract(dow FROM timezone('Asia/Tokyo', now()));  -- 0=Sun .. 6=Sat
  v_ok    boolean;
  v_body  text := '';
  v_files text[] := '{}';
BEGIN
  FOR w IN
    SELECT workflow_file, job_name, kind, deadline_jst
      FROM ops.expected_workflows
     WHERE enabled = true
       AND job_name IS NOT NULL
       AND deadline_jst IS NOT NULL
       AND kind IN ('daily', 'weekday')
  LOOP
    IF w.kind = 'weekday' AND v_dow IN (0, 6) THEN
      CONTINUE;
    END IF;
    IF v_time < w.deadline_jst::time THEN
      CONTINUE;
    END IF;
    IF EXISTS (
      SELECT 1 FROM ops.freshness_alert_log f
       WHERE f.workflow_file = w.workflow_file AND f.check_date = v_today
    ) THEN
      CONTINUE;
    END IF;
    SELECT EXISTS (
      SELECT 1 FROM jquants_ingest.job_runs r
       WHERE r.job_name = w.job_name
         AND r.status = 'success'
         AND (timezone('Asia/Tokyo', r.started_at))::date = v_today
    ) INTO v_ok;

    IF NOT v_ok THEN
      v_body  := v_body || format(
        '- %s (job_name=%s): 当日(%s JST)の成功 job_runs が締切 %s を過ぎても無し<br>',
        w.workflow_file, w.job_name, v_today, w.deadline_jst
      );
      v_files := array_append(v_files, w.workflow_file);
    END IF;
  END LOOP;

  IF v_body <> '' THEN
    IF ops.notify(
         '[JapanStock] データ未達検知 (freshness)',
         '<p>以下のワークフローが当日データを生成できていません（pg_cron/フォールバック両方の失敗、またはワークフロー内部エラーの可能性）:</p>' || v_body
       ) IS NOT NULL THEN
      INSERT INTO ops.freshness_alert_log(workflow_file, check_date)
        SELECT unnest(v_files), v_today
        ON CONFLICT DO NOTHING;
    END IF;
  END IF;
END;
$$;

REVOKE ALL ON FUNCTION ops.check_freshness() FROM PUBLIC;

ALTER TABLE ops.expected_workflows DROP COLUMN IF EXISTS requires_jquants;

DROP FUNCTION IF EXISTS ops.jquants_enabled();
DROP TABLE IF EXISTS ops.data_source_switches;
