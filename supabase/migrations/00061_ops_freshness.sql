-- ============================================================================
-- Phase 5b: データ未達検知 check_freshness
-- ----------------------------------------------------------------------------
-- enabled なワークフローについて、当日(JST)そのジョブの成功 job_runs が締切
-- (deadline_jst)を過ぎても存在しなければ「データ未達」として記録＋通知する。
--   ・pg_cron/フォールバック両方の失敗、またはワークフロー内部エラーを捕捉
--   ・reconcile/cron_health(=dispatch層の健全性)より深い「実際に処理されたか」の層
--   ・通知が届かなくても freshness_alert_log に記録が残る(queryable)
--
-- 判定は job_runs.started_at を Asia/Tokyo 基準で当日判定（target_date の意味が
-- ジョブ毎に異なる・null もあるため started_at を使う）。daily/weekday のみ対応
-- （monthly/weekly は頻度が低く別途）。
-- ============================================================================

-- deadline_jst は 'HH:MM' 前提。不正値で check_freshness が毎時失敗しないよう制約を追加
ALTER TABLE ops.expected_workflows
  DROP CONSTRAINT IF EXISTS expected_workflows_deadline_jst_chk;
ALTER TABLE ops.expected_workflows
  ADD CONSTRAINT expected_workflows_deadline_jst_chk
  CHECK (deadline_jst IS NULL OR deadline_jst ~ '^([01][0-9]|2[0-3]):[0-5][0-9]$');

-- 当日・ワークフロー単位の重複通知抑制
CREATE TABLE IF NOT EXISTS ops.freshness_alert_log (
  workflow_file text,
  check_date    date,
  alerted_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (workflow_file, check_date)
);
REVOKE ALL ON TABLE ops.freshness_alert_log FROM PUBLIC;

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
    -- weekday ジョブは土日はスキップ
    IF w.kind = 'weekday' AND v_dow IN (0, 6) THEN
      CONTINUE;
    END IF;
    -- 締切(JST)前ならスキップ
    IF v_time < w.deadline_jst::time THEN
      CONTINUE;
    END IF;
    -- 当日すでに通知済みならスキップ
    IF EXISTS (
      SELECT 1 FROM ops.freshness_alert_log f
       WHERE f.workflow_file = w.workflow_file AND f.check_date = v_today
    ) THEN
      CONTINUE;
    END IF;
    -- 当日(JST)の成功 job_runs があるか
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

-- 毎時 :20 に実行（各ワークフローの deadline_jst を過ぎたものだけ判定・当日1回通知）
SELECT cron.schedule('ops-freshness', '20 * * * *', 'SELECT ops.check_freshness()');
