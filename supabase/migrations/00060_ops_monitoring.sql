-- ============================================================================
-- Phase 5a: 外部トリガー機構の監視（reconcile / check_cron_health / notify）
-- ----------------------------------------------------------------------------
-- 目的: pg_cron→workflow_dispatch 機構「自体」の健全性を監視する。
--   - notify()               : Resend 経由でアラートメール（email.ts と同じ送信元）
--   - reconcile_dispatches() : pg_net 応答を workflow_dispatch_log に永続化(TTL約6h対策)
--                              し、204以外 / 無応答 を検知して通知
--   - check_cron_health()    : pg_cron ジョブ自体の失敗(cron.job_run_details)を通知
--
-- データ未達検知(check_freshness)は、フォールバック(GitHub schedule)を剥がす
-- カットオーバーと同時に別途導入する（フォールバックがある間は締切時点で未達に
-- 見える＝誤検知になるため）。
--
-- 必要な Vault secret（手動投入）:
--   select vault.create_secret('<RESEND_API_KEY>', 'resend_api_key', 'alert email via Resend');
--   select vault.create_secret('<ALERT_EMAIL_TO>', 'alert_email_to', 'alert recipient');
--   -- 任意: 検証済み送信元を使う場合
--   select vault.create_secret('MyApp <alerts@example.com>', 'alert_email_from', 'verified sender');
-- ============================================================================

-- 監査ログに応答詳細＋アラート済みフラグを追加
ALTER TABLE ops.workflow_dispatch_log
  ADD COLUMN IF NOT EXISTS alerted_at         timestamptz,
  ADD COLUMN IF NOT EXISTS response_timed_out boolean,
  ADD COLUMN IF NOT EXISTS response_error_msg text;

-- pg_cron ジョブ失敗の重複通知抑制（runid 単位）
CREATE TABLE IF NOT EXISTS ops.cron_alert_log (
  runid      bigint PRIMARY KEY,
  alerted_at timestamptz NOT NULL DEFAULT now()
);
REVOKE ALL ON TABLE ops.cron_alert_log FROM PUBLIC;

-- ============================================================================
-- notify: Resend でアラートメール送信。返り値=net request id（NULL=送信キュー投入せず）
-- ============================================================================
CREATE OR REPLACE FUNCTION ops.notify(p_subject text, p_html text)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_key  text;
  v_to   text;
  v_from text;
  v_req  bigint;
BEGIN
  SELECT decrypted_secret INTO v_key  FROM vault.decrypted_secrets WHERE name = 'resend_api_key';
  SELECT decrypted_secret INTO v_to   FROM vault.decrypted_secrets WHERE name = 'alert_email_to';
  SELECT decrypted_secret INTO v_from FROM vault.decrypted_secrets WHERE name = 'alert_email_from';
  IF v_key IS NULL OR v_to IS NULL THEN
    RAISE WARNING 'ops.notify: resend_api_key / alert_email_to が Vault に無いため送信スキップ';
    RETURN NULL;
  END IF;
  v_from := COALESCE(v_from, 'JapanStockDataPipeline <noreply@resend.dev>');

  SELECT net.http_post(
    url     := 'https://api.resend.com/emails',
    body    := jsonb_build_object(
      'from',    v_from,
      'to',      jsonb_build_array(v_to),
      'subject', p_subject,
      'html',    p_html
    ),
    headers := jsonb_build_object(
      'Authorization', 'Bearer ' || v_key,
      'Content-Type',  'application/json'
    ),
    timeout_milliseconds := 10000
  ) INTO v_req;

  RETURN v_req;
END;
$$;

-- ============================================================================
-- reconcile_dispatches: pg_net 応答の永続化 + 失敗検知
--   ・通知がキュー投入できた(戻り値非NULL)ときのみ alerted_at を立てる
--     → Vault未設定/通知不能時は抑制せず次回再試行
-- ============================================================================
CREATE OR REPLACE FUNCTION ops.reconcile_dispatches()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  r      record;
  v_body text := '';
  v_ids  bigint[] := '{}';
BEGIN
  -- 1. 未取得(status_code is null)の応答を net._http_response(約6h TTL)から永続化
  UPDATE ops.workflow_dispatch_log l
     SET status_code        = resp.status_code,
         response_timed_out = resp.timed_out,
         response_error_msg = resp.error_msg,
         reconciled_at      = now()
    FROM net._http_response resp
   WHERE resp.id = l.net_request_id
     AND l.status_code IS NULL;

  -- 2. 未通知の失敗（204以外の応答 / 15分経っても無応答）を集約（永続化列を参照＝TTL非依存）
  FOR r IN
    SELECT l.id, l.repo, l.workflow_file, l.dispatch_label, l.status_code, l.dispatched_at,
           l.response_error_msg, l.response_timed_out
      FROM ops.workflow_dispatch_log l
     WHERE l.alerted_at IS NULL
       AND l.dispatched_at > now() - interval '24 hours'
       AND (
             (l.status_code IS NOT NULL AND l.status_code <> 204)                    -- 応答したが失敗
          OR (l.status_code IS NULL AND l.dispatched_at < now() - interval '15 minutes')  -- 無応答/timeout
           )
  LOOP
    v_body := v_body || format(
      '- %s / %s (%s): status=%s, error=%s, timeout=%s, at %s<br>',
      r.repo, r.workflow_file, COALESCE(r.dispatch_label, '-'),
      COALESCE(r.status_code::text, 'NO-RESPONSE'),
      COALESCE(r.response_error_msg, '-'), COALESCE(r.response_timed_out::text, '-'), r.dispatched_at
    );
    v_ids := array_append(v_ids, r.id);
  END LOOP;

  IF v_body <> '' THEN
    IF ops.notify(
         '[JapanStock] pg_cron dispatch 失敗検知',
         '<p>以下の workflow_dispatch が 204 以外 / 無応答でした（PAT期限・ワークフロー改名・GitHub障害等を確認）:</p>' || v_body
       ) IS NOT NULL THEN
      UPDATE ops.workflow_dispatch_log SET alerted_at = now() WHERE id = ANY(v_ids);
    END IF;
  END IF;
END;
$$;

-- ============================================================================
-- check_cron_health: pg_cron ジョブ自体の失敗を検知（failed のみ・runid単位で重複抑制）
-- ============================================================================
CREATE OR REPLACE FUNCTION ops.check_cron_health()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  r        record;
  v_body   text := '';
  v_runids bigint[] := '{}';
BEGIN
  FOR r IN
    SELECT d.runid, j.jobname, d.status, d.return_message, d.start_time
      FROM cron.job_run_details d
      JOIN cron.job j ON j.jobid = d.jobid
     WHERE (j.jobname LIKE 'dispatch-%' OR j.jobname LIKE 'ops-%')
       AND d.status = 'failed'
       AND d.start_time > now() - interval '25 hours'
       AND NOT EXISTS (SELECT 1 FROM ops.cron_alert_log c WHERE c.runid = d.runid)
  LOOP
    v_body := v_body || format(
      '- %s: %s (%s) at %s<br>',
      r.jobname, r.status, COALESCE(r.return_message, '-'), r.start_time
    );
    v_runids := array_append(v_runids, r.runid);
  END LOOP;

  IF v_body <> '' THEN
    IF ops.notify(
         '[JapanStock] pg_cron ジョブ失敗検知',
         '<p>以下の pg_cron ジョブが失敗しました:</p>' || v_body
       ) IS NOT NULL THEN
      INSERT INTO ops.cron_alert_log(runid) SELECT unnest(v_runids) ON CONFLICT DO NOTHING;
    END IF;
  END IF;
END;
$$;

-- ============================================================================
-- 権限: owner(postgres) のみ。pg_cron からは owner として呼ばれるため付与不要。
-- ============================================================================
REVOKE ALL ON FUNCTION ops.notify(text, text)     FROM PUBLIC;
REVOKE ALL ON FUNCTION ops.reconcile_dispatches() FROM PUBLIC;
REVOKE ALL ON FUNCTION ops.check_cron_health()    FROM PUBLIC;

-- ============================================================================
-- 監視ジョブのスケジュール（reconcile: 15分毎 / cron-health: 毎時5分）
-- ============================================================================
SELECT cron.schedule('ops-reconcile',   '*/15 * * * *', 'SELECT ops.reconcile_dispatches()');
SELECT cron.schedule('ops-cron-health', '5 * * * *',    'SELECT ops.check_cron_health()');

-- 監視導入前の既存 dispatch 行は遡及アラートしない
UPDATE ops.workflow_dispatch_log SET alerted_at = now() WHERE alerted_at IS NULL;
