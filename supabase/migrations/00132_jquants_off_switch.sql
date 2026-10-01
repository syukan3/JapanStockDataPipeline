-- 00132_jquants_off_switch.sql
-- ============================================================================
-- J-Quants の ON/OFF スイッチ（完全解約しても運用を続けるための土台）
-- ----------------------------------------------------------------------------
-- 設計正本: ../../../docs/PLANS-jquants-off-switch-2026-10.md §3
--
-- 1. ops.data_source_switches: スイッチ本体（provider='jquants' の1行）。
--    3リポ（DataPipeline の GH Actions / Scouter / Portfolio）がここだけを読む。
-- 2. 読み書きは 00125 と同じ橋渡しRPC（jquants_ingest・service_role 専用）。
--    ops スキーマは PostgREST 非公開のまま。
-- 3. ops.expected_workflows.requires_jquants: OFF 中は dispatch も未達検知もしない行。
--    dispatch_by_name / check_freshness を OFF 対応に差し替える（それ以外の挙動は 00057 / 00061 と同一）。
--
-- 既定は ON（現状維持）。この migration を適用しただけでは何も変わらない。
-- ロールバック: supabase/rollbacks/00132_jquants_off_switch.down.sql
-- ============================================================================

-- ============================================================================
-- 1. スイッチ本体
-- ============================================================================

CREATE TABLE IF NOT EXISTS ops.data_source_switches (
  provider                  text PRIMARY KEY CHECK (provider IN ('jquants')),
  enabled                   boolean     NOT NULL,
  changed_at                timestamptz NOT NULL DEFAULT now(),
  changed_by                text,
  reason                    text,
  -- OFF にした時点の equity_bar_daily（session='DAY'）の最大日。
  -- 画面の「○/○時点」と、ON 復帰時の埋め戻し起点（jquants-restore）に使う。ON にしても消さない。
  last_official_trade_date  date
);
COMMENT ON TABLE ops.data_source_switches IS
  '外部データ源の ON/OFF スイッチ（00132）。読み書きは jquants_ingest.get/set_data_source_mode 経由。';

REVOKE ALL ON TABLE ops.data_source_switches FROM PUBLIC, anon, authenticated;

INSERT INTO ops.data_source_switches (provider, enabled, changed_by, reason)
VALUES ('jquants', true, 'migration-00132', '初期値（現状維持）')
ON CONFLICT (provider) DO NOTHING;

-- DB 内部（dispatch / freshness）から使う判定関数。
-- 行が無いときは ON 扱い（スイッチ導入前と同じ挙動。誤って全停止させない）。
CREATE OR REPLACE FUNCTION ops.jquants_enabled()
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = ''
AS $$
  SELECT COALESCE(
    (SELECT s.enabled FROM ops.data_source_switches s WHERE s.provider = 'jquants'),
    true
  )
$$;
REVOKE ALL ON FUNCTION ops.jquants_enabled() FROM PUBLIC, anon, authenticated;

-- ============================================================================
-- 2. manifest に requires_jquants を追加し、J-Quants 依存の行に立てる
-- ============================================================================

ALTER TABLE ops.expected_workflows
  ADD COLUMN IF NOT EXISTS requires_jquants boolean NOT NULL DEFAULT false;
COMMENT ON COLUMN ops.expected_workflows.requires_jquants IS
  'true の行は J-Quants OFF 中に dispatch も未達検知もしない（00132）。混在ワークフロー（cron-a/cron-c/evening-checks 等）はモードで内部分岐するので false。';

-- 対象行が manifest に揃っていることを確認してから立てる（typo で黙って外れるのを防ぐ = fail closed）
DO $$
DECLARE
  v_missing text;
BEGIN
  SELECT string_agg(x.workflow_file, ', ' ORDER BY x.workflow_file)
    INTO v_missing
  FROM (VALUES
    ('cron-b.yml'),
    ('cron-f.yml'),
    ('high-dividend-screening.yml'),
    ('signal-performance-eval.yml'),
    ('yutai-cross-screening.yml'),
    ('overheat.yml'),
    ('earnings-alert.yml'),
    ('earnings-surprise.yml'),
    ('entry-timing-signal.yml')
  ) AS x(workflow_file)
  WHERE NOT EXISTS (
    SELECT 1 FROM ops.expected_workflows w WHERE w.workflow_file = x.workflow_file
  );
  IF v_missing IS NOT NULL THEN
    RAISE EXCEPTION 'requires_jquants を立てる manifest 行が無い: %', v_missing;
  END IF;
END $$;

UPDATE ops.expected_workflows
   SET requires_jquants = true
 WHERE workflow_file IN (
   'cron-b.yml',                   -- 決算発表カレンダー（J-Quants）
   'cron-f.yml',                   -- 銘柄別信用残（J-Quants Standard）
   'high-dividend-screening.yml',  -- 個別株スクリーニング（株価・財務）
   'signal-performance-eval.yml',  -- 株価が止まると誤ったリターンを永続書込みする
   'yutai-cross-screening.yml',    -- 株価で実質利回りを計算する
   'overheat.yml',                 -- 以下 4 本は evening-checks 内の戦略の未達検知専用行（00131）
   'earnings-alert.yml',
   'earnings-surprise.yml',
   'entry-timing-signal.yml'
 );

-- ============================================================================
-- 3. dispatch_by_name: OFF 中は requires_jquants の行を起動しない（00057 の差し替え）
-- ============================================================================

CREATE OR REPLACE FUNCTION ops.dispatch_by_name(p_workflow_file text)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_repo             text;
  v_ref              text;
  v_enabled          boolean;
  v_requires_jquants boolean;
  v_label            text;
BEGIN
  SELECT repo, ref, enabled, requires_jquants
    INTO v_repo, v_ref, v_enabled, v_requires_jquants
  FROM ops.expected_workflows
  WHERE workflow_file = p_workflow_file;

  IF v_repo IS NULL THEN
    RAISE EXCEPTION 'workflow % not registered in ops.expected_workflows', p_workflow_file;
  END IF;

  IF NOT v_enabled THEN
    RAISE NOTICE 'workflow % is disabled in ops.expected_workflows; skipping dispatch', p_workflow_file;
    RETURN NULL;
  END IF;

  IF v_requires_jquants AND NOT ops.jquants_enabled() THEN
    RAISE NOTICE 'workflow % requires J-Quants and J-Quants is OFF; skipping dispatch', p_workflow_file;
    RETURN NULL;
  END IF;

  v_label := to_char(timezone('Asia/Tokyo', now()), 'YYYY-MM-DD') || ':' || p_workflow_file;

  RETURN ops.dispatch_github_workflow(v_repo, p_workflow_file, v_ref, '{}'::jsonb, v_label);
END;
$$;

-- ============================================================================
-- 4. check_freshness: OFF 中は requires_jquants の行を未達検知しない（00061 の差し替え）
-- ============================================================================

CREATE OR REPLACE FUNCTION ops.check_freshness()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  w         record;
  v_today   date := (timezone('Asia/Tokyo', now()))::date;
  v_time    time := (timezone('Asia/Tokyo', now()))::time;
  v_dow     int  := extract(dow FROM timezone('Asia/Tokyo', now()));  -- 0=Sun .. 6=Sat
  v_ok      boolean;
  v_body    text := '';
  v_files   text[] := '{}';
  v_jquants boolean := ops.jquants_enabled();
BEGIN
  FOR w IN
    SELECT workflow_file, job_name, kind, deadline_jst
      FROM ops.expected_workflows
     WHERE enabled = true
       AND job_name IS NOT NULL
       AND deadline_jst IS NOT NULL
       AND kind IN ('daily', 'weekday')
       AND (v_jquants OR NOT requires_jquants)
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

-- ============================================================================
-- 5. 橋渡しRPC（jquants_ingest・service_role 専用）
-- ============================================================================

-- 読み取り: 現在のモードと、OFF 中に止まるワークフローの一覧（Scouter の fallback-sweeper が使う）
CREATE OR REPLACE FUNCTION jquants_ingest.get_data_source_mode(
  p_provider text DEFAULT 'jquants'
)
RETURNS TABLE (
  provider                  text,
  enabled                   boolean,
  changed_at                timestamptz,
  changed_by                text,
  reason                    text,
  last_official_trade_date  date,
  skipped_workflows         text[]
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = ''
AS $$
  SELECT
    s.provider,
    s.enabled,
    s.changed_at,
    s.changed_by,
    s.reason,
    s.last_official_trade_date,
    CASE WHEN s.enabled THEN ARRAY[]::text[]
         ELSE COALESCE(
           (SELECT array_agg(w.workflow_file ORDER BY w.workflow_file)
              FROM ops.expected_workflows w
             WHERE w.requires_jquants),
           ARRAY[]::text[])
    END
  FROM ops.data_source_switches s
  WHERE s.provider = p_provider
$$;

COMMENT ON FUNCTION jquants_ingest.get_data_source_mode(text) IS
  'J-Quants ON/OFF スイッチの読み取り橋渡し（00132）。行が無ければ0行を返す（呼び出し側は例外にすること）。service_role専用。';

-- 書き込み: 切替。OFF にするときは最後の公式取引日を記録する。
CREATE OR REPLACE FUNCTION jquants_ingest.set_data_source_mode(
  p_provider   text,
  p_enabled    boolean,
  p_reason     text,
  p_changed_by text
)
RETURNS TABLE (
  provider                  text,
  enabled                   boolean,
  changed_at                timestamptz,
  last_official_trade_date  date
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_last date;
BEGIN
  IF p_provider IS NULL OR p_enabled IS NULL THEN
    RAISE EXCEPTION 'provider and enabled are required';
  END IF;
  IF p_reason IS NULL OR btrim(p_reason) = '' THEN
    RAISE EXCEPTION 'reason is required（切替理由を監査のため必ず残す）';
  END IF;

  -- 行をロックしてから更新する（同時切替で last_official_trade_date を取り違えない）
  PERFORM 1 FROM ops.data_source_switches s WHERE s.provider = p_provider FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'unknown provider: %', p_provider;
  END IF;

  IF NOT p_enabled THEN
    SELECT max(b.trade_date) INTO v_last
      FROM jquants_core.equity_bar_daily b
     WHERE b.session = 'DAY';
  END IF;

  RETURN QUERY
  UPDATE ops.data_source_switches s
     SET enabled = p_enabled,
         changed_at = now(),
         changed_by = p_changed_by,
         reason = p_reason,
         -- ON にしても OFF 時点の値は残す（jquants-restore が埋め戻し起点に使う）
         last_official_trade_date = CASE WHEN p_enabled THEN s.last_official_trade_date ELSE v_last END
   WHERE s.provider = p_provider
  RETURNING s.provider, s.enabled, s.changed_at, s.last_official_trade_date;
END;
$$;

COMMENT ON FUNCTION jquants_ingest.set_data_source_mode(text, boolean, text, text) IS
  'J-Quants ON/OFF スイッチの切替（00132）。OFF 時に最後の公式取引日を記録。ON の疎通確認は呼び出し側（jquants-toggle.sh）が行う。service_role専用。';

REVOKE ALL ON FUNCTION jquants_ingest.get_data_source_mode(text)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION jquants_ingest.set_data_source_mode(text, boolean, text, text)
  FROM PUBLIC, anon, authenticated, service_role;

GRANT EXECUTE ON FUNCTION jquants_ingest.get_data_source_mode(text) TO service_role;
GRANT EXECUTE ON FUNCTION jquants_ingest.set_data_source_mode(text, boolean, text, text) TO service_role;
