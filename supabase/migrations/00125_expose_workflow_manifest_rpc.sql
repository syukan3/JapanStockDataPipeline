-- 00125_expose_workflow_manifest_rpc.sql
-- カナリア用トグル（ops.expected_workflows.enabled）をスクリプトから操作できるようにする。
--
-- 背景:
--   ops スキーマは PostgREST に公開していない（00057 の設計。anon/authenticated へは一切grantしない）。
--   そのため manifest の enabled 切替が Supabase の SQL エディタ手作業になっていた。
--   新機能を出すたびに「適用 → canary → 手でUPDATE」の手作業が挟まるのは運用事故のもとなので、
--   公開スキーマ jquants_ingest（ジョブ管理の所在）へ**用途を絞ったRPCだけ**を置く。
--
-- 設計:
--   * ops スキーマ自体は非公開のまま。RPCはSECURITY DEFINERで橋渡しするだけ。
--   * EXECUTE は service_role のみ。anon/authenticated/PUBLIC からは剥奪する。
--   * setter は既存行の UPDATE だけを行う。INSERT はしない（typoで幽霊manifestを作らせない）。
--   * setter は manifest と pg_cron の dispatch ジョブが揃っていることを確認してから有効化する
--     （契約が壊れたまま有効化して、静かに未実行になるのを防ぐ = fail closed）。

-- ============================================================================
-- 1. 読み取り: manifest の現在値
-- ============================================================================

CREATE OR REPLACE FUNCTION jquants_ingest.list_expected_workflows(
  p_workflow_file TEXT DEFAULT NULL
)
RETURNS TABLE (
  workflow_file   TEXT,
  repo            TEXT,
  friendly_name   TEXT,
  ref             TEXT,
  schedule_utc    TEXT,
  kind            TEXT,
  deadline_jst    TEXT,
  job_name        TEXT,
  enabled         BOOLEAN,
  notes           TEXT,
  cron_job_count  INTEGER,
  cron_jobname    TEXT,
  cron_schedule   TEXT
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = ''
AS $$
  -- cron.job は LATERAL で集約する。同じ dispatch コマンドが重複登録されていても
  -- manifest 1行が複数行に増えないようにし、件数を呼び出し側へ見せる。
  SELECT
    w.workflow_file,
    w.repo,
    w.friendly_name,
    w.ref,
    w.schedule_utc,
    w.kind,
    w.deadline_jst,
    w.job_name,
    w.enabled,
    w.notes,
    c.cron_job_count,
    c.cron_jobname,
    c.cron_schedule
  FROM ops.expected_workflows w
  LEFT JOIN LATERAL (
    SELECT
      count(*)::integer AS cron_job_count,
      string_agg(DISTINCT j.jobname, ', ' ORDER BY j.jobname)   AS cron_jobname,
      string_agg(DISTINCT j.schedule, ', ' ORDER BY j.schedule) AS cron_schedule
    FROM cron.job j
    WHERE btrim(j.command) = 'SELECT ops.dispatch_by_name(''' || w.workflow_file || ''')'
  ) c ON true
  WHERE p_workflow_file IS NULL OR w.workflow_file = p_workflow_file
  ORDER BY w.workflow_file
$$;

COMMENT ON FUNCTION jquants_ingest.list_expected_workflows(text) IS
  'ops.expected_workflows の読み取り橋渡し（opsは非公開スキーマのため）。service_role専用。';

-- ============================================================================
-- 2. 書き込み: カナリアトグルの切替
-- ============================================================================

CREATE OR REPLACE FUNCTION jquants_ingest.set_expected_workflow_enabled(
  p_workflow_file TEXT,
  p_enabled       BOOLEAN
)
RETURNS TABLE (
  workflow_file   TEXT,
  friendly_name   TEXT,
  enabled         BOOLEAN,
  cron_job_count  INTEGER,
  cron_jobname    TEXT,
  cron_schedule   TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  w ops.expected_workflows%ROWTYPE;
  v_cron_count    INTEGER;
  v_cron_jobname  TEXT;
  v_cron_schedule TEXT;
  v_updated       RECORD;
BEGIN
  IF p_workflow_file IS NULL OR p_enabled IS NULL THEN
    RAISE EXCEPTION 'workflow_file and enabled are required';
  END IF;

  -- 対象行をロックしてから読む。判定に使った値と実際に更新する行を食い違わせない。
  SELECT * INTO w
  FROM ops.expected_workflows e
  WHERE e.workflow_file = p_workflow_file
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'unknown workflow_file: % (このRPCは既存manifestの更新のみを行う)', p_workflow_file;
  END IF;

  -- dispatch ジョブは集約して数える。重複登録は二重発火の温床なので有効化を拒否する。
  SELECT
    count(*)::integer,
    string_agg(DISTINCT j.jobname, ', ' ORDER BY j.jobname),
    string_agg(DISTINCT j.schedule, ', ' ORDER BY j.schedule)
  INTO v_cron_count, v_cron_jobname, v_cron_schedule
  FROM cron.job j
  WHERE btrim(j.command) = 'SELECT ops.dispatch_by_name(''' || p_workflow_file || ''')';

  -- 有効化のときだけ器が揃っているかを確認する（fail closed）。
  -- 無効化は事故対応で使うため、器が壊れていても常に通す。
  IF p_enabled THEN
    IF v_cron_count = 0 THEN
      RAISE EXCEPTION 'no pg_cron dispatch job for %; 有効化しても起動されないため中止する', p_workflow_file;
    END IF;
    IF v_cron_count > 1 THEN
      RAISE EXCEPTION 'multiple pg_cron dispatch jobs for % (% 件: %); 二重発火するため重複を解消してから有効化する',
        p_workflow_file, v_cron_count, v_cron_jobname;
    END IF;
    IF v_cron_schedule IS DISTINCT FROM w.schedule_utc THEN
      RAISE EXCEPTION 'schedule mismatch for %: manifest=% / cron=%',
        p_workflow_file, w.schedule_utc, v_cron_schedule;
    END IF;
  END IF;

  -- 同値でも必ず UPDATE ... RETURNING を通し、返す値を実際のDB状態と一致させる。
  UPDATE ops.expected_workflows e
     SET enabled = p_enabled
   WHERE e.workflow_file = p_workflow_file
  RETURNING e.workflow_file, e.friendly_name, e.enabled
  INTO v_updated;

  -- FOR UPDATE で行を押さえているので通常は起こらないが、防御的に確認する。
  IF NOT FOUND THEN
    RAISE EXCEPTION 'failed to update manifest for %', p_workflow_file;
  END IF;

  RETURN QUERY
  SELECT v_updated.workflow_file, v_updated.friendly_name, v_updated.enabled,
         v_cron_count, v_cron_jobname, v_cron_schedule;
END;
$$;

COMMENT ON FUNCTION jquants_ingest.set_expected_workflow_enabled(text, boolean) IS
  'カナリア用 enabled トグルの切替。既存manifestのUPDATEのみ。有効化時はpg_cron dispatchの存在と schedule一致を要求する。service_role専用。';

-- ============================================================================
-- 3. 権限（service_role のみ）
-- ============================================================================

REVOKE ALL ON FUNCTION jquants_ingest.list_expected_workflows(text)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION jquants_ingest.set_expected_workflow_enabled(text, boolean)
  FROM PUBLIC, anon, authenticated, service_role;

GRANT EXECUTE ON FUNCTION jquants_ingest.list_expected_workflows(text) TO service_role;
GRANT EXECUTE ON FUNCTION jquants_ingest.set_expected_workflow_enabled(text, boolean) TO service_role;
