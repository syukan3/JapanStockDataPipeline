-- 00128_register_lifeos_job_watch.sql
-- ============================================================================
-- lifeOS の JobWatch（job-watch.yml）を外部トリガー(pg_cron dispatch)へ載せる
-- ----------------------------------------------------------------------------
-- 背景:
--   00126 で lifeOS の配信系 10 本、00127 で EventWatch を pg_cron dispatch へ
--   移した。JobWatch はその後（2026-09-20）に追加された 12 本目で、
--   amazon.jobs の検索 JSON を毎朝 05:52 JST に読み、AWS 求人の
--   **掲載の開閉**（新規掲載・掲載終了・必要経験年数の変更・再掲載）を
--   差分検知してメールする。
--
--   GitHub schedule の実測遅延（2026-08 時点で最大 +8〜11 時間、高負荷時は
--   ジョブ破棄）を被ると、掲載終了の検知が数日ずれる。求人の窓は実測で
--   5 週間程度で閉じる（経験年数 7 年の掲載 Job ID 10450223 が
--   2026-08-12 から 09-20 の間に閉じた）ため、遅延はそのまま
--   「閉じたことに気づかない」に化ける。00126 / 00127 と同じ理由で外部トリガーへ載せる。
--
-- 特記事項:
--   JobWatch は **差分 0 件の日はメールを送らない**設計なので、EventWatch と同様に
--   dispatch が静かに止まっても受信側からは気づけない。検知は lifeOS 側の
--   lifeos-doctor（`github.job-watch` heartbeat・毎日 08:00 JST・36h 閾値）が担う。
--   ops 側では従来どおり ops.check_cron_health / ops.reconcile_dispatches が
--   dispatch 自体の失敗を拾う。
--
-- 時刻の根拠:
--   05:52 JST は EventWatch（05:47）の直後・DailyBrief（06:17）の前。
--   朝メールを読む前に前日ぶんの求人差分が届いている状態にする。
--   :00/:15/:30/:45 は cron 混雑で遅延しやすいため :52 に置く（00126 と同じ方針）。
--
-- 前提(この migration を適用する前に満たすこと):
--   1. lifeOS の job-watch.yml が dispatch_label input・run-name・guard job を
--      備えた状態で **main へ push 済み**であること。未 push または input 未宣言だと
--      GitHub は 404 / 422 を返して静かに落ちる（ops.dispatch_status の
--      status_code で確認する。204 = 成功）。
--      → 2026-09-20 のコミット e53b12a1 で満たしている。
--   2. Vault の github_dispatch_pat が lifeOS リポジトリへ Actions:RW を持つこと。
--      00126 で既に満たされている（同一リポジトリ・同一 PAT）。
--
-- 運用: 00126 / 00127 と同じ。GitHub schedule はフォールバックとして残置し、
--   二重実行は workflow 側の guard job が抑止する。lifeOS には Supabase の
--   job_runs 相当が無いため job_name / deadline_jst は NULL とし、
--   ops.check_freshness の対象外。
-- ============================================================================

INSERT INTO ops.expected_workflows
  (workflow_file, repo, friendly_name, schedule_utc, kind, deadline_jst, job_name, enabled, notes)
VALUES
  ('job-watch.yml', 'lifeOS', 'Job Watch', '52 20 * * *', 'daily', NULL, NULL, true,
   'lifeOS: AWS 求人の掲載開閉の日次差分検知 05:52 JST（差分がある日だけメール送信）')
ON CONFLICT (workflow_file) DO NOTHING;

-- workflow_file は単独 PRIMARY KEY なので、将来 JapanStock 側に同名ファイルが
-- 生まれると DO NOTHING が黙って効き、pg_cron が別リポジトリへ dispatch してしまう。
-- repo だけでなく ref / schedule_utc / kind / enabled まで期待値と一致することを
-- 確認し、1 つでも違えば適用を失敗させる（fail closed）。00126 / 00127 と同じ検査。
DO $$
DECLARE
  v_bad text;
BEGIN
  SELECT string_agg(format('%s(%s)', e.workflow_file, e.reason), ', ' ORDER BY e.workflow_file)
    INTO v_bad
  FROM (
    SELECT x.workflow_file,
           CASE
             WHEN w.workflow_file IS NULL                        THEN '未登録'
             WHEN w.repo         IS DISTINCT FROM 'lifeOS'       THEN 'repo=' || w.repo
             WHEN w.ref          IS DISTINCT FROM 'main'         THEN 'ref=' || w.ref
             WHEN w.schedule_utc IS DISTINCT FROM x.schedule_utc THEN 'schedule=' || w.schedule_utc
             WHEN w.kind         IS DISTINCT FROM x.kind         THEN 'kind=' || w.kind
             WHEN w.enabled      IS DISTINCT FROM true           THEN 'enabled=false'
           END AS reason
    FROM (VALUES
      ('job-watch.yml', '52 20 * * *', 'daily')
    ) AS x(workflow_file, schedule_utc, kind)
    LEFT JOIN ops.expected_workflows w ON w.workflow_file = x.workflow_file
  ) e
  WHERE e.reason IS NOT NULL;

  IF v_bad IS NOT NULL THEN
    RAISE EXCEPTION 'lifeOS manifest が期待値と不一致: % (workflow_file の名前衝突・既存行の残留を疑うこと)', v_bad;
  END IF;
END $$;

-- cron.schedule は jobname 単位の upsert なので、既存の同名ジョブがあれば黙って
-- 上書きしてしまう。別用途で使われていないことを先に確かめる（fail closed）。
DO $$
DECLARE
  v_conflict text;
BEGIN
  SELECT string_agg(format('%s -> %s', j.jobname, btrim(j.command)), ', ' ORDER BY j.jobname)
    INTO v_conflict
  FROM cron.job j
  WHERE j.jobname = 'dispatch-lifeos-job-watch'
    AND btrim(j.command) <> 'SELECT ops.dispatch_by_name(''job-watch.yml'')';

  IF v_conflict IS NOT NULL THEN
    RAISE EXCEPTION 'dispatch-lifeos-job-watch に想定外のジョブが既存: % (上書きを避けるため中止)', v_conflict;
  END IF;
END $$;

-- pg_cron ジョブは manifest の schedule_utc と文字列完全一致させる
-- （00125 の set_expected_workflow_enabled が一致を要求するため）。
SELECT cron.schedule('dispatch-lifeos-job-watch', '52 20 * * *', $$ SELECT ops.dispatch_by_name('job-watch.yml') $$);
