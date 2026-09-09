-- 00126_register_lifeos_dispatch.sql
-- ============================================================================
-- lifeOS の配信・成果物系 10 ワークフローを外部トリガー(pg_cron dispatch)へ載せる
-- ----------------------------------------------------------------------------
-- 背景:
--   外部トリガー基盤(00057/00058)は JapanStockDataPipeline / JapanStockScouter の
--   2 リポジトリだけを対象にしていた。lifeOS は素の GitHub schedule のままで、
--   2026-08-26 頃から発火遅延が悪化（+15分 → +2〜3時間 → +8〜11時間）し、
--   2026-08-28(金) は Watch Check(cron '17 7 * * 5' = 16:17 JST) の run が
--   1件も作られず MarketWatch メールが欠測した。
--   GitHub の schedule はベストエフォートで、高負荷時はキュー済みジョブが
--   破棄されうる（今回まさにそれ）。pg_cron は分単位で正確に発火する。
--
-- 対象外:
--   * periodic-review.yml は cron を 4 本(四半期ごとに別日付)持つため、
--     00125 の manifest RPC が要求する「1 workflow = 1 dispatch ジョブ・
--     schedule 完全一致」契約に反する。年4回で遅延の実害が無いので GitHub
--     schedule のまま残す。
--   * prompt-improvement 系 / archive-old-feeds / source-suggestion /
--     secret-scan / tavily-usage-alert は内部メンテ用で数時間ずれても実害なし。
--
-- 前提(この migration を適用する前に満たすこと):
--   1. lifeOS 側の 10 ワークフローに dispatch_label input・run-name・
--      二重実行ガード job が入った状態で main へ push 済みであること。
--      input 未宣言のまま dispatch すると GitHub は 422 を返して静かに落ちる。
--   2. Vault の github_dispatch_pat が lifeOS リポジトリにも Actions:RW を
--      持つこと（従来は 2 リポジトリスコープ）。不足時は dispatch が 404 になる。
--      ops.dispatch_status の status_code で確認する（204 = 成功）。
--
-- 運用: GitHub schedule はフォールバックとして残置する。lifeOS には Supabase の
--   job_runs 相当が無いため、二重実行ガードは各 workflow の guard job が
--   GitHub API を引いて「当日ラベルの run が既に成功/実行中なら skip」で実装した。
--   よって job_name / deadline_jst は NULL とし、ops.check_freshness の対象外
--   （同関数は job_name IS NOT NULL で絞っている）。dispatch 自体の失敗は
--   ops.check_cron_health / ops.reconcile_dispatches がリポジトリ非依存で拾う。
-- ============================================================================

INSERT INTO ops.expected_workflows
  (workflow_file, repo, friendly_name, schedule_utc, kind, deadline_jst, job_name, enabled, notes)
VALUES
  ('daily-brief.yml',      'lifeOS', 'DailyBrief',        '17 21 * * *', 'daily',   NULL, NULL, true, 'lifeOS: メール配信 06:17 JST'),
  ('blog-check.yml',       'lifeOS', 'Blog Check',        '45 19 * * *', 'daily',   NULL, NULL, true, 'lifeOS: メール配信 04:45 JST'),
  ('dashboard-deploy.yml', 'lifeOS', 'Dashboard Deploy',  '17 0 * * *',  'daily',   NULL, NULL, true, 'lifeOS: Vercel デプロイ 09:17 JST'),
  ('watch-check.yml',      'lifeOS', 'Watch Check',       '17 7 * * 5',  'weekly',  NULL, NULL, true, 'lifeOS: MarketWatch メール 金 16:17 JST'),
  ('week-ahead.yml',       'lifeOS', 'WeekAhead',         '0 21 * * 0',  'weekly',  NULL, NULL, true, 'lifeOS: メール配信 月 06:00 JST'),
  ('journal-review.yml',   'lifeOS', 'Journal Review',    '17 23 * * 6', 'weekly',  NULL, NULL, true, 'lifeOS: メール配信 日 08:17 JST'),
  ('nakajima-summary.yml', 'lifeOS', 'Nakajima Summary',  '30 23 * * 1', 'weekly',  NULL, NULL, true, 'lifeOS: メール配信 火 08:30 JST'),
  ('life-dashboard.yml',   'lifeOS', 'Life Dashboard',    '17 22 * * 0', 'weekly',  NULL, NULL, true, 'lifeOS: 成果物生成 月 07:17 JST'),
  ('monthly-reminder.yml', 'lifeOS', 'Monthly Reminder',  '0 21 1 * *',  'monthly', NULL, NULL, true, 'lifeOS: メール配信 2日 06:00 JST'),
  ('monthly-finance.yml',  'lifeOS', 'MonthlyFinance',    '0 21 2 * *',  'monthly', NULL, NULL, true, 'lifeOS: メール配信 3日 06:00 JST')
ON CONFLICT (workflow_file) DO NOTHING;

-- workflow_file は単独 PRIMARY KEY なので、将来 JapanStock 側に同名ファイルが
-- 生まれると DO NOTHING が黙って効き、pg_cron が別リポジトリへ dispatch してしまう。
-- repo だけでなく ref / schedule_utc / kind / enabled まで期待値と一致することを
-- 確認し、1 つでも違えば適用を失敗させる（fail closed）。
-- enabled=false のまま既存行が残ると dispatch は恒久 no-op、ref 不一致なら別ブランチを
-- 叩くため、どちらも「静かに動かない」故障になる。
DO $$
DECLARE
  v_bad text;
BEGIN
  SELECT string_agg(format('%s(%s)', e.workflow_file, e.reason), ', ' ORDER BY e.workflow_file)
    INTO v_bad
  FROM (
    SELECT x.workflow_file,
           CASE
             WHEN w.workflow_file IS NULL              THEN '未登録'
             WHEN w.repo         IS DISTINCT FROM 'lifeOS'        THEN 'repo=' || w.repo
             WHEN w.ref          IS DISTINCT FROM 'main'          THEN 'ref=' || w.ref
             WHEN w.schedule_utc IS DISTINCT FROM x.schedule_utc  THEN 'schedule=' || w.schedule_utc
             WHEN w.kind         IS DISTINCT FROM x.kind          THEN 'kind=' || w.kind
             WHEN w.enabled      IS DISTINCT FROM true            THEN 'enabled=false'
           END AS reason
    FROM (VALUES
      ('daily-brief.yml',      '17 21 * * *', 'daily'),
      ('blog-check.yml',       '45 19 * * *', 'daily'),
      ('dashboard-deploy.yml', '17 0 * * *',  'daily'),
      ('watch-check.yml',      '17 7 * * 5',  'weekly'),
      ('week-ahead.yml',       '0 21 * * 0',  'weekly'),
      ('journal-review.yml',   '17 23 * * 6', 'weekly'),
      ('nakajima-summary.yml', '30 23 * * 1', 'weekly'),
      ('life-dashboard.yml',   '17 22 * * 0', 'weekly'),
      ('monthly-reminder.yml', '0 21 1 * *',  'monthly'),
      ('monthly-finance.yml',  '0 21 2 * *',  'monthly')
    ) AS x(workflow_file, schedule_utc, kind)
    LEFT JOIN ops.expected_workflows w ON w.workflow_file = x.workflow_file
  ) e
  WHERE e.reason IS NOT NULL;

  IF v_bad IS NOT NULL THEN
    RAISE EXCEPTION 'lifeOS manifest が期待値と不一致: % (workflow_file の名前衝突・既存行の残留を疑うこと)', v_bad;
  END IF;
END $$;

-- 既存の dispatch-lifeos-* ジョブがあれば、cron.schedule は jobname 単位の upsert なので
-- 黙って上書きしてしまう。別用途で同名が使われていないことを先に確かめる（fail closed）。
DO $$
DECLARE
  v_conflict text;
BEGIN
  SELECT string_agg(format('%s -> %s', j.jobname, btrim(j.command)), ', ' ORDER BY j.jobname)
    INTO v_conflict
  FROM cron.job j
  WHERE j.jobname LIKE 'dispatch-lifeos-%'
    AND btrim(j.command) <> 'SELECT ops.dispatch_by_name('''
                            || replace(j.jobname, 'dispatch-lifeos-', '') || '.yml'')';

  IF v_conflict IS NOT NULL THEN
    RAISE EXCEPTION 'dispatch-lifeos-* に想定外のジョブが既存: % (上書きを避けるため中止)', v_conflict;
  END IF;
END $$;

-- pg_cron ジョブは manifest の schedule_utc と文字列完全一致させる
-- （00125 の set_expected_workflow_enabled が一致を要求するため）。
SELECT cron.schedule('dispatch-lifeos-daily-brief',      '17 21 * * *', $$ SELECT ops.dispatch_by_name('daily-brief.yml') $$);
SELECT cron.schedule('dispatch-lifeos-blog-check',       '45 19 * * *', $$ SELECT ops.dispatch_by_name('blog-check.yml') $$);
SELECT cron.schedule('dispatch-lifeos-dashboard-deploy', '17 0 * * *',  $$ SELECT ops.dispatch_by_name('dashboard-deploy.yml') $$);
SELECT cron.schedule('dispatch-lifeos-watch-check',      '17 7 * * 5',  $$ SELECT ops.dispatch_by_name('watch-check.yml') $$);
SELECT cron.schedule('dispatch-lifeos-week-ahead',       '0 21 * * 0',  $$ SELECT ops.dispatch_by_name('week-ahead.yml') $$);
SELECT cron.schedule('dispatch-lifeos-journal-review',   '17 23 * * 6', $$ SELECT ops.dispatch_by_name('journal-review.yml') $$);
SELECT cron.schedule('dispatch-lifeos-nakajima-summary', '30 23 * * 1', $$ SELECT ops.dispatch_by_name('nakajima-summary.yml') $$);
SELECT cron.schedule('dispatch-lifeos-life-dashboard',   '17 22 * * 0', $$ SELECT ops.dispatch_by_name('life-dashboard.yml') $$);
SELECT cron.schedule('dispatch-lifeos-monthly-reminder', '0 21 1 * *',  $$ SELECT ops.dispatch_by_name('monthly-reminder.yml') $$);
SELECT cron.schedule('dispatch-lifeos-monthly-finance',  '0 21 2 * *',  $$ SELECT ops.dispatch_by_name('monthly-finance.yml') $$);
