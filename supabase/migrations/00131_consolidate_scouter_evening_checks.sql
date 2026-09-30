-- 00131_consolidate_scouter_evening_checks.sql
-- ============================================================================
-- JapanStockScouter の夕方の短いチェック 7 本の dispatch を、集約 workflow
-- evening-checks.yml 1 本に置き換える
-- ----------------------------------------------------------------------------
-- 背景（2026-09-30）:
--   GitHub Actions はジョブごとに 1 分へ切り上げて課金される（private リポジトリの
--   無料枠 2,000 分/月。2026-09-28 に使い切った）。Scouter の次の 7 本はどれも本体 3〜8 秒・
--   準備（checkout + setup-node + npm ci）17 秒ほどのジョブで、別々に dispatch すると
--   1 日 7 分前後・月 ~210 分を使っていた。Scouter 側で 7 本を evening-checks.yml の
--   1 ジョブに集約し（準備が 1 回で済む。月 ~80 分）、dispatch もそれ 1 本にする。
--
--     price-alert.yml（19:10）/ overheat.yml（19:40）/ earnings-alert.yml（20:00）/
--     entry-timing-signal.yml（平日 20:10）/ yutai-alert.yml（20:15）/
--     holdings-news.yml（平日 20:30）/ earnings-surprise.yml（20:45）
--
--   evening-checks は毎日 20:15 JST（Cron B 19:30・high-dividend / macro-ai 19:50 の後）に
--   7 戦略をデータ依存の順に動かす。entry-timing-signal / holdings-news は workflow 側で
--   土日を飛ばす（従来の '* * 1-5' と同じ挙動）。yutai-cross-screening.yml（17:00）は
--   当日 16:30 取り込みの信用在庫で「在庫あり」を知らせるため遅らせられず、対象外（変更なし）。
--
-- 未達検知（ops.check_freshness）の扱い:
--   check_freshness は manifest 1 行につき job_name を 1 つだけ見る（00061）。各戦略は
--   集約後も自分の job_name（scouter-<戦略>）で job_runs を書くので、**旧 7 行は
--   enabled=true のまま「dispatch なし・freshness 専用」として残し**、cron ジョブだけ外す。
--   締切（deadline_jst）は新しい起動時刻に合わせて 21:00 JST に揃える（旧値は 19:40〜21:15。
--   price-alert 19:40 は 20:15 起動より前なので、直さないと毎日誤報になる）。
--   schedule_utc は表示用として実際の起動（evening-checks の 20:15）に合わせる。
--   新しい evening-checks.yml の行は job_name=NULL の dispatch 専用行（freshness の対象外）。
--
--   注意: この 7 行の enabled は「dispatch を許可するか」ではなく「未達検知を続けるか」の
--   意味で残している。jquants_ingest.set_expected_workflow_enabled で disable すると
--   未達検知が黙って外れ、enable は cron ジョブが無いので失敗する（00125 の fail closed）。
--   notes にも明記する。
--
-- 前提（この migration を適用する前に満たすこと）:
--   1. Scouter の evening-checks.yml（dispatch_label input・run-name 付き）が main へ push 済み。
--      未 push だと dispatch が 404 で落ちる（ops.dispatch_status の status_code で確認。204 = 成功）。
--   2. Scouter から旧 7 本の workflow は削除済み（同じ push）。先に消して本 migration が
--      遅れると、その間の旧 dispatch が 404 になる（reconcile_dispatches が通知する）。
--   3. 切替（Scouter の push → 本 migration の適用）は、旧 7 本の定刻（19:10〜20:45 JST）と
--      新しい 20:15 を避け、旧 run が実行中・待機中でない時間に続けて行う。旧 run と新 run が
--      並ぶと holdings-news の「単一実行は workflow の concurrency が保証」の前提が崩れる
--      （concurrency group が旧 holdings-news-check と新 evening-checks で別のため）。
--      rollback も同じ。
--
-- ロールバック: supabase/rollbacks/00131_consolidate_scouter_evening_checks.down.sql
--   （Scouter 側も旧 7 本の workflow を戻す必要がある）
-- ============================================================================

-- 0-1. 旧 7 行が期待どおり存在する（名前衝突・想定外の既存値を疑って fail closed）
DO $$
DECLARE
  v_bad text;
BEGIN
  SELECT string_agg(format('%s(%s)', e.workflow_file, e.reason), ', ' ORDER BY e.workflow_file)
    INTO v_bad
  FROM (
    SELECT x.workflow_file,
           CASE
             WHEN w.workflow_file IS NULL                    THEN '未登録'
             WHEN w.repo     IS DISTINCT FROM 'JapanStockScouter' THEN 'repo=' || w.repo
             WHEN w.job_name IS DISTINCT FROM x.job_name     THEN 'job_name=' || coalesce(w.job_name, 'NULL')
             WHEN w.kind     IS DISTINCT FROM x.kind         THEN 'kind=' || w.kind
             WHEN w.enabled  IS DISTINCT FROM true           THEN 'enabled=false'
           END AS reason
    FROM (VALUES
      ('price-alert.yml',           'scouter-price-alert',         'daily'),
      ('overheat.yml',              'scouter-overheat',            'daily'),
      ('earnings-alert.yml',        'scouter-earnings-alert',      'daily'),
      ('entry-timing-signal.yml',   'scouter-entry-timing-signal', 'weekday'),
      ('yutai-alert.yml',           'scouter-yutai-alert',         'daily'),
      ('holdings-news.yml',         'scouter-holdings-news',       'weekday'),
      ('earnings-surprise.yml',     'scouter-earnings-surprise',   'daily')
    ) AS x(workflow_file, job_name, kind)
    LEFT JOIN ops.expected_workflows w ON w.workflow_file = x.workflow_file
  ) e
  WHERE e.reason IS NOT NULL;

  IF v_bad IS NOT NULL THEN
    RAISE EXCEPTION 'Scouter の旧 manifest が期待値と不一致: % (中止)', v_bad;
  END IF;
END $$;

-- 0-2. cron.schedule は jobname 単位の upsert なので、既存の同名ジョブを黙って
-- 上書きしないことを確かめる（00130 と同じ fail closed）
DO $$
DECLARE
  v_conflict text;
BEGIN
  SELECT string_agg(format('%s -> %s', j.jobname, btrim(j.command)), ', ' ORDER BY j.jobname)
    INTO v_conflict
  FROM cron.job j
  WHERE j.jobname = 'dispatch-evening-checks'
    AND btrim(j.command) <> 'SELECT ops.dispatch_by_name(''evening-checks.yml'')';

  IF v_conflict IS NOT NULL THEN
    RAISE EXCEPTION 'dispatch-evening-checks に想定外のジョブが既存: % (上書きを避けるため中止)', v_conflict;
  END IF;
END $$;

-- 1. 集約 workflow（dispatch 専用。未達検知は旧 7 行が戦略ごとに行う）
INSERT INTO ops.expected_workflows
  (workflow_file, repo, friendly_name, schedule_utc, kind, deadline_jst, job_name, enabled, notes)
VALUES
  ('evening-checks.yml', 'JapanStockScouter', 'Evening Checks', '15 11 * * *', 'daily', NULL, NULL, true,
   '夕方の短いチェック 7 本を 1 ジョブで順に実行（20:15 JST）。未達検知は各戦略の旧行（dispatch なし）で行う')
ON CONFLICT (workflow_file) DO NOTHING;

-- workflow_file は単独 PRIMARY KEY。同名の既存行が残っていると DO NOTHING が黙って効くので、
-- 期待値と一致することを確かめる（00126〜00130 と同じ検査）
DO $$
DECLARE
  v_row ops.expected_workflows%ROWTYPE;
BEGIN
  SELECT * INTO v_row FROM ops.expected_workflows WHERE workflow_file = 'evening-checks.yml';
  IF v_row.repo IS DISTINCT FROM 'JapanStockScouter'
     OR v_row.ref IS DISTINCT FROM 'main'
     OR v_row.schedule_utc IS DISTINCT FROM '15 11 * * *'
     OR v_row.kind IS DISTINCT FROM 'daily'
     OR v_row.job_name IS NOT NULL
     OR v_row.enabled IS DISTINCT FROM true THEN
    RAISE EXCEPTION 'evening-checks.yml の manifest が期待値と不一致: repo=% ref=% schedule=% kind=% job_name=% enabled=%',
      v_row.repo, v_row.ref, v_row.schedule_utc, v_row.kind, v_row.job_name, v_row.enabled;
  END IF;
END $$;

-- pg_cron ジョブは manifest の schedule_utc と文字列完全一致させる（00125 の有効化条件）
SELECT cron.schedule('dispatch-evening-checks', '15 11 * * *', $$ SELECT ops.dispatch_by_name('evening-checks.yml') $$);

-- 2. 旧 7 本の dispatch を止める（jobname と command が両方一致する時だけ。再適用しても落ちない）。
--    同名でも別用途のジョブは触らない。別名で登録された旧 dispatch は 4 の事後検査で止める
DO $$
DECLARE
  v_pair record;
BEGIN
  FOR v_pair IN
    SELECT x.jobname, x.workflow_file
    FROM (VALUES
      ('dispatch-price-alert',         'price-alert.yml'),
      ('dispatch-overheat',            'overheat.yml'),
      ('dispatch-earnings-alert',      'earnings-alert.yml'),
      ('dispatch-entry-timing-signal', 'entry-timing-signal.yml'),
      ('dispatch-yutai-alert',         'yutai-alert.yml'),
      ('dispatch-holdings-news',       'holdings-news.yml'),
      ('dispatch-earnings-surprise',   'earnings-surprise.yml')
    ) AS x(jobname, workflow_file)
  LOOP
    IF EXISTS (
      SELECT 1 FROM cron.job j
      WHERE j.jobname = v_pair.jobname
        AND btrim(j.command) = format('SELECT ops.dispatch_by_name(%L)', v_pair.workflow_file)
    ) THEN
      PERFORM cron.unschedule(v_pair.jobname);
    END IF;
  END LOOP;
END $$;

-- 3. 旧 7 行は freshness 専用として残す（enabled=true・job_name・kind はそのまま）
UPDATE ops.expected_workflows
   SET schedule_utc = CASE kind WHEN 'weekday' THEN '15 11 * * 1-5' ELSE '15 11 * * *' END,
       deadline_jst = '21:00',
       notes = 'dispatch なし（evening-checks.yml の 1 ジョブ内で実行・00131）。未達検知（check_freshness）専用の行。'
               || 'disable すると未達検知が外れる／cron ジョブが無いので enable RPC は通らない'
 WHERE repo = 'JapanStockScouter'
   AND workflow_file IN (
     'price-alert.yml',
     'overheat.yml',
     'earnings-alert.yml',
     'entry-timing-signal.yml',
     'yutai-alert.yml',
     'holdings-news.yml',
     'earnings-surprise.yml'
   );

-- 4. 事後検査: 旧 7 本の dispatch ジョブが残っていない・新ジョブが 1 本だけある
DO $$
DECLARE
  v_left text;
  v_new  int;
BEGIN
  SELECT string_agg(j.jobname, ', ') INTO v_left
  FROM cron.job j
  WHERE btrim(j.command) IN (
    'SELECT ops.dispatch_by_name(''price-alert.yml'')',
    'SELECT ops.dispatch_by_name(''overheat.yml'')',
    'SELECT ops.dispatch_by_name(''earnings-alert.yml'')',
    'SELECT ops.dispatch_by_name(''entry-timing-signal.yml'')',
    'SELECT ops.dispatch_by_name(''yutai-alert.yml'')',
    'SELECT ops.dispatch_by_name(''holdings-news.yml'')',
    'SELECT ops.dispatch_by_name(''earnings-surprise.yml'')'
  );
  IF v_left IS NOT NULL THEN
    RAISE EXCEPTION '旧 dispatch ジョブが残っている（別名で登録されていた可能性）: %', v_left;
  END IF;

  SELECT count(*) INTO v_new FROM cron.job
  WHERE btrim(command) = 'SELECT ops.dispatch_by_name(''evening-checks.yml'')';
  IF v_new <> 1 THEN
    RAISE EXCEPTION 'evening-checks.yml の dispatch ジョブが % 本ある（1 本のはず）', v_new;
  END IF;
END $$;
