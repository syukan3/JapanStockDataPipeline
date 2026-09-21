-- 00129_register_lifeos_rate_watch.sql
-- ============================================================================
-- lifeOS の RateWatch（rate-watch.yml）を外部トリガー(pg_cron dispatch)へ載せる
-- ----------------------------------------------------------------------------
-- 背景:
--   00126 で lifeOS の配信系 10 本、00127 で EventWatch、00128 で JobWatch を
--   pg_cron dispatch へ移した。RateWatch はその後（2026-09-21）に追加された
--   13 本目で、日銀の長・短期プライムレート一覧と、三菱UFJ / 三井住友 /
--   ドコモSMTBネット銀行（旧・住信SBIネット銀行）/ 三井住友信託銀行が金利ページ用に
--   配信しているデータを毎週月曜 05:37 JST に読み、**短期プライムレートと
--   住宅ローン変動の店頭表示（基準）金利**の差分を検知してメールする。
--
--   GitHub schedule の実測遅延（2026-08 時点で最大 +8〜11 時間、高負荷時は
--   ジョブ破棄）を被った場合、**週次なので 1 回落ちると次の観測まで 1 週間空く**。
--   日次の 00127 / 00128 より 1 回あたりの欠測コストが大きい一方、金利自体は
--   数ヶ月動かないことが多いので、実害は「動いた週を 1 週遅れで知る」程度。
--   それでも 2028 年の住宅ローン実行を控えて短プラの改定を追う目的なので、
--   00126〜00128 と同じ理由で外部トリガーへ載せる。
--
-- 特記事項:
--   RateWatch は **差分 0 の週はメールを送らない**設計（金利は数ヶ月動かないのが
--   普通で、毎週「変化なし」を送ると読まれなくなる）。EventWatch / JobWatch と
--   同様に dispatch が静かに止まっても受信側からは気づけないので、検知は lifeOS 側の
--   lifeos-doctor（`github.rate-watch` heartbeat・毎日 08:00 JST・192h 閾値）が担う。
--   192h（8 日）は週次カデンス（168h）に 1 日の余裕を足した値。
--   ops 側では従来どおり ops.check_cron_health / ops.reconcile_dispatches が
--   dispatch 自体の失敗を拾う。
--
-- 時刻の根拠:
--   **`37 20 * * 0` は日曜 20:37 UTC = 月曜 05:37 JST**。JST は UTC+9 なので
--   曜日が 1 つ戻る（`* * 1` と書くと火曜朝になる。lifeOS 側の workflow で
--   実際にこの取り違えをして修正した）。
--   EventWatch（05:47）/ JobWatch（05:52）/ WeekAhead（06:00）より前に置き、
--   月曜の朝メールを読み始める時点で週初の金利差分が届いている状態にする。
--   :00/:15/:30/:45 は cron 混雑で遅延しやすいため :37 に置く（00126 と同じ方針）。
--
-- 前提(この migration を適用する前に満たすこと):
--   1. lifeOS の rate-watch.yml が dispatch_label input・run-name・guard job を
--      備えた状態で **main へ push 済み**であること。未 push または input 未宣言だと
--      GitHub は 404 / 422 を返して静かに落ちる（ops.dispatch_status の
--      status_code で確認する。204 = 成功）。
--      → 2026-09-21 のコミット 34f23fd7 で満たしている。
--   2. Vault の github_dispatch_pat が lifeOS リポジトリへ Actions:RW を持つこと。
--      00126 で既に満たされている（同一リポジトリ・同一 PAT）。
--
-- 運用: 00126 / 00127 / 00128 と同じ。GitHub schedule はフォールバックとして残置し、
--   二重実行は workflow 側の guard job が抑止する。lifeOS には Supabase の
--   job_runs 相当が無いため job_name / deadline_jst は NULL とし、
--   ops.check_freshness の対象外。
-- ============================================================================

INSERT INTO ops.expected_workflows
  (workflow_file, repo, friendly_name, schedule_utc, kind, deadline_jst, job_name, enabled, notes)
VALUES
  ('rate-watch.yml', 'lifeOS', 'Rate Watch', '37 20 * * 0', 'weekly', NULL, NULL, true,
   'lifeOS: 短プラ連動の住宅ローン変動金利の週次差分検知 月 05:37 JST（差分がある週だけメール送信）')
ON CONFLICT (workflow_file) DO NOTHING;

-- workflow_file は単独 PRIMARY KEY なので、将来 JapanStock 側に同名ファイルが
-- 生まれると DO NOTHING が黙って効き、pg_cron が別リポジトリへ dispatch してしまう。
-- repo だけでなく ref / schedule_utc / kind / enabled まで期待値と一致することを
-- 確認し、1 つでも違えば適用を失敗させる（fail closed）。00126〜00128 と同じ検査。
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
      ('rate-watch.yml', '37 20 * * 0', 'weekly')
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
  WHERE j.jobname = 'dispatch-lifeos-rate-watch'
    AND btrim(j.command) <> 'SELECT ops.dispatch_by_name(''rate-watch.yml'')';

  IF v_conflict IS NOT NULL THEN
    RAISE EXCEPTION 'dispatch-lifeos-rate-watch に想定外のジョブが既存: % (上書きを避けるため中止)', v_conflict;
  END IF;
END $$;

-- pg_cron ジョブは manifest の schedule_utc と文字列完全一致させる
-- （00125 の set_expected_workflow_enabled が一致を要求するため）。
SELECT cron.schedule('dispatch-lifeos-rate-watch', '37 20 * * 0', $$ SELECT ops.dispatch_by_name('rate-watch.yml') $$);
