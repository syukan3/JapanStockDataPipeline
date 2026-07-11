-- 00089_analyst_target_monitor_weekly.sql
-- Analyst Target Monitorの実行頻度を平日毎日から金曜週1回へ変更する。
--
-- 背景: 外部検索providerの無料枠を他プロジェクト(Tavily利用中のlifeOS等)と
-- 共有しているため、日次実行を止めて消費を抑える。DBの予約RPC・budget guardは
-- 元々「callerがどれだけ呼んでもDB固定のhard ceilingを超えない」設計のため、
-- 頻度を落としても安全性契約に変更はない。
--
-- GitHub Actions側のschedule cron('25 12 * * 1-5' → '25 12 * * 5')は
-- 別途 .github/workflows/analyst-target-monitor.yml で変更する(このmigrationは
-- pg_cron dispatchとops manifestだけを対象にする)。
--
-- kind='weekly'にすることで ops.check_freshness() の対象(daily/weekdayのみ)から
-- 外れる(db-archival.ymlと同じ扱い)。deadline_jstもfreshness判定専用のため
-- NULLにする。

SELECT cron.unschedule('dispatch-analyst-target-monitor');

SELECT cron.schedule(
  'dispatch-analyst-target-monitor',
  '25 12 * * 5',
  $$ SELECT ops.dispatch_by_name('analyst-target-monitor.yml') $$
);

UPDATE ops.expected_workflows
SET
  schedule_utc = '25 12 * * 5',
  kind = 'weekly',
  deadline_jst = NULL,
  notes = 'GitHub schedule運用中はfreshness対象外。cutover前にGitHub scheduleを削除してからenabled=trueにする。2026-07-12: 外部検索providerの無料枠節約のため平日毎日→金曜週1回へ変更。'
WHERE workflow_file = 'analyst-target-monitor.yml';
