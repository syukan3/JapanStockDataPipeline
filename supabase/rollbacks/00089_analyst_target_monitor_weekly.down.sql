-- 00089_analyst_target_monitor_weekly.sql の手動ロールバック。
-- 平日毎日(JST 21:25)のdispatchとops manifestへ戻す。

SELECT cron.unschedule('dispatch-analyst-target-monitor');

SELECT cron.schedule(
  'dispatch-analyst-target-monitor',
  '25 12 * * 1-5',
  $$ SELECT ops.dispatch_by_name('analyst-target-monitor.yml') $$
);

UPDATE ops.expected_workflows
SET
  schedule_utc = '25 12 * * 1-5',
  kind = 'weekday',
  deadline_jst = '22:00',
  notes = 'GitHub schedule運用中はfreshness対象外。cutover前にGitHub scheduleを削除してからenabled=trueにする。'
WHERE workflow_file = 'analyst-target-monitor.yml';
