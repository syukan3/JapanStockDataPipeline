-- cron-b の起動時刻を JST 19:20 → 19:30（UTC 10:20 → 10:30）へ後ろ倒し。
-- 理由: 決算発表予定(announcement)の公開は ~19:00 JST。19:20 では余裕20分と薄いため、
--       定刻(pg_cron)起動でも安全なよう +10分の余裕を確保する。
-- cron-b.yml の GitHub schedule も同時に '30 10 * * *' へ変更済み。
-- cron.schedule は job_name で upsert（既存 dispatch-cron-b を置き換え）。
SELECT cron.schedule('dispatch-cron-b', '30 10 * * *', $$ SELECT ops.dispatch_by_name('cron-b.yml') $$);

-- manifest の表示用スケジュールも整合させる。
UPDATE ops.expected_workflows
  SET schedule_utc = '30 10 * * *'
  WHERE workflow_file = 'cron-b.yml';
