-- 00126_register_lifeos_dispatch.down.sql
-- lifeOS の外部トリガー登録を取り消す。
-- 取り消すと lifeOS 10 本は GitHub schedule 単独に戻る（= 遅延・欠測のリスクが復活）。
-- ワークフロー側の guard job は「外部トリガーの実行が見つからない」warning を出しつつ
-- 常に本処理を走らせる fail-open なので、この rollback 単独で配信は止まらない。

SELECT cron.unschedule('dispatch-lifeos-daily-brief');
SELECT cron.unschedule('dispatch-lifeos-blog-check');
SELECT cron.unschedule('dispatch-lifeos-dashboard-deploy');
SELECT cron.unschedule('dispatch-lifeos-watch-check');
SELECT cron.unschedule('dispatch-lifeos-week-ahead');
SELECT cron.unschedule('dispatch-lifeos-journal-review');
SELECT cron.unschedule('dispatch-lifeos-nakajima-summary');
SELECT cron.unschedule('dispatch-lifeos-life-dashboard');
SELECT cron.unschedule('dispatch-lifeos-monthly-reminder');
SELECT cron.unschedule('dispatch-lifeos-monthly-finance');

-- この migration が対象とする 10 行だけを消す。repo='lifeOS' 一括だと、別 migration が
-- 後から足した lifeOS 登録まで巻き込んで消してしまう。
-- 注: up 側は ON CONFLICT DO NOTHING だが、直後の assertion が ref/schedule_utc/kind/
-- enabled まで期待値一致を要求するため、既存行が温存されていたとしても内容は
-- この migration が入れるものと同一であることが保証されている。よって無条件 DELETE で可。
DELETE FROM ops.expected_workflows
 WHERE repo = 'lifeOS'
   AND workflow_file IN (
     'daily-brief.yml','blog-check.yml','dashboard-deploy.yml','watch-check.yml',
     'week-ahead.yml','journal-review.yml','nakajima-summary.yml','life-dashboard.yml',
     'monthly-reminder.yml','monthly-finance.yml'
   );
