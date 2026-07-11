-- 00087_register_analyst_target_monitor.sql の手動ロールバック。
-- workflowを先に無効化してから実行する。

SELECT cron.unschedule('dispatch-analyst-target-monitor');

DELETE FROM ops.expected_workflows
WHERE workflow_file = 'analyst-target-monitor.yml';
