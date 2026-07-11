/** Analyst Target Monitorの実行頻度を金曜週1回へ変更する契約をSQL上で固定する。 */

import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

const migration = readFileSync(
  resolve(process.cwd(), 'supabase/migrations/00089_analyst_target_monitor_weekly.sql'),
  'utf8'
);
const rollback = readFileSync(
  resolve(process.cwd(), 'supabase/rollbacks/00089_analyst_target_monitor_weekly.down.sql'),
  'utf8'
);

describe('00089_analyst_target_monitor_weekly.sql', () => {
  it('pg_cronを金曜週1回へ再スケジュールする', () => {
    expect(migration).toContain("cron.unschedule('dispatch-analyst-target-monitor')");
    expect(migration).toMatch(
      /cron\.schedule\(\s*'dispatch-analyst-target-monitor',\s*\n\s*'25 12 \* \* 5',/
    );
    expect(migration.indexOf('cron.unschedule')).toBeLessThan(migration.indexOf('cron.schedule'));
  });

  it('ops.expected_workflowsをweekly/deadline無しへ更新する', () => {
    expect(migration).toMatch(
      /UPDATE ops\.expected_workflows[\s\S]*schedule_utc = '25 12 \* \* 5'[\s\S]*kind = 'weekly'[\s\S]*deadline_jst = NULL[\s\S]*WHERE workflow_file = 'analyst-target-monitor\.yml'/
    );
  });

  it('rollbackは平日毎日21:25/weekday/deadline 22:00へ戻す', () => {
    expect(rollback).toMatch(
      /cron\.schedule\(\s*'dispatch-analyst-target-monitor',\s*\n\s*'25 12 \* \* 1-5',/
    );
    expect(rollback).toMatch(
      /UPDATE ops\.expected_workflows[\s\S]*schedule_utc = '25 12 \* \* 1-5'[\s\S]*kind = 'weekday'[\s\S]*deadline_jst = '22:00'[\s\S]*WHERE workflow_file = 'analyst-target-monitor\.yml'/
    );
  });
});
