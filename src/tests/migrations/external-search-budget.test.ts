/** Analyst Target Monitorの外部検索予算・Ops契約をSQL上で固定する。 */

import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

const budgetMigration = readFileSync(
  resolve(
    process.cwd(),
    'supabase/migrations/00086_create_external_search_budget.sql'
  ),
  'utf8'
);
const budgetRollback = readFileSync(
  resolve(
    process.cwd(),
    'supabase/rollbacks/00086_create_external_search_budget.down.sql'
  ),
  'utf8'
);
const opsMigration = readFileSync(
  resolve(
    process.cwd(),
    'supabase/migrations/00087_register_analyst_target_monitor.sql'
  ),
  'utf8'
);
const opsRollback = readFileSync(
  resolve(
    process.cwd(),
    'supabase/rollbacks/00087_register_analyst_target_monitor.down.sql'
  ),
  'utf8'
);

function functionBody(startMarker: string, endMarker: string): string {
  const start = budgetMigration.indexOf(startMarker);
  const end = budgetMigration.indexOf(endMarker, start);
  expect(start).toBeGreaterThan(-1);
  expect(end).toBeGreaterThan(start);
  return budgetMigration.slice(start, end);
}

function tableDefinitionForRequests(): string {
  const start = budgetMigration.indexOf(
    'CREATE TABLE scouter.external_search_requests'
  );
  const end = budgetMigration.indexOf(
    'COMMENT ON TABLE scouter.external_search_requests',
    start
  );
  expect(start).toBeGreaterThan(-1);
  expect(end).toBeGreaterThan(start);
  return budgetMigration.slice(start, end);
}

describe('00086_create_external_search_budget.sql', () => {
  it('job_runsへanalyst target monitorのjob名を追加する', () => {
    expect(budgetMigration).toContain(
      "'scouter-analyst-target-monitor'"
    );
    expect(budgetMigration).toMatch(
      /DROP CONSTRAINT IF EXISTS job_runs_job_name_check[\s\S]*ADD CONSTRAINT job_runs_job_name_check/
    );
    for (const existingName of [
      'scouter-holdings-news',
      'scouter-earnings-alert',
      'scouter-factor-paper',
      'scouter-price-alert',
      'scouter-earnings-surprise',
    ]) {
      expect(budgetMigration).toContain(`'${existingName}'`);
    }
  });

  it('provider guardとsafe outcomeだけを持つrequest ledgerを作る', () => {
    expect(budgetMigration).toContain(
      'CREATE TABLE scouter.external_search_budget_guard'
    );
    expect(budgetMigration).toContain(
      'CREATE TABLE scouter.external_search_requests'
    );
    expect(budgetMigration).toContain(
      'reservation_id    bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY'
    );
    for (const requiredColumn of ['provider', 'scan_date', 'local_code']) {
      expect(tableDefinitionForRequests()).toMatch(
        new RegExp(`\\b${requiredColumn}\\s+\\w+\\s+NOT NULL`)
      );
    }
    expect(budgetMigration).toContain(
      'UNIQUE (provider, scan_date, local_code)'
    );
    expect(budgetMigration).toContain(
      'REFERENCES scouter.external_search_budget_guard(provider)'
    );

    const tableDefinition = tableDefinitionForRequests();
    for (const safeOutcome of [
      'succeeded',
      'http_error',
      'timeout',
      'invalid_response',
    ]) {
      expect(tableDefinition).toContain(`'${safeOutcome}'`);
    }
    for (const forbiddenField of [
      'query_text',
      'response_body',
      'source_url',
      'title',
      'snippet',
    ]) {
      expect(tableDefinition).not.toContain(forbiddenField);
    }
    expect(tableDefinition).toMatch(
      /outcome IS NULL AND http_status_class IS NULL AND completed_at IS NULL/
    );
    expect(tableDefinition).toMatch(
      /outcome IS NOT NULL[\s\S]*completed_at IS NOT NULL/
    );
  });

  it('DB server時刻でJST日次35・実時間30日900をhard ceilingにする', () => {
    expect(budgetMigration).toContain('CHECK (daily_request_limit = 35)');
    expect(budgetMigration).toContain(
      'CHECK (rolling_30d_request_limit = 900)'
    );
    expect(budgetMigration).toContain("VALUES ('brave_search', 35, 900)");

    const reserve = functionBody(
      'CREATE OR REPLACE FUNCTION scouter.reserve_external_search_request(',
      'CREATE OR REPLACE FUNCTION scouter.complete_external_search_request('
    );
    expect(reserve).toContain('v_now timestamptz;');
    const guardLock = reserve.indexOf(
      'FROM scouter.external_search_budget_guard g'
    );
    const serverNow = reserve.indexOf('v_now := clock_timestamp();');
    const quotaClock = reserve.indexOf("timezone('Asia/Tokyo', v_now)");
    expect(serverNow).toBeGreaterThan(guardLock);
    expect(quotaClock).toBeGreaterThan(serverNow);
    expect(reserve).toContain("timezone('Asia/Tokyo', v_now)");
    expect(reserve).toMatch(
      /r\.reserved_at >= v_day_start[\s\S]*r\.reserved_at < v_day_end/
    );
    expect(reserve).toContain("v_now - interval '720 hours'");
    expect(reserve).not.toContain("v_now - interval '30 days'");
    expect(reserve).toMatch(/v_daily_used >= v_daily_limit/);
    expect(reserve).toMatch(/v_rolling_used >= v_rolling_limit/);
    expect(reserve).toMatch(/v_daily_used \+ 1/);
    expect(reserve).toMatch(/v_rolling_used \+ 1/);
    expect(reserve).not.toMatch(/p_reserved_at|p_daily_limit|p_rolling_limit/);
  });

  it('current job attemptをfenceしてからguardをFOR UPDATEし、予約を原子的に挿入する', () => {
    const reserve = functionBody(
      'CREATE OR REPLACE FUNCTION scouter.reserve_external_search_request(',
      'CREATE OR REPLACE FUNCTION scouter.complete_external_search_request('
    );
    const jobLock = reserve.indexOf('FROM jquants_ingest.job_runs jr');
    const guardLock = reserve.indexOf(
      'FROM scouter.external_search_budget_guard g'
    );
    const requestInsert = reserve.indexOf(
      'INSERT INTO scouter.external_search_requests'
    );

    expect(jobLock).toBeGreaterThan(-1);
    expect(guardLock).toBeGreaterThan(jobLock);
    expect(requestInsert).toBeGreaterThan(guardLock);
    expect(reserve.slice(jobLock, guardLock)).toContain('FOR UPDATE');
    expect(reserve.slice(guardLock, requestInsert)).toContain('FOR UPDATE');
    expect(reserve).toMatch(
      /jr\.run_id = p_run_id[\s\S]*jr\.job_name = 'scouter-analyst-target-monitor'[\s\S]*jr\.target_date = p_scan_date[\s\S]*jr\.status = 'running'[\s\S]*jr\.attempt_id = p_attempt_id/
    );
    const firstQuotaCount = reserve.indexOf('SELECT count(*)::integer');
    expect(firstQuotaCount).toBeGreaterThan(guardLock);
    expect(reserve).toContain("'already_reserved'::text");
    expect(reserve).toContain("'daily_limit_reached'::text");
    expect(reserve).toContain("'rolling_limit_reached'::text");
    expect(reserve).toContain("'reserved'::text");
  });

  it('完了RPCもcurrent attemptに限定し、安全な結果以外を拒否する', () => {
    const complete = functionBody(
      'CREATE OR REPLACE FUNCTION scouter.complete_external_search_request(',
      'REVOKE ALL ON FUNCTION scouter.reserve_external_search_request('
    );
    expect(complete).toMatch(
      /FROM jquants_ingest\.job_runs jr[\s\S]*jr\.run_id = p_run_id[\s\S]*jr\.job_name = 'scouter-analyst-target-monitor'[\s\S]*jr\.status = 'running'[\s\S]*jr\.attempt_id = p_attempt_id[\s\S]*FOR UPDATE/
    );
    expect(complete).toMatch(
      /WHERE r\.reservation_id = p_reservation_id[\s\S]*r\.run_id = p_run_id[\s\S]*r\.attempt_id = p_attempt_id[\s\S]*r\.outcome IS NULL/
    );
    expect(complete).toContain('RETURN false');
    expect(complete).toContain(
      "p_outcome NOT IN ('succeeded', 'http_error', 'timeout', 'invalid_response')"
    );
  });

  it('table直接DMLを拒否し、service roleには専用RPCだけを許可する', () => {
    for (const table of [
      'scouter.external_search_budget_guard',
      'scouter.external_search_requests',
    ]) {
      expect(budgetMigration).toContain(
        `ALTER TABLE ${table} ENABLE ROW LEVEL SECURITY`
      );
      expect(budgetMigration).toContain(
        `ALTER TABLE ${table} FORCE ROW LEVEL SECURITY`
      );
      expect(budgetMigration).toMatch(
        new RegExp(
          `REVOKE ALL ON TABLE ${table.replace('.', '\\.')}` +
            `[\\s\\S]*FROM PUBLIC, anon, authenticated, service_role`
        )
      );
    }
    expect(budgetMigration).not.toMatch(
      /GRANT (SELECT|INSERT|UPDATE|DELETE|ALL) ON (TABLE )?scouter\.external_search_/i
    );
    for (const signature of [
      'scouter.reserve_external_search_request(text, date, text, uuid, uuid)',
      'scouter.complete_external_search_request(bigint, uuid, uuid, text, smallint)',
    ]) {
      expect(budgetMigration).toMatch(
        new RegExp(
          `REVOKE ALL ON FUNCTION ${signature.replace(/[().]/g, '\\$&')}` +
            `[\\s\\S]*FROM PUBLIC, anon, authenticated, service_role;` +
            `[\\s\\S]*GRANT EXECUTE ON FUNCTION ${signature.replace(/[().]/g, '\\$&')}` +
            `[\\s\\S]*TO service_role;`
        )
      );
    }
    expect(
      budgetMigration.match(/SECURITY DEFINER\s+SET search_path = ''/g)
    ).toHaveLength(2);
  });

  it('手動rollbackでRPC・ledgerを落とし、旧job名制約を復元する', () => {
    expect(budgetRollback).toContain(
      'DROP FUNCTION IF EXISTS scouter.complete_external_search_request(bigint, uuid, uuid, text, smallint)'
    );
    expect(budgetRollback).toContain(
      'DROP FUNCTION IF EXISTS scouter.reserve_external_search_request(text, date, text, uuid, uuid)'
    );
    expect(budgetRollback).toMatch(
      /DROP TABLE IF EXISTS scouter\.external_search_requests[\s\S]*DROP TABLE IF EXISTS scouter\.external_search_budget_guard/
    );
    const restoredConstraint = budgetRollback.slice(
      budgetRollback.indexOf('ADD CONSTRAINT job_runs_job_name_check')
    );
    expect(restoredConstraint).not.toContain(
      "'scouter-analyst-target-monitor'"
    );
    for (const existingName of [
      'cron_a',
      'cron_b',
      'cron_c',
      'scouter-high-dividend',
      'cron-d-macro',
      'scouter-macro-regime',
      'scouter-macro-ai',
      'cron-e-yutai',
      'scouter-yutai-cross',
      'db-archival',
      'scouter-signal-performance',
      'scouter-growth-signal',
      'scouter-macro-ai-meta',
      'scouter-holdings-news',
      'scouter-earnings-alert',
      'scouter-factor-paper',
      'scouter-price-alert',
      'scouter-earnings-surprise',
    ]) {
      expect(restoredConstraint).toContain(`'${existingName}'`);
    }
    expect(budgetRollback).toContain('NOT VALID');
    const transactionStart = budgetRollback.indexOf('BEGIN;');
    const jobRunsLock = budgetRollback.indexOf(
      'LOCK TABLE jquants_ingest.job_runs IN ACCESS EXCLUSIVE MODE;'
    );
    const runningCheck = budgetRollback.indexOf(
      "jr.job_name = 'scouter-analyst-target-monitor'"
    );
    const firstDrop = budgetRollback.indexOf('DROP FUNCTION IF EXISTS');
    const transactionCommit = budgetRollback.lastIndexOf('COMMIT;');
    expect(transactionStart).toBeGreaterThan(-1);
    expect(jobRunsLock).toBeGreaterThan(transactionStart);
    expect(runningCheck).toBeGreaterThan(jobRunsLock);
    expect(firstDrop).toBeGreaterThan(runningCheck);
    expect(transactionCommit).toBeGreaterThan(firstDrop);
    expect(budgetRollback).toMatch(
      /jr\.job_name = 'scouter-analyst-target-monitor'[\s\S]*jr\.status = 'running'[\s\S]*RAISE EXCEPTION 'analyst target monitor is still running; drain it before rollback'/
    );
  });
});

describe('00087_register_analyst_target_monitor.sql', () => {
  it('manifestをenabled=falseでJST 21:25相当へ登録する', () => {
    for (const value of [
      "'analyst-target-monitor.yml'",
      "'JapanStockScouter'",
      "'Analyst Target Monitor'",
      "'25 12 * * 1-5'",
      "'weekday'",
      "'22:00'",
      "'scouter-analyst-target-monitor'",
    ]) {
      expect(opsMigration).toContain(value);
    }
    expect(opsMigration).toMatch(
      /'scouter-analyst-target-monitor',\s*false,/
    );
    expect(opsMigration).not.toContain('ON CONFLICT (workflow_file)');
    expect(opsMigration).toMatch(
      /FROM cron\.job j[\s\S]*j\.jobname = 'dispatch-analyst-target-monitor'[\s\S]*RAISE EXCEPTION/
    );
    expect(opsMigration).toContain(
      "'dispatch-analyst-target-monitor'"
    );
    expect(opsMigration).toContain(
      "SELECT ops.dispatch_by_name('analyst-target-monitor.yml')"
    );
  });

  it('rollbackはcronを先にunscheduleしてからmanifestを削除する', () => {
    const unschedule = opsRollback.indexOf(
      "cron.unschedule('dispatch-analyst-target-monitor')"
    );
    const manifestDelete = opsRollback.indexOf(
      'DELETE FROM ops.expected_workflows'
    );
    expect(unschedule).toBeGreaterThan(-1);
    expect(manifestDelete).toBeGreaterThan(unschedule);
    expect(opsRollback).toContain(
      "WHERE workflow_file = 'analyst-target-monitor.yml'"
    );
  });
});
