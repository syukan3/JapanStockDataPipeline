/** jquants_core.dataset_coverage の公開スキーマ契約をSQL上で固定する。 */

import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

const migration = readFileSync(
  resolve(process.cwd(), 'supabase/migrations/00085_create_dataset_coverage.sql'),
  'utf8'
);
const rollback = readFileSync(
  resolve(
    process.cwd(),
    'supabase/rollbacks/00085_create_dataset_coverage.down.sql'
  ),
  'utf8'
);

describe('00085_create_dataset_coverage.sql', () => {
  it('必須カラムとdataset,target_date主キーを定義する', () => {
    expect(migration).toContain(
      'CREATE TABLE IF NOT EXISTS jquants_core.dataset_coverage'
    );
    for (const column of [
      'dataset',
      'target_date',
      'status',
      'row_count',
      'error_count',
      'source_observed_at',
      'run_id',
      'updated_at',
    ]) {
      expect(migration).toMatch(new RegExp(`\\b${column}\\b`));
    }
    expect(migration).toContain('PRIMARY KEY (dataset, target_date)');
    expect(migration).toContain("CHECK (status IN ('success', 'failed'))");
    expect(migration).toMatch(/run_id\s+uuid\s*,/);
    expect(migration).not.toMatch(/run_id\s+uuid\s+NOT NULL/);
    expect(migration).not.toMatch(/run_id[^\n]*REFERENCES/i);
    expect(migration).toContain('dataset_coverage_status_consistency');
    expect(migration).toMatch(
      /status = 'success' AND error_count = 0 AND source_observed_at IS NOT NULL/
    );
    expect(migration).toMatch(/status = 'failed' AND error_count > 0/);
  });

  it('service_roleのみALLとし、anon/authenticatedは直接参照させない', () => {
    expect(migration).toContain(
      'ALTER TABLE jquants_core.dataset_coverage ENABLE ROW LEVEL SECURITY'
    );
    expect(migration).toContain(
      'ALTER TABLE jquants_core.dataset_coverage FORCE ROW LEVEL SECURITY'
    );
    expect(migration).not.toMatch(/FOR SELECT TO authenticated/);
    expect(migration).toMatch(
      /FOR ALL TO service_role\s+USING \(true\)\s+WITH CHECK \(true\)/
    );
    expect(migration).toContain(
      'REVOKE ALL ON jquants_core.dataset_coverage FROM PUBLIC, anon, authenticated'
    );
    expect(migration).not.toMatch(/GRANT SELECT[^\n]*authenticated/);
    expect(migration).toContain(
      'GRANT ALL ON jquants_core.dataset_coverage TO service_role'
    );
  });

  it('manifestと実件数・銘柄eventを単一snapshotで返すservice-role限定RPCを定義する', () => {
    expect(migration).toContain(
      'CREATE OR REPLACE FUNCTION jquants_core.get_earnings_coverage('
    );
    for (const field of [
      'dataset',
      'target_date',
      'status',
      'row_count',
      'error_count',
      'source_observed_at',
      'run_id',
      'actual_count',
      'has_local_event',
    ]) {
      expect(migration).toMatch(new RegExp(`\\b${field}\\b`));
    }
    expect(migration).toMatch(/LANGUAGE sql\s+STABLE\s+SECURITY INVOKER/);
    expect(migration).toContain("SET search_path = ''");
    expect(migration).toMatch(
      /count\(\*\)::bigint AS actual_count[\s\S]*bool_or\(ec\.local_code = p_local_code\)[\s\S]*FROM jquants_core\.earnings_calendar ec/
    );
    expect(migration).toMatch(
      /FROM event_stats es\s+LEFT JOIN jquants_core\.dataset_coverage dc/
    );
    expect(migration).toContain(
      'REVOKE ALL ON FUNCTION jquants_core.get_earnings_coverage(date, text)'
    );
    expect(migration).toContain('FROM PUBLIC, anon, authenticated');
    expect(migration).toContain(
      'GRANT EXECUTE ON FUNCTION jquants_core.get_earnings_coverage(date, text)'
    );
    expect(migration).toContain('TO service_role');
  });

  it('job claimを行ロック内で原子的に判定しattempt_idとfailed fenceを更新する', () => {
    const start = migration.indexOf(
      'CREATE OR REPLACE FUNCTION jquants_ingest.claim_job_run('
    );
    const end = migration.indexOf(
      'CREATE OR REPLACE FUNCTION jquants_ingest.complete_job_run_attempt(',
      start
    );
    const body = migration.slice(start, end);

    expect(start).toBeGreaterThan(-1);
    expect(migration).toMatch(
      /ALTER TABLE jquants_ingest\.job_runs[\s\S]*ADD COLUMN IF NOT EXISTS attempt_id uuid NOT NULL DEFAULT gen_random_uuid\(\)/
    );
    expect(body).toMatch(
      /FROM jquants_ingest\.job_runs jr[\s\S]*FOR UPDATE;/
    );
    expect(body).toMatch(
      /v_status = 'failed'[\s\S]*v_status = 'running'[\s\S]*v_status = 'success'/
    );
    expect(body).toContain('attempt_id = v_new_attempt_id');
    expect(body).toMatch(
      /p_coverage_dataset[\s\S]*'failed'[\s\S]*ON CONFLICT \(dataset, target_date\) DO UPDATE/
    );
    expect(body).toMatch(
      /INSERT INTO jquants_ingest\.job_heartbeat[\s\S]*'running'/
    );
    expect(body).toMatch(
      /p_coverage_dataset = 'earnings_calendar'[\s\S]*p_job_name <> 'cron_b'/
    );
    expect(migration).toContain(
      'REVOKE ALL ON FUNCTION jquants_ingest.claim_job_run(text, date, jsonb, integer, integer, text)'
    );
  });

  it('current attemptだけが対象日全置換・exact count・success manifestを単一transactionでpublishする', () => {
    const start = migration.indexOf(
      'CREATE OR REPLACE FUNCTION jquants_core.commit_earnings_calendar_attempt('
    );
    const end = migration.indexOf(
      'REVOKE ALL ON FUNCTION jquants_core.fail_earnings_coverage_attempt',
      start
    );
    const body = migration.slice(start, end);

    expect(start).toBeGreaterThan(-1);
    expect(body).toMatch(
      /jr\.attempt_id = p_attempt_id[\s\S]*FOR UPDATE;/
    );
    expect(body).toMatch(
      /DELETE FROM jquants_core\.earnings_calendar[\s\S]*INSERT INTO jquants_core\.earnings_calendar[\s\S]*SELECT count\(\*\)::bigint[\s\S]*'success'/
    );
    expect(body).toContain('v_actual_count <> v_expected_count');
    expect(migration).toContain(
      'REVOKE ALL ON FUNCTION jquants_core.commit_earnings_calendar_attempt(date, uuid, uuid, timestamptz, jsonb)'
    );
    expect(migration).toContain(
      'GRANT EXECUTE ON FUNCTION jquants_core.commit_earnings_calendar_attempt(date, uuid, uuid, timestamptz, jsonb)'
    );
  });

  it('job完了とfinal heartbeatもcurrent attemptに限定する', () => {
    expect(migration).toContain(
      'CREATE OR REPLACE FUNCTION jquants_ingest.complete_job_run_attempt('
    );
    expect(migration).toMatch(
      /jr\.attempt_id = p_attempt_id[\s\S]*jr\.status = 'running'[\s\S]*INSERT INTO jquants_ingest\.job_heartbeat/
    );
    expect(migration).toContain(
      'REVOKE ALL ON FUNCTION jquants_ingest.complete_job_run_attempt(uuid, uuid, text, text, jsonb)'
    );
  });

  it('手動ロールバックSQLを持つ', () => {
    expect(rollback).toContain(
      'DROP TABLE IF EXISTS jquants_core.dataset_coverage'
    );
    expect(rollback).toContain(
      'DROP FUNCTION IF EXISTS jquants_core.get_earnings_coverage(date, text)'
    );
    expect(rollback).toContain(
      'DROP FUNCTION IF EXISTS jquants_core.commit_earnings_calendar_attempt(date, uuid, uuid, timestamptz, jsonb)'
    );
    expect(rollback).toContain(
      'DROP FUNCTION IF EXISTS jquants_ingest.claim_job_run(text, date, jsonb, integer, integer, text)'
    );
    expect(rollback).toContain(
      'ALTER TABLE jquants_ingest.job_runs DROP COLUMN IF EXISTS attempt_id'
    );
  });
});
