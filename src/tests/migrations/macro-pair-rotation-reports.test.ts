/** マクロ対立軸週次レポートのimmutable snapshot / outbox / Ops契約をSQL上で固定する。 */

import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

const migration = readFileSync(
  resolve(process.cwd(), 'supabase/migrations/00123_create_macro_pair_rotation_reports.sql'),
  'utf8'
);
const rollback = readFileSync(
  resolve(process.cwd(), 'supabase/rollbacks/00123_create_macro_pair_rotation_reports.down.sql'),
  'utf8'
);
const schemaBytes = readFileSync(
  resolve(process.cwd(), 'docs/contracts/macro-pair-rotation-v1.schema.json')
);
const fixtureBytes = readFileSync(
  resolve(process.cwd(), 'docs/contracts/fixtures/macro-pair-rotation-v1.json')
);
const CONTRACT_HASH = 'a7f79568c12767e7ebeb4174c48c12264a959fd380a2f3e26c5fe43cb951073e';

function section(startMarker: string, endMarker: string): string {
  const start = migration.indexOf(startMarker);
  const end = migration.indexOf(endMarker, start);
  expect(start).toBeGreaterThan(-1);
  expect(end).toBeGreaterThan(start);
  return migration.slice(start, end);
}

describe('00123_create_macro_pair_rotation_reports.sql', () => {
  it('versioned schemaの同一byte列と固定hashをDB契約に含める', () => {
    expect(createHash('sha256').update(schemaBytes).digest('hex')).toBe(CONTRACT_HASH);
    expect(migration).toContain(`contract_hash = '${CONTRACT_HASH}'`);
    const schema = JSON.parse(schemaBytes.toString('utf8')) as {
      properties: { schemaVersion: { const: number }; axes: { minItems: number; maxItems: number } };
      $defs: { axis: { properties: { axisId: { enum: string[] } } } };
    };
    expect(schema.properties.schemaVersion.const).toBe(1);
    expect(schema.properties.axes).toMatchObject({ minItems: 4, maxItems: 4 });
    expect(schema.$defs.axis.properties.axisId.enum).toEqual(['rates', 'oil', 'fx', 'risk']);
    const fixture = JSON.parse(fixtureBytes.toString('utf8')) as {
      schemaVersion: number;
      contractHash: string;
      axes: Array<{ axisId: string }>;
    };
    expect(fixture.schemaVersion).toBe(1);
    expect(fixture.contractHash).toBe(CONTRACT_HASH);
    expect(fixture.axes.map((axis) => axis.axisId)).toEqual(['rates', 'oil', 'fx', 'risk']);
  });

  it('report_period_dateを土曜の週次PK、as-of/comparisonを別列にする', () => {
    expect(migration).toContain('CREATE TABLE scouter.macro_pair_rotation_reports');
    expect(migration).toMatch(/report_period_date\s+DATE PRIMARY KEY/);
    for (const column of [
      'as_of_date',
      'comparison_date',
      'schema_version',
      'strategy_version',
      'information_cutoff_at',
      'summary',
      'axes',
      'caveats',
      'contract_hash',
      'generated_at',
    ]) {
      expect(migration).toMatch(new RegExp(`\\b${column}\\b`));
    }
    expect(migration).toContain('extract(isodow FROM report_period_date) = 6');
    expect(migration).toContain('comparison_date < as_of_date AND as_of_date <= report_period_date');
    expect(migration).toContain('CHECK (schema_version = 1)');
  });

  it('schema v1は既知4軸を各1件・正しいdriver・unvalidatedで固定する', () => {
    expect(migration).toContain('jsonb_array_length(axes) = 4');
    for (const [axisId, seriesId] of [
      ['rates', 'mof_jgb_20y'],
      ['oil', 'DCOILWTICO'],
      ['fx', 'DEXJPUS'],
      ['risk', 'VIXCLS'],
    ]) {
      expect(migration).toContain(`@.axisId == "${axisId}"`);
      expect(migration).toContain(
        `@.axisId == "${axisId}" && @.driver.seriesId == "${seriesId}"`
      );
    }
    expect(migration).toContain('@.validationStatus == "unvalidated"');
  });

  it('report insertが同一transactionで週キー由来のpending outboxを1件作る', () => {
    expect(migration).toContain('CREATE TABLE scouter.macro_pair_rotation_email_outbox');
    expect(migration).toMatch(
      /report_period_date\s+DATE PRIMARY KEY[\s\S]*REFERENCES scouter\.macro_pair_rotation_reports\(report_period_date\)/
    );
    expect(migration).toContain('idempotency_key     TEXT NOT NULL UNIQUE');
    expect(migration).toContain(
      "format('macro-pair-rotation:%s:%s', NEW.report_period_date, NEW.strategy_version)"
    );
    const triggerAt = migration.indexOf('CREATE TRIGGER trg_macro_pair_rotation_create_outbox');
    const rlsAt = migration.indexOf('ALTER TABLE scouter.macro_pair_rotation_reports ENABLE ROW LEVEL SECURITY');
    expect(triggerAt).toBeGreaterThan(-1);
    expect(triggerAt).toBeLessThan(rlsAt);
  });

  it('reportはUPDATE/DELETE/TRUNCATEを拒否し、outboxも削除できない', () => {
    expect(migration).toContain('BEFORE UPDATE OR DELETE ON scouter.macro_pair_rotation_reports');
    expect(migration).toContain('BEFORE TRUNCATE ON scouter.macro_pair_rotation_reports');
    expect(migration).toContain('BEFORE DELETE ON scouter.macro_pair_rotation_email_outbox');
    expect(migration).toContain('BEFORE TRUNCATE ON scouter.macro_pair_rotation_email_outbox');
    expect(migration).toContain("ERRCODE = '55000'");
    for (const fn of [
      'scouter.create_macro_pair_rotation_outbox()',
      'scouter.reject_macro_pair_rotation_report_mutation()',
      'scouter.reject_macro_pair_rotation_outbox_delete()',
    ]) {
      expect(migration).toContain(
        `REVOKE ALL ON FUNCTION ${fn} FROM PUBLIC, anon, authenticated, service_role`
      );
    }
  });

  it('全roleから剥奪後、service_roleへreport SELECT/INSERTとoutbox SELECTだけを許す', () => {
    for (const table of [
      'scouter.macro_pair_rotation_reports',
      'scouter.macro_pair_rotation_email_outbox',
    ]) {
      expect(migration).toContain(`ALTER TABLE ${table} ENABLE ROW LEVEL SECURITY`);
      expect(migration).toContain(`ALTER TABLE ${table} FORCE ROW LEVEL SECURITY`);
      expect(migration).toMatch(
        new RegExp(
          `REVOKE ALL ON TABLE ${table.replace('.', '\\.')}` +
            `[\\s\\S]*FROM PUBLIC, anon, authenticated, service_role`
        )
      );
    }
    expect(migration).toContain(
      'GRANT SELECT, INSERT ON TABLE scouter.macro_pair_rotation_reports TO service_role'
    );
    expect(migration).toContain(
      'GRANT SELECT ON TABLE scouter.macro_pair_rotation_email_outbox TO service_role'
    );
    expect(migration).not.toMatch(
      /GRANT (UPDATE|DELETE|TRUNCATE|ALL)[^;]*macro_pair_rotation/i
    );
  });

  it('latest-storedマクロ8点を固定4系列・単一SQL statement snapshotで返す', () => {
    const body = section(
      'CREATE OR REPLACE FUNCTION scouter.get_macro_pair_rotation_macro_snapshot(',
      '-- ============================================================================\n-- 6. Outbox state transition RPCs'
    );
    expect(body).toMatch(/LANGUAGE sql\s+STABLE\s+SECURITY DEFINER\s+SET search_path = ''/);
    expect(body).toContain('statement_timestamp() AS cutoff');
    expect(body).toContain('CROSS JOIN series s');
    expect(body).toContain('CROSS JOIN points p');
    expect(body).toContain('LEFT JOIN LATERAL');
    expect(body).toContain('mid.indicator_date <= p.target_date');
    expect(body).toContain('mid.value IS NOT NULL');
    expect(body).toContain('ORDER BY mid.indicator_date DESC');
    for (const id of ['mof_jgb_20y', 'DCOILWTICO', 'DEXJPUS', 'VIXCLS']) {
      expect(body).toContain(`'${id}'::text`);
    }
    expect(body).not.toContain('released_at <=');
  });

  it('通常claimはpendingだけをattempt+lease付きsendingへCASする', () => {
    const body = section(
      'CREATE OR REPLACE FUNCTION scouter.claim_macro_pair_rotation_email(',
      'CREATE OR REPLACE FUNCTION scouter.finalize_macro_pair_rotation_email('
    );
    expect(body).toContain("status = 'sending'");
    expect(body).toContain('attempt_id = v_attempt_id');
    expect(body).toContain('attempt_count = o.attempt_count + 1');
    expect(body).toContain('make_interval(secs => p_lease_seconds)');
    expect(body).toMatch(
      /WHERE o\.report_period_date = p_report_period_date\s+AND o\.status = 'pending'/
    );
    expect(body).not.toMatch(/o\.status IN \([^)]*failed/);
    expect(body).toContain('to_jsonb(r)');
  });

  it('finalizeは同一sending attemptだけをsent/failed/unknownへ終端化する', () => {
    const body = section(
      'CREATE OR REPLACE FUNCTION scouter.finalize_macro_pair_rotation_email(',
      'CREATE OR REPLACE FUNCTION scouter.retry_macro_pair_rotation_email('
    );
    expect(body).toContain("p_outcome NOT IN ('sent','failed','unknown')");
    expect(body).toMatch(
      /WHERE o\.report_period_date = p_report_period_date[\s\S]*AND o\.status = 'sending'[\s\S]*AND o\.attempt_id = p_attempt_id/
    );
    expect(body).toContain('RETURN coalesce(v_updated, false)');
  });

  it('failedだけを24時間内・同一frozen envelopeで明示retryする', () => {
    const body = section(
      'CREATE OR REPLACE FUNCTION scouter.retry_macro_pair_rotation_email(',
      'CREATE OR REPLACE FUNCTION scouter.reconcile_macro_pair_rotation_email('
    );
    expect(body).toContain("v_row.status <> 'failed'");
    expect(body).toContain("v_row.created_at < v_now - interval '24 hours'");
    for (const field of ['payload_hash', 'recipient', 'subject', 'render_version']) {
      expect(body).toContain(`v_row.${field}`);
    }
    expect(body).toContain('retry envelope differs from the frozen first-attempt envelope');
    expect(body).toContain('attempt_id = v_attempt_id');
  });

  it('unknownだけを受付証跡付きでreconcileし、期限切れsendingをattempt+lease CASでunknownへ隔離する', () => {
    const reconcile = section(
      'CREATE OR REPLACE FUNCTION scouter.reconcile_macro_pair_rotation_email(',
      'CREATE OR REPLACE FUNCTION scouter.recover_expired_macro_pair_rotation_emails('
    );
    expect(reconcile).toContain("AND o.status = 'unknown'");
    expect(reconcile).toContain('reconciliation evidence is required');
    expect(reconcile).toContain("status = 'sent'");

    const recover = section(
      'CREATE OR REPLACE FUNCTION scouter.recover_expired_macro_pair_rotation_emails()',
      '-- Exact signatures: revoke implicit PUBLIC execute'
    );
    expect(recover).toContain("o.status = 'sending'");
    expect(recover).toContain('o.lease_expires_at < clock_timestamp()');
    expect(recover).toContain('FOR UPDATE SKIP LOCKED');
    expect(recover).toContain('AND o.attempt_id = e.attempt_id');
    expect(recover).toContain('AND o.lease_expires_at = e.lease_expires_at');
    expect(recover).toContain("status = 'unknown'");
  });

  it('全SECURITY DEFINER RPCを全roleから剥奪し必要な6本だけservice_roleへ戻す', () => {
    const signatures = [
      'scouter.get_macro_pair_rotation_macro_snapshot(date, date)',
      'scouter.claim_macro_pair_rotation_email(date, text, text, text, text, integer)',
      'scouter.finalize_macro_pair_rotation_email(date, uuid, text, text, text)',
      'scouter.retry_macro_pair_rotation_email(date, text, text, text, text, integer)',
      'scouter.reconcile_macro_pair_rotation_email(date, text, text)',
      'scouter.recover_expired_macro_pair_rotation_emails()',
    ];
    for (const signature of signatures) {
      expect(migration).toContain(
        `REVOKE ALL ON FUNCTION ${signature}\n  FROM PUBLIC, anon, authenticated, service_role`
      );
      expect(migration).toContain(`GRANT EXECUTE ON FUNCTION ${signature} TO service_role`);
    }
  });

  it('job名、disabled manifest、土曜10:30の唯一の定時dispatchを登録する', () => {
    expect(migration).toContain("'scouter-macro-pair-rotation-report'");
    expect(migration).toContain("'macro-pair-rotation-report.yml'");
    expect(migration).toContain("'JapanStockScouter'");
    expect(migration).toContain("'Weekly Macro Pair Rotation Report'");
    expect(migration).toContain("'30 1 * * 6'");
    expect(migration).toContain("'weekly'");
    expect(migration).toMatch(
      /'scouter-macro-pair-rotation-report',\s*\n\s*false,/
    );
    expect(migration).not.toContain('ON CONFLICT (workflow_file)');
    expect(migration).toContain("'dispatch-macro-pair-rotation-report'");
    expect(migration).toContain("SELECT ops.dispatch_by_name('macro-pair-rotation-report.yml')");
    expect(migration).toContain(
      "btrim(v_command) <> 'SELECT ops.dispatch_by_name(''macro-pair-rotation-report.yml'')'"
    );
    expect(migration).not.toMatch(/command (?:NOT )?LIKE '%ops\.dispatch_by_name/);
  });

  it('週次freshnessはdisabled no-op後、有効時だけlease回収→当週/as-of未達を通知する', () => {
    const body = section(
      'CREATE OR REPLACE FUNCTION ops.check_macro_pair_rotation_weekly_freshness()',
      'REVOKE ALL ON FUNCTION ops.check_macro_pair_rotation_weekly_freshness()'
    );
    const disabledAt = body.indexOf('IF NOT w.enabled THEN');
    const recoverAt = body.indexOf('PERFORM scouter.recover_expired_macro_pair_rotation_emails()');
    const notifyAt = body.indexOf('IF ops.notify(');
    expect(disabledAt).toBeGreaterThan(-1);
    expect(recoverAt).toBeGreaterThan(disabledAt);
    expect(notifyAt).toBeGreaterThan(recoverAt);
    for (const exact of [
      "w.repo <> 'JapanStockScouter'",
      "w.ref <> 'main'",
      "w.schedule_utc <> '30 1 * * 6'",
      "w.kind <> 'weekly'",
      "w.job_name <> 'scouter-macro-pair-rotation-report'",
    ]) {
      expect(body).toContain(exact);
    }
    expect(body).toContain('max(c.calendar_date)');
    expect(body).toContain('c.is_business_day = true');
    expect(body).toContain('r.report_period_date = v_period');
    expect(body).toContain('r.as_of_date = v_expected_as_of');
    expect(body).toContain("o.status = 'sent'");
    // 送信実績を job_runs.status='success' で代替しない（競合送信者検出時もjobはsuccessで終わるため）
    expect(body).not.toContain('jquants_ingest.job_runs');
    expect(body).not.toContain("jr.status = 'success'");
    expect(migration).toContain("'ops-macro-pair-rotation-freshness'");
    expect(migration).toContain("'15 2 * * 6'");
    expect(body).toContain(
      "btrim(j.command) = 'SELECT ops.dispatch_by_name(''macro-pair-rotation-report.yml'')'"
    );
    expect(migration).toContain(
      "btrim(v_command) <> 'SELECT ops.check_macro_pair_rotation_weekly_freshness()'"
    );
  });

  it('rollbackはrunning/sendingを拒否し、cron→manifest→RPC→table順で戻す', () => {
    expect(rollback).toContain('LOCK TABLE jquants_ingest.job_runs IN ACCESS EXCLUSIVE MODE');
    expect(rollback).toMatch(
      /jr\.job_name = 'scouter-macro-pair-rotation-report'[\s\S]*jr\.status = 'running'[\s\S]*RAISE EXCEPTION/
    );
    expect(rollback).toMatch(
      /scouter\.macro_pair_rotation_email_outbox[\s\S]*o\.status = 'sending'[\s\S]*RAISE EXCEPTION/
    );
    const unscheduleAt = rollback.indexOf("cron.unschedule('dispatch-macro-pair-rotation-report')");
    const manifestDeleteAt = rollback.indexOf('DELETE FROM ops.expected_workflows');
    const dropRpcAt = rollback.indexOf(
      'DROP FUNCTION IF EXISTS scouter.recover_expired_macro_pair_rotation_emails()'
    );
    const dropTableAt = rollback.indexOf(
      'DROP TABLE IF EXISTS scouter.macro_pair_rotation_email_outbox'
    );
    expect(unscheduleAt).toBeGreaterThan(-1);
    expect(manifestDeleteAt).toBeGreaterThan(unscheduleAt);
    expect(dropRpcAt).toBeGreaterThan(manifestDeleteAt);
    expect(dropTableAt).toBeGreaterThan(dropRpcAt);
    expect(rollback).toContain('NOT VALID');
    expect(rollback).toContain(
      "btrim(v_command) <> 'SELECT ops.dispatch_by_name(''macro-pair-rotation-report.yml'')'"
    );
    expect(rollback).toContain(
      "btrim(v_command) <> 'SELECT ops.check_macro_pair_rotation_weekly_freshness()'"
    );
    const restored = rollback.slice(rollback.indexOf('ADD CONSTRAINT job_runs_job_name_check'));
    expect(restored).not.toContain("'scouter-macro-pair-rotation-report'");
    expect(restored).toContain("'scouter-overheat'");
  });
});
