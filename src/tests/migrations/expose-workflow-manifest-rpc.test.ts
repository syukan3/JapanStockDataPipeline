/**
 * カナリアトグル用RPCの契約をSQL上で固定する。
 * opsスキーマを公開せずにmanifestを操作するための橋渡しなので、
 * 「service_role以外へ漏れない」「既存行のUPDATEに限る」「有効化はfail closed」を守る。
 */

import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

const migration = readFileSync(
  resolve(process.cwd(), 'supabase/migrations/00125_expose_workflow_manifest_rpc.sql'),
  'utf8'
);
const rollback = readFileSync(
  resolve(process.cwd(), 'supabase/rollbacks/00125_expose_workflow_manifest_rpc.down.sql'),
  'utf8'
);

/** 行コメントを除いた実SQL。句の個数を数えるときは解説文を拾わないようにする。 */
const statements = migration
  .split('\n')
  .filter((line) => !line.trimStart().startsWith('--'))
  .join('\n');

describe('00125_expose_workflow_manifest_rpc.sql', () => {
  it('公開スキーマ側にRPCを置き、opsスキーマ自体は公開しない', () => {
    expect(migration).toContain('CREATE OR REPLACE FUNCTION jquants_ingest.list_expected_workflows(');
    expect(migration).toContain('CREATE OR REPLACE FUNCTION jquants_ingest.set_expected_workflow_enabled(');
    // ops を PostgREST へ公開する操作（スキーマ単位のgrant）を混ぜない
    expect(migration).not.toMatch(/GRANT\s+USAGE\s+ON\s+SCHEMA\s+ops/i);
    expect(migration).not.toMatch(/GRANT[\s\S]{0,80}ON\s+ops\.expected_workflows/i);
  });

  it('両RPCがSECURITY DEFINER・search_path=空・完全修飾である', () => {
    const definers = statements.match(/SECURITY DEFINER/g) ?? [];
    const searchPaths = statements.match(/SET search_path = ''/g) ?? [];
    expect(definers).toHaveLength(2);
    expect(searchPaths).toHaveLength(2);
    expect(migration).toContain('FROM ops.expected_workflows');
    expect(migration).toContain('FROM cron.job j');
  });

  it('EXECUTEをservice_roleだけに与える', () => {
    for (const signature of [
      'jquants_ingest.list_expected_workflows(text)',
      'jquants_ingest.set_expected_workflow_enabled(text, boolean)',
    ]) {
      expect(migration).toContain(
        `REVOKE ALL ON FUNCTION ${signature}\n  FROM PUBLIC, anon, authenticated, service_role;`
      );
      expect(migration).toContain(`GRANT EXECUTE ON FUNCTION ${signature} TO service_role;`);
    }
    // revoke より後に grant すること（順序が逆だと権限が残らない）
    expect(migration.lastIndexOf('REVOKE ALL ON FUNCTION')).toBeLessThan(
      migration.indexOf('GRANT EXECUTE ON FUNCTION')
    );
  });

  it('setterは既存manifestのUPDATEに限り、INSERTしない', () => {
    expect(migration).toMatch(/RAISE EXCEPTION 'unknown workflow_file/);
    expect(migration).toMatch(/UPDATE ops\.expected_workflows/);
    expect(migration).not.toMatch(/INSERT INTO ops\.expected_workflows/);
  });

  it('対象行をロックしてから判定し、UPDATE...RETURNINGの実値を返す', () => {
    // 判定に使った行と実際に更新する行を食い違わせない
    expect(statements).toContain('FOR UPDATE');
    expect(migration.indexOf('FOR UPDATE')).toBeLessThan(
      migration.indexOf('UPDATE ops.expected_workflows')
    );
    // 返却値は更新前に読んだ変数ではなく RETURNING の実値（同値時もUPDATEを通す）
    expect(migration).toMatch(
      /RETURNING e\.workflow_file, e\.friendly_name, e\.enabled\s*\n\s*INTO v_updated/
    );
    expect(migration).toMatch(
      /RETURN QUERY\s*\n\s*SELECT v_updated\.workflow_file, v_updated\.friendly_name, v_updated\.enabled/
    );
    expect(migration).toMatch(/RETURNING[\s\S]{0,120}INTO v_updated;\s*\n[\s\S]{0,120}IF NOT FOUND THEN/);
  });

  it('dispatch cronを集約して数え、重複登録では有効化しない', () => {
    // LEFT JOIN のままだと重複cronでmanifest行が増え、判定も1件を任意に選んでしまう
    expect(migration).toContain('LEFT JOIN LATERAL');
    expect(migration).toMatch(/count\(\*\)::integer/);
    expect(migration).toMatch(/v_cron_count > 1[\s\S]{0,220}RAISE EXCEPTION 'multiple pg_cron dispatch jobs/);
  });

  it('有効化時だけガードを効かせる（無効化は事故対応なので常に通す）', () => {
    expect(migration).toMatch(/IF p_enabled THEN[\s\S]*no pg_cron dispatch job for/);
    expect(migration).toMatch(/v_cron_schedule IS DISTINCT FROM w\.schedule_utc/);
    const guardAt = migration.indexOf('IF p_enabled THEN');
    const updateAt = migration.indexOf('UPDATE ops.expected_workflows');
    expect(guardAt).toBeGreaterThan(-1);
    expect(guardAt).toBeLessThan(updateAt);
  });

  it('rollbackは両RPCを落とすだけでmanifestの値を書き換えない', () => {
    expect(rollback).toContain(
      'DROP FUNCTION IF EXISTS jquants_ingest.set_expected_workflow_enabled(text, boolean);'
    );
    expect(rollback).toContain(
      'DROP FUNCTION IF EXISTS jquants_ingest.list_expected_workflows(text);'
    );
    expect(rollback).not.toMatch(/UPDATE ops\.expected_workflows/);
  });
});
