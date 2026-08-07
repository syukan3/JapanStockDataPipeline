/** 収集レーティング履歴（閲覧専用）のDB契約をSQL上で固定する。 */

import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

const tableMigration = readFileSync(
  resolve(process.cwd(), 'supabase/migrations/00113_create_analyst_rating_history.sql'),
  'utf8'
);
const tableRollback = readFileSync(
  resolve(process.cwd(), 'supabase/rollbacks/00113_create_analyst_rating_history.down.sql'),
  'utf8'
);
const rpcMigration = readFileSync(
  resolve(process.cwd(), 'supabase/migrations/00114_analyst_rating_history_rpc.sql'),
  'utf8'
);
const rpcRollback = readFileSync(
  resolve(process.cwd(), 'supabase/rollbacks/00114_analyst_rating_history_rpc.down.sql'),
  'utf8'
);
const alphanumericMigration = readFileSync(
  resolve(process.cwd(), 'supabase/migrations/00119_local_code_alphanumeric.sql'),
  'utf8'
);

describe('00113_create_analyst_rating_history.sql', () => {
  it('event fingerprintをPKにして再収集を1行へ収束させる', () => {
    expect(tableMigration).toContain('CREATE TABLE IF NOT EXISTS scouter.analyst_rating_history');
    expect(tableMigration).toMatch(/event_fingerprint\s+TEXT PRIMARY KEY/);
    expect(tableMigration).toContain("CHECK (event_fingerprint ~ '^[0-9a-f]{64}$')");
  });

  it('価格・コード・出典URLの妥当性をDB側でも縛る', () => {
    // 00113 時点の条件。英字入りコード対応で 00119 が上書きしている（下の describe）
    expect(tableMigration).toContain("CHECK (local_code ~ '^\\d{5}$')");
    expect(tableMigration).toContain('target_price          NUMERIC(12,2) NOT NULL CHECK (target_price > 0)');
    expect(tableMigration).toContain("source_url ~ '^https://'");
  });

  it('service_roleだけがアクセスでき、anon/authenticatedはREVOKEする', () => {
    expect(tableMigration).toContain(
      'ALTER TABLE scouter.analyst_rating_history ENABLE ROW LEVEL SECURITY'
    );
    expect(tableMigration).toContain('FOR ALL TO service_role');
    expect(tableMigration).toContain(
      'REVOKE ALL ON scouter.analyst_rating_history FROM anon, authenticated'
    );
  });

  it('銘柄×公表日で読むためのindexを張る', () => {
    expect(tableMigration).toContain(
      'ON scouter.analyst_rating_history (local_code, published_on DESC)'
    );
  });

  it('rollbackはテーブルを落とす', () => {
    expect(tableRollback).toContain('DROP TABLE IF EXISTS scouter.analyst_rating_history');
  });
});

describe('00114_analyst_rating_history_rpc.sql', () => {
  it('service_role限定のSECURITY DEFINERで、検索pathを固定する', () => {
    expect(rpcMigration).toContain('CREATE OR REPLACE FUNCTION scouter.record_analyst_rating_history');
    expect(rpcMigration).toContain('SECURITY DEFINER');
    expect(rpcMigration).toContain("SET search_path = ''");
    expect(rpcMigration).toContain("IF (SELECT auth.role()) IS DISTINCT FROM 'service_role' THEN");
    expect(rpcMigration).toContain(') TO service_role;');
    expect(rpcMigration).toMatch(/REVOKE ALL ON FUNCTION scouter\.record_analyst_rating_history[\s\S]*FROM PUBLIC, anon, authenticated/);
  });

  it('run_id + attempt_idでfenceし、失効attemptは副作用なしでfalseを返す', () => {
    expect(rpcMigration).toContain('FROM jquants_ingest.job_runs jr');
    expect(rpcMigration).toContain('AND jr.attempt_id = p_attempt_id');
    expect(rpcMigration).toContain("AND jr.status = 'running'");
    expect(rpcMigration).toContain('AND jr.target_date = p_scan_date');
    expect(rpcMigration).toContain('FOR UPDATE');
    expect(rpcMigration).toMatch(/IF NOT FOUND THEN\s+RETURN false;/);
    // fenceを通す前にupsert/deleteしない
    const fenceAt = rpcMigration.indexOf('FROM jquants_ingest.job_runs jr');
    expect(rpcMigration.indexOf('INSERT INTO scouter.analyst_rating_history')).toBeGreaterThan(fenceAt);
    expect(rpcMigration.indexOf('DELETE FROM scouter.analyst_rating_history')).toBeGreaterThan(fenceAt);
  });

  it('保持期間外・未来日の行は入口で拒否し、既存の古い行はpruneする', () => {
    expect(rpcMigration).toContain('v_cutoff := p_scan_date - p_retention_days;');
    expect(rpcMigration).toContain("WHERE (r.value ->> 'published_on')::date >= v_cutoff");
    expect(rpcMigration).toContain("AND (r.value ->> 'published_on')::date <= p_scan_date");
    expect(rpcMigration).toMatch(
      /DELETE FROM scouter\.analyst_rating_history h[\s\S]*AND h\.published_on < v_cutoff/
    );
  });

  it('pruneは今回取得できた銘柄だけに効かせる', () => {
    expect(rpcMigration).toContain('WHERE h.local_code = ANY (p_local_codes)');
    expect(rpcMigration).toContain('IF p_local_codes IS NOT NULL AND array_length(p_local_codes, 1) > 0 THEN');
  });

  it('upsertはfirst_seen_atを上書きしない', () => {
    const conflictAt = rpcMigration.indexOf('ON CONFLICT (event_fingerprint) DO UPDATE');
    expect(conflictAt).toBeGreaterThan(-1);
    const updateClause = rpcMigration.slice(conflictAt, rpcMigration.indexOf(';', conflictAt));
    // コメント内の言及は許すが、代入は許さない
    expect(updateClause).not.toMatch(/first_seen_at\s*=/);
    expect(updateClause).toContain('last_seen_at          = EXCLUDED.last_seen_at');
  });

  it('retention_daysの範囲を検証する', () => {
    expect(rpcMigration).toContain(
      'IF p_retention_days IS NULL OR p_retention_days <= 0 OR p_retention_days > 3650 THEN'
    );
  });

  it('rollbackは関数を落とす', () => {
    expect(rpcRollback).toContain('DROP FUNCTION IF EXISTS scouter.record_analyst_rating_history');
  });
});

describe('00119_local_code_alphanumeric.sql', () => {
  it('英字入り銘柄コード(285A0)を通すCHECKへ緩める', () => {
    // JPX の新形式は4桁目だけが英字。旧条件 '^\d{5}$' / '^[0-9]{4,5}$' の上位集合に留める
    expect(alphanumericMigration).toContain("CHECK (local_code ~ '^[0-9]{3}[0-9A-Z][0-9]$')");
    expect(alphanumericMigration).toContain("CHECK (local_code ~ '^[0-9]{3}[0-9A-Z][0-9]?$')");
  });

  it('外部検索予約RPCのguardも同時に緩める', () => {
    expect(alphanumericMigration).toContain("p_local_code !~ '^[0-9]{3}[0-9A-Z][0-9]?$'");
  });

  it('小文字・4桁目以外の英字は通さない条件になっている', () => {
    expect('285A0').toMatch(/^[0-9]{3}[0-9A-Z][0-9]$/);
    expect('285a0').not.toMatch(/^[0-9]{3}[0-9A-Z][0-9]$/);
    expect('2A5A0').not.toMatch(/^[0-9]{3}[0-9A-Z][0-9]$/);
  });
});
