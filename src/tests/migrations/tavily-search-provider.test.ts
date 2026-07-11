/** Analyst Target MonitorへのTavily provider追加契約をSQL上で固定する。 */

import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

const migration = readFileSync(
  resolve(process.cwd(), 'supabase/migrations/00088_add_tavily_search_provider.sql'),
  'utf8'
);
const rollback = readFileSync(
  resolve(process.cwd(), 'supabase/rollbacks/00088_add_tavily_search_provider.down.sql'),
  'utf8'
);

describe('00088_add_tavily_search_provider.sql', () => {
  it('daily/rolling limitのCHECKをprovider別へ差し替える', () => {
    expect(migration).toContain(
      'DROP CONSTRAINT external_search_budget_guard_daily_limit_chk'
    );
    expect(migration).toContain(
      'DROP CONSTRAINT external_search_budget_guard_rolling_limit_chk'
    );
    expect(migration).toMatch(
      /ADD CONSTRAINT external_search_budget_guard_daily_limit_chk[\s\S]*provider = 'brave_search' AND daily_request_limit = 35[\s\S]*provider = 'tavily' AND daily_request_limit = 35/
    );
    expect(migration).toMatch(
      /ADD CONSTRAINT external_search_budget_guard_rolling_limit_chk[\s\S]*provider = 'brave_search' AND rolling_30d_request_limit = 900[\s\S]*provider = 'tavily' AND rolling_30d_request_limit = 900/
    );
  });

  it('tavily行をON CONFLICT DO NOTHINGで冪等に追加する', () => {
    expect(migration).toContain("VALUES ('tavily', 35, 900)");
    expect(migration).toContain('ON CONFLICT (provider) DO NOTHING');
  });

  it('rollbackはtavily行を削除しCHECKを単一provider版へ戻す', () => {
    expect(rollback).toContain("DELETE FROM scouter.external_search_budget_guard WHERE provider = 'tavily'");
    expect(rollback).toMatch(
      /DROP CONSTRAINT external_search_budget_guard_daily_limit_chk[\s\S]*ADD CONSTRAINT external_search_budget_guard_daily_limit_chk\s*\n\s*CHECK \(daily_request_limit = 35\)/
    );
    expect(rollback).toMatch(
      /DROP CONSTRAINT external_search_budget_guard_rolling_limit_chk[\s\S]*ADD CONSTRAINT external_search_budget_guard_rolling_limit_chk\s*\n\s*CHECK \(rolling_30d_request_limit = 900\)/
    );
    // DELETEがCHECK差し替えより先: 参照行があるFK RESTRICTでtrxごと中断させ、
    // 使用実績があるproviderのrollbackを安全側で拒否する。
    expect(rollback.indexOf('DELETE FROM')).toBeLessThan(
      rollback.indexOf('DROP CONSTRAINT external_search_budget_guard_daily_limit_chk')
    );
  });
});
