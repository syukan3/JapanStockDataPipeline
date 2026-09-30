/**
 * 00131: Scouter の夕方 7 本の dispatch を evening-checks.yml 1 本へ集約する migration の契約。
 * 旧 7 行は dispatch なし・未達検知（check_freshness）専用で残すので、
 * 「cron ジョブは外すが manifest 行は enabled のまま」「締切を新しい起動時刻の後ろへ」を固定する。
 */

import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

const migration = readFileSync(
  resolve(process.cwd(), 'supabase/migrations/00131_consolidate_scouter_evening_checks.sql'),
  'utf8'
);
const rollback = readFileSync(
  resolve(process.cwd(), 'supabase/rollbacks/00131_consolidate_scouter_evening_checks.down.sql'),
  'utf8'
);

/** 行コメントを除いた実 SQL */
const sql = (text: string) =>
  text
    .split('\n')
    .filter((line) => !line.trimStart().startsWith('--'))
    .join('\n');

const OLD = [
  { file: 'price-alert.yml', job: 'dispatch-price-alert', name: 'scouter-price-alert', kind: 'daily', schedule: '10 10 * * *', deadline: '19:40' },
  { file: 'overheat.yml', job: 'dispatch-overheat', name: 'scouter-overheat', kind: 'daily', schedule: '40 10 * * *', deadline: '20:40' },
  { file: 'earnings-alert.yml', job: 'dispatch-earnings-alert', name: 'scouter-earnings-alert', kind: 'daily', schedule: '0 11 * * *', deadline: '20:30' },
  { file: 'entry-timing-signal.yml', job: 'dispatch-entry-timing-signal', name: 'scouter-entry-timing-signal', kind: 'weekday', schedule: '10 11 * * 1-5', deadline: '20:40' },
  { file: 'yutai-alert.yml', job: 'dispatch-yutai-alert', name: 'scouter-yutai-alert', kind: 'daily', schedule: '15 11 * * *', deadline: '20:45' },
  { file: 'holdings-news.yml', job: 'dispatch-holdings-news', name: 'scouter-holdings-news', kind: 'weekday', schedule: '30 11 * * 1-5', deadline: '21:00' },
  { file: 'earnings-surprise.yml', job: 'dispatch-earnings-surprise', name: 'scouter-earnings-surprise', kind: 'daily', schedule: '45 11 * * *', deadline: '21:15' },
];

describe('00131_consolidate_scouter_evening_checks.sql', () => {
  const body = sql(migration);

  it('evening-checks.yml を dispatch 専用行（job_name なし）として 20:15 JST に登録する', () => {
    expect(body).toMatch(
      /\('evening-checks\.yml', 'JapanStockScouter', 'Evening Checks', '15 11 \* \* \*', 'daily', NULL, NULL, true,/
    );
    expect(body).toContain(
      "SELECT cron.schedule('dispatch-evening-checks', '15 11 * * *', $$ SELECT ops.dispatch_by_name('evening-checks.yml') $$);"
    );
    // 同名ジョブの上書きを避ける fail closed
    expect(body).toContain("WHERE j.jobname = 'dispatch-evening-checks'");
  });

  it('旧 7 本の cron ジョブを外し（jobname と command が一致する時だけ）、事後に残っていないことを確かめる', () => {
    for (const { job, file } of OLD) {
      expect(body, job).toMatch(new RegExp(`\\('${job}',\\s+'${file.replace('.', '\\.')}'\\)`));
      expect(body, file).toContain(`'SELECT ops.dispatch_by_name(''${file}'')'`);
    }
    expect(body).toContain("AND btrim(j.command) = format('SELECT ops.dispatch_by_name(%L)', v_pair.workflow_file)");
    expect(body).toContain('PERFORM cron.unschedule(v_pair.jobname)');
    // 旧 8 本を再登録しない
    for (const { job } of OLD) {
      expect(body, job).not.toContain(`cron.schedule('${job}'`);
    }
  });

  it('旧 7 行は削除も disable もせず、未達検知の締切を 20:15 起動の後ろへ直す', () => {
    expect(body).not.toMatch(/DELETE\s+FROM\s+ops\.expected_workflows/i);
    expect(body).not.toMatch(/SET\s+enabled\s*=\s*false/i);
    const update = body.slice(body.indexOf('UPDATE ops.expected_workflows'));
    expect(update).toContain("deadline_jst = '21:00'");
    expect(update).not.toMatch(/SET[\s\S]*enabled\s*=/);
    expect(update).not.toMatch(/SET[\s\S]*job_name\s*=/);
    expect(update).toContain("CASE kind WHEN 'weekday' THEN '15 11 * * 1-5' ELSE '15 11 * * *' END");
    for (const { file } of OLD) expect(update, file).toContain(`'${file}'`);
    // 21:00 は 20:15 起動より後（HH:MM の文字列比較）
    expect('21:00' > '20:15').toBe(true);
  });

  it('yutai-cross（17:00 の在庫アラート）には触れない', () => {
    expect(body).not.toContain('yutai-cross-screening.yml');
    expect(body).not.toContain('dispatch-yutai-cross');
    expect(sql(rollback)).not.toContain('yutai-cross');
  });

  it('適用前に旧 7 行の job_name / kind / enabled を検査する（fail closed）', () => {
    for (const { file, name, kind } of OLD) {
      expect(body, file).toMatch(new RegExp(`\\('${file.replace('.', '\\.')}',\\s+'${name}',\\s+'${kind}'\\)`));
    }
    expect(body).toContain("RAISE EXCEPTION 'Scouter の旧 manifest が期待値と不一致");
  });
});

describe('00131 rollback', () => {
  const body = sql(rollback);

  it('evening-checks の dispatch と行を消し、旧 7 本を元の時刻で再登録する', () => {
    expect(body).toContain("SELECT cron.unschedule('dispatch-evening-checks');");
    expect(body).toMatch(/DELETE FROM ops\.expected_workflows[\s\S]*workflow_file = 'evening-checks\.yml'/);
    for (const { file, job, schedule, deadline } of OLD) {
      expect(body, job).toContain(
        `cron.schedule('${job}',`
      );
      expect(body, job).toMatch(
        new RegExp(`cron\\.schedule\\('${job}',\\s+'${schedule.replace(/\*/g, '\\*')}',\\s+\\$\\$ SELECT ops\\.dispatch_by_name\\('${file.replace('.', '\\.')}'\\) \\$\\$\\);`)
      );
      expect(body, file).toMatch(
        new RegExp(`\\('${file.replace('.', '\\.')}',\\s+'${schedule.replace(/\*/g, '\\*')}',\\s+'${deadline}',`)
      );
    }
  });
});
