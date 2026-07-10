/** earnings calendar writerがCron Bの公開プロトコルを迂回しないことを固定する。 */

import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

const seedSource = readFileSync(
  resolve(process.cwd(), 'scripts/seed/earnings.ts'),
  'utf8'
);

describe('earnings seed writer contract', () => {
  it('Cron B route経由で実行し、sync関数を直接呼ばない', () => {
    expect(seedSource).toContain(
      "import('../../src/app/api/cron/jquants/b/route')"
    );
    expect(seedSource).not.toContain('syncEarningsCalendar(');
  });
});
