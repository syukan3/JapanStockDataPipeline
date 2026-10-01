/**
 * J-Quants の書き込み処理が source='jquants' を明示的に書くこと（00133）
 *
 * upsert の DO UPDATE では列の DEFAULT が効かない。代替行（生成カレンダー・推計TOPIX・JPX）を
 * 公式値で上書きしたときに source が代替のまま残らないよう、mapper が必ず 'jquants' を返す。
 */

import { describe, it, expect } from 'vitest';
import { toTradingCalendarRecord } from '@/lib/jquants/endpoints/trading-calendar';
import { toTopixBarDailyRecord } from '@/lib/jquants/endpoints/index-topix';
import { toInvestorTypeTradingRecords } from '@/lib/jquants/endpoints/investor-types';
import type { InvestorTypeTradingItem } from '@/lib/jquants/types';

describe('source 列（00133）', () => {
  it('取引カレンダー', () => {
    expect(toTradingCalendarRecord({ Date: '2026-10-01', HolDiv: '1' }).source).toBe('jquants');
  });

  it('TOPIX', () => {
    expect(
      toTopixBarDailyRecord({ Date: '2026-10-01', O: 1, H: 2, L: 0.5, C: 1.5 }).source
    ).toBe('jquants');
  });

  it('投資部門別（全レコード）', () => {
    const item = {
      PubDate: '2026-09-29',
      StDate: '2026-09-14',
      EnDate: '2026-09-18',
      Section: 'TSEPrime',
      FrgnSell: 1,
      FrgnBuy: 2,
      FrgnTot: 3,
      FrgnBal: 1,
    } as unknown as InvestorTypeTradingItem;
    const records = toInvestorTypeTradingRecords(item);
    expect(records.length).toBeGreaterThan(0);
    expect(records.every((r) => r.source === 'jquants')).toBe(true);
  });
});
