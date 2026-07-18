/**
 * basket-valuation（バスケット割安判定の計算ロジック）のテスト
 *
 * 重点: キャップ水充填の収束・境界 / PIT選定の disclosed_date 境界とFYフォールバック /
 *       分割補正の向き（1→5分割で eps ×0.2） / 調和集計（赤字含む） / 指数連結
 */
import { describe, it, expect } from 'vitest';
import {
  extractSplitEvents,
  cumulativeAdjustmentFactor,
  addOneYear,
  buildPitFinancials,
  pitFy,
  pitForwardEps,
  waterFillCap,
  resolveConstituentWeights,
  effectiveCoverageWeight,
  diffSectorConstituents,
  buildConstituentDay,
  aggregateBasketDay,
  chainIndexSeries,
  pearsonCorrelation,
  annualizedTrackingError,
  type SlimBar,
  type RawDisclosure,
  type ConstituentDay,
  type PitFinancials,
  type AnchorMcapInput,
} from '@/lib/analytics/basket-valuation';

// ============================================================
// 分割補正
// ============================================================

describe('extractSplitEvents', () => {
  const bar = (date: string, adjFactor: number | null): SlimBar => ({
    date,
    close: 100,
    adjClose: 100,
    adjFactor,
  });

  it('factor=1/null/0以下を除外し、日付昇順で返す', () => {
    const events = extractSplitEvents([
      bar('2026-06-29', 0.2),
      bar('2026-06-26', 1),
      bar('2026-06-25', null),
      bar('2026-01-05', 2),
      bar('2026-03-01', 0),
    ]);
    expect(events).toEqual([
      { date: '2026-01-05', factor: 2 },
      { date: '2026-06-29', factor: 0.2 },
    ]);
  });

  it('同日重複は先勝ちで1件にする', () => {
    const events = extractSplitEvents([bar('2026-06-29', 0.2), bar('2026-06-29', 0.2)]);
    expect(events).toHaveLength(1);
  });
});

describe('cumulativeAdjustmentFactor', () => {
  // 実データ検証済み: 日東紡績 31100 の 1→5 分割は効力発生日 2026-06-29 に factor=0.2
  const events = [{ date: '2026-06-29', factor: 0.2 }];

  it('区間 (from, to] にイベントを含む（境界: to 当日は含む）', () => {
    expect(cumulativeAdjustmentFactor(events, '2026-04-30', '2026-06-29')).toBe(0.2);
  });

  it('from 当日のイベントは含まない（開示値に織り込み済み扱い）', () => {
    expect(cumulativeAdjustmentFactor(events, '2026-06-29', '2026-07-17')).toBe(1);
  });

  it('イベントが区間外なら 1', () => {
    expect(cumulativeAdjustmentFactor(events, '2026-04-30', '2026-06-26')).toBe(1);
    expect(cumulativeAdjustmentFactor(events, '2026-06-30', '2026-07-17')).toBe(1);
  });

  it('分割補正の向き: 分割前開示の eps は ×factor で分割後価格と比較可能になる', () => {
    // 開示時 eps=1000・株価20000（PER20倍）。1→5 分割後の株価4000に対し
    // eps_adj = 1000 × 0.2 = 200 → PER = 4000/200 = 20倍 で不変
    const cum = cumulativeAdjustmentFactor(events, '2026-04-30', '2026-07-01');
    const epsAdj = 1000 * cum;
    expect(epsAdj).toBeCloseTo(200);
    expect(4000 / epsAdj).toBeCloseTo(20);
    // 株式数は ÷factor（時価総額不変: 4000 × 500株 = 20000 × 100株）
    expect(100 / cum).toBeCloseTo(500);
  });

  it('複数イベントは累積積', () => {
    const multi = [
      { date: '2025-01-06', factor: 0.5 },
      { date: '2026-06-29', factor: 0.2 },
    ];
    expect(cumulativeAdjustmentFactor(multi, '2024-12-31', '2026-07-01')).toBeCloseTo(0.1);
  });
});

describe('addOneYear', () => {
  it('通常日はそのまま1年後', () => {
    expect(addOneYear('2025-03-31')).toBe('2026-03-31');
  });
  it('閏日は月末にクランプ', () => {
    expect(addOneYear('2024-02-29')).toBe('2025-02-28');
  });
});

// ============================================================
// PIT 財務系列
// ============================================================

describe('buildPitFinancials / pitFy / pitForwardEps', () => {
  const row = (over: Partial<RawDisclosure>): RawDisclosure => ({
    disclosed_date: null,
    disclosed_time: null,
    period_type: null,
    sales: null,
    net_income: null,
    eps: null,
    bps: null,
    dividend_annual: null,
    forecast_eps: null,
    next_forecast_eps: null,
    shares_outstanding_fy: null,
    fiscal_year_end: null,
    ...over,
  });

  // TEL 80350 の実開示パターンを模したフィクスチャ
  const rows: RawDisclosure[] = [
    row({
      disclosed_date: '2025-04-30', period_type: 'FY', fiscal_year_end: '2025-03-31',
      sales: 1000, eps: 100, bps: 400, dividend_annual: 50, shares_outstanding_fy: 10,
      next_forecast_eps: 120, // FY短信は forecast_eps NULL・来期は next_forecast_eps
    }),
    row({
      disclosed_date: '2025-07-31', period_type: '1Q', fiscal_year_end: '2026-03-31',
      sales: 300, eps: 30, forecast_eps: 125, shares_outstanding_fy: 10,
    }),
    row({
      // 予想修正開示（sales NULL・period_type FY・自年度向け forecast_eps）
      disclosed_date: '2025-09-15', period_type: 'FY', fiscal_year_end: '2026-03-31',
      forecast_eps: 130,
    }),
    row({
      disclosed_date: '2026-04-30', period_type: 'FY', fiscal_year_end: '2026-03-31',
      sales: 1100, eps: 128, bps: 450, dividend_annual: 60, shares_outstanding_fy: 10,
      // next_forecast_eps NULL（ガイダンス未公表）
    }),
  ];
  const pit = buildPitFinancials(rows);

  it('FY系列は period_type=FY かつ sales 非NULL のみ（予想修正・四半期を除外）', () => {
    expect(pit.fy.map((r) => r.disclosedDate)).toEqual(['2025-04-30', '2026-04-30']);
  });

  it('pitFy: disclosed_date 境界（当日開示は当日から有効）', () => {
    expect(pitFy(pit.fy, '2025-04-29')).toBeNull();
    expect(pitFy(pit.fy, '2025-04-30')?.fiscalYearEnd).toBe('2025-03-31');
    expect(pitFy(pit.fy, '2026-04-29')?.fiscalYearEnd).toBe('2025-03-31');
    expect(pitFy(pit.fy, '2026-04-30')?.eps).toBe(128);
  });

  it('pitFy: 同一年度の訂正開示は新しい開示が勝つ', () => {
    const correction = buildPitFinancials([
      row({ disclosed_date: '2025-04-30', period_type: 'FY', fiscal_year_end: '2025-03-31', sales: 1000, eps: 100 }),
      row({ disclosed_date: '2025-06-10', period_type: 'FY', fiscal_year_end: '2025-03-31', sales: 1000, eps: 95 }),
    ]);
    expect(pitFy(correction.fy, '2025-06-10')?.eps).toBe(95);
  });

  it('pitForwardEps: FY短信直後は next_forecast_eps を採用', () => {
    expect(pitForwardEps(pit, '2025-05-01')).toEqual({
      forecastEps: 120,
      disclosedDate: '2025-04-30',
    });
  });

  it('pitForwardEps: 四半期開示・予想修正で forecast_eps に更新される', () => {
    expect(pitForwardEps(pit, '2025-08-01')?.forecastEps).toBe(125);
    expect(pitForwardEps(pit, '2025-10-01')?.forecastEps).toBe(130);
  });

  it('pitForwardEps: 実績化した予想は無効（FY実績後にガイダンス無しなら null）', () => {
    // 2026-04-30 のFY実績（2026-03-31期）で 130 の予想は実績化。next も無いので null
    expect(pitForwardEps(pit, '2026-05-01')).toBeNull();
  });

  it('pitForwardEps: 開示前は null', () => {
    expect(pitForwardEps(pit, '2025-04-29')).toBeNull();
  });
});

// ============================================================
// キャップ水充填
// ============================================================

describe('waterFillCap', () => {
  it('上限違反が無ければ生シェアのまま', () => {
    const result = waterFillCap([
      { code: 'A', rawShare: 0.10, capLimit: 0.15 },
      { code: 'B', rawShare: 0.60, capLimit: 0.65 },
      { code: 'C', rawShare: 0.30, capLimit: 0.35 },
    ]);
    expect(result.get('A')).toBeCloseTo(0.1);
    expect(result.get('B')).toBeCloseTo(0.6);
    expect(result.get('C')).toBeCloseTo(0.3);
  });

  it('超過分は未キャップ銘柄へシェア比例で再配分される', () => {
    const result = waterFillCap([
      { code: 'A', rawShare: 0.40, capLimit: 0.15 },
      { code: 'B', rawShare: 0.40, capLimit: 0.60 },
      { code: 'C', rawShare: 0.20, capLimit: 0.60 },
    ]);
    // A=15%固定、残り85%を B:C = 2:1 で配分
    expect(result.get('A')).toBeCloseTo(0.15);
    expect(result.get('B')).toBeCloseTo(0.85 * (0.4 / 0.6));
    expect(result.get('C')).toBeCloseTo(0.85 * (0.2 / 0.6));
  });

  it('再配分で新たに上限超過した銘柄も収束するまで反復される', () => {
    const result = waterFillCap([
      { code: 'A', rawShare: 0.50, capLimit: 0.15 },
      { code: 'B', rawShare: 0.14, capLimit: 0.15 }, // 再配分後に 0.15 超過
      { code: 'C', rawShare: 0.18, capLimit: 0.40 },
      { code: 'D', rawShare: 0.18, capLimit: 0.40 },
    ]);
    expect(result.get('A')).toBeCloseTo(0.15);
    expect(result.get('B')).toBeCloseTo(0.15);
    expect(result.get('C')).toBeCloseTo(0.35);
    expect(result.get('D')).toBeCloseTo(0.35);
    const sum = [...result.values()].reduce((s, w) => s + w, 0);
    expect(sum).toBeCloseTo(1);
  });

  it('5%枠と15%枠の混在（非半導体主業銘柄の想定）', () => {
    const inputs = [
      { code: 'SONY', rawShare: 0.20, capLimit: 0.05 },
      ...Array.from({ length: 7 }, (_, i) => ({
        code: `A${i}`,
        rawShare: 0.8 / 7,
        capLimit: 0.15,
      })),
    ];
    const result = waterFillCap(inputs);
    // SONY は 5% に固定され、残余 95% が他 7 銘柄へ均等（≈13.57% < 15%）
    expect(result.get('SONY')).toBeCloseTo(0.05);
    for (let i = 0; i < 7; i++) {
      expect(result.get(`A${i}`)).toBeCloseTo(0.95 / 7);
    }
    const sum = [...result.values()].reduce((s, w) => s + w, 0);
    expect(sum).toBeCloseTo(1);
  });

  it('キャップ合計が1未満（実行不能）なら上限比で正規化して返す', () => {
    const result = waterFillCap([
      { code: 'A', rawShare: 0.7, capLimit: 0.3 },
      { code: 'B', rawShare: 0.3, capLimit: 0.3 },
    ]);
    const sum = [...result.values()].reduce((s, w) => s + w, 0);
    expect(sum).toBeCloseTo(1);
    expect(result.get('A')).toBeCloseTo(0.5);
    expect(result.get('B')).toBeCloseTo(0.5);
  });

  it('生シェアは正規化してから扱う（合計が1でなくてもよい）', () => {
    const result = waterFillCap([
      { code: 'A', rawShare: 2, capLimit: 0.9 },
      { code: 'B', rawShare: 2, capLimit: 0.9 },
    ]);
    expect(result.get('A')).toBeCloseTo(0.5);
    expect(result.get('B')).toBeCloseTo(0.5);
  });

  it('空入力は空Map、シェア合計0はエラー', () => {
    expect(waterFillCap([]).size).toBe(0);
    expect(() => waterFillCap([{ code: 'A', rawShare: 0, capLimit: 0.15 }])).toThrow();
  });
});

// ============================================================
// 構成銘柄ウエートの決定（curated / sector33_auto）
// ============================================================

describe('resolveConstituentWeights', () => {
  const anchor = (code: string, mcap: number, isSemiconMain: boolean): AnchorMcapInput => ({
    code,
    mcap,
    isSemiconMain,
  });

  it('curated: 水充填キャップの結果を weight_factor=capped/rawShare・official_weight=capped% で返す', () => {
    // A(非主業/5%枠)=時価総額40%、B/C=各30%。A が 5% に固定され残り95%を B:C=1:1 で配分
    const resolved = resolveConstituentWeights(
      { kind: 'curated', capMain: 0.5, capOther: 0.05 },
      [anchor('A', 40, false), anchor('B', 30, true), anchor('C', 30, true)]
    );
    const byCode = new Map(resolved.map((r) => [r.code, r]));
    // rawShare: A=0.4, B=0.3, C=0.3 / capped: A=0.05, B=0.475, C=0.475
    expect(byCode.get('A')!.officialWeight).toBeCloseTo(5);
    expect(byCode.get('A')!.weightFactor).toBeCloseTo(0.05 / 0.4);
    expect(byCode.get('B')!.officialWeight).toBeCloseTo(47.5);
    expect(byCode.get('B')!.weightFactor).toBeCloseTo(0.475 / 0.3);
    // 公式ウエート合計は100%
    const sum = resolved.reduce((s, r) => s + (r.officialWeight ?? 0), 0);
    expect(sum).toBeCloseTo(100);
  });

  it('curated: キャップ非超過なら weight_factor は全て1（時価総額シェアそのまま）', () => {
    const resolved = resolveConstituentWeights(
      { kind: 'curated', capMain: 0.9, capOther: 0.9 },
      [anchor('A', 30, true), anchor('B', 70, true)]
    );
    for (const r of resolved) expect(r.weightFactor).toBeCloseTo(1);
  });

  it('curated: 時価総額合計0はエラー', () => {
    expect(() =>
      resolveConstituentWeights({ kind: 'curated', capMain: 0.15, capOther: 0.05 }, [
        anchor('A', 0, true),
      ])
    ).toThrow();
  });

  it('sector33_auto: 全銘柄 weight_factor=1・official_weight=null（mcap は無視）', () => {
    const resolved = resolveConstituentWeights({ kind: 'sector33_auto' }, [
      anchor('83060', 999, true),
      anchor('83160', 0, false),
    ]);
    expect(resolved).toEqual([
      { code: '83060', weightFactor: 1, officialWeight: null },
      { code: '83160', weightFactor: 1, officialWeight: null },
    ]);
  });
});

describe('effectiveCoverageWeight', () => {
  it('curated（officialWeight あり）はその値をそのまま返す', () => {
    expect(effectiveCoverageWeight(15, 30)).toBe(15);
    expect(effectiveCoverageWeight(0, 30)).toBe(0);
  });

  it('sector33_auto（officialWeight=null）は均等按分 100/N を返す', () => {
    expect(effectiveCoverageWeight(null, 80)).toBeCloseTo(1.25);
    expect(effectiveCoverageWeight(null, 4)).toBe(25);
  });

  it('構成銘柄0件（N=0）は0（ゼロ割回避）', () => {
    expect(effectiveCoverageWeight(null, 0)).toBe(0);
  });
});

describe('diffSectorConstituents', () => {
  it('新規（equity_master のみ）は toAdd、消滅（constituents のみ）は toClose', () => {
    const diff = diffSectorConstituents(['A', 'B', 'C'], ['B', 'C', 'D']);
    expect(diff.toAdd).toEqual(['A']);
    expect(diff.toClose).toEqual(['D']);
  });

  it('変化なしは両方空', () => {
    const diff = diffSectorConstituents(['A', 'B'], ['B', 'A']);
    expect(diff.toAdd).toEqual([]);
    expect(diff.toClose).toEqual([]);
  });

  it('全入替（重複なし）は全てが toAdd/toClose に分かれ、昇順で返る', () => {
    const diff = diffSectorConstituents(['C', 'A'], ['Z', 'Y']);
    expect(diff.toAdd).toEqual(['A', 'C']);
    expect(diff.toClose).toEqual(['Y', 'Z']);
  });

  it('入力の重複は排除する', () => {
    const diff = diffSectorConstituents(['A', 'A', 'B'], ['B', 'B']);
    expect(diff.toAdd).toEqual(['A']);
    expect(diff.toClose).toEqual([]);
  });
});

// ============================================================
// 1銘柄×1日の集計素材（seed / cron 共有パス）
// ============================================================

describe('buildConstituentDay', () => {
  const basePit = (): PitFinancials => ({
    fy: [
      {
        disclosedDate: '2026-04-30',
        disclosedTime: '15:00',
        fiscalYearEnd: '2026-03-31',
        sales: 1000,
        netIncome: 90,
        eps: 100,
        bps: 400,
        dividendAnnual: 50,
        sharesOutstanding: 10,
      },
    ],
    forward: [
      {
        disclosedDate: '2026-04-30',
        disclosedTime: '15:00',
        targetFyEnd: '2027-03-31',
        forecastEps: 120,
      },
    ],
  });
  const input = (over: Partial<Parameters<typeof buildConstituentDay>[0]> = {}) => ({
    code: 'X',
    factor: 2,
    officialWeight: 15,
    close: 2000,
    pit: basePit(),
    events: [],
    ...over,
  });

  it('価格またはPIT株数が無ければ null', () => {
    expect(buildConstituentDay(input({ close: null }), '2026-07-17')).toBeNull();
    expect(
      buildConstituentDay(input({ pit: { fy: [], forward: [] } }), '2026-07-17')
    ).toBeNull();
  });

  it('通常時: mcap=close×株数、earnings/book/dividendTotal は開示不変量', () => {
    const item = buildConstituentDay(input(), '2026-07-17')!;
    expect(item.mcap).toBeCloseTo(2000 * 10);
    expect(item.earnings).toBeCloseTo(100 * 10);
    expect(item.forwardEarnings).toBeCloseTo(120 * 10);
    expect(item.book).toBeCloseTo(400 * 10);
    expect(item.sales).toBe(1000);
    expect(item.dividendTotal).toBeCloseTo(50 * 10);
    expect(item.factor).toBe(2);
    expect(item.officialWeight).toBe(15);
  });

  it('開示後の分割: 株数は÷factorで増え、earnings等の総額は不変', () => {
    // 1→5 分割（factor 0.2）が開示後に発生。分割後の close=400 で時価総額は不変
    const item = buildConstituentDay(
      input({ close: 400, events: [{ date: '2026-06-29', factor: 0.2 }] }),
      '2026-07-17'
    )!;
    expect(item.mcap).toBeCloseTo(400 * (10 / 0.2)); // = 2000×10 と同じ
    expect(item.earnings).toBeCloseTo(1000); // 不変量
    // 予想EPSも開示日基準→t基準へ換算されるため予想純利益も不変
    expect(item.forwardEarnings).toBeCloseTo(120 * 0.2 * (10 / 0.2));
  });
});

// ============================================================
// 日次集計（調和集計）
// ============================================================

describe('aggregateBasketDay', () => {
  const item = (over: Partial<ConstituentDay>): ConstituentDay => ({
    code: 'X',
    factor: 1,
    officialWeight: 10,
    mcap: 1000,
    earnings: null,
    forwardEarnings: null,
    book: null,
    sales: null,
    dividendTotal: null,
    ...over,
  });

  it('調和集計: PER = Σ(f×mcap) / Σ(f×earnings)（手計算一致）', () => {
    const agg = aggregateBasketDay([
      item({ code: 'A', factor: 1, mcap: 2000, earnings: 100 }), // PER 20
      item({ code: 'B', factor: 0.5, mcap: 1000, earnings: 100 }), // PER 10
    ]);
    // (1×2000 + 0.5×1000) / (1×100 + 0.5×100) = 2500 / 150
    expect(agg.weightedPer).toBeCloseTo(2500 / 150);
  });

  it('赤字銘柄は分母に含める（負の earnings が PER を押し上げる）', () => {
    const agg = aggregateBasketDay([
      item({ code: 'A', mcap: 2000, earnings: 200 }),
      item({ code: 'B', mcap: 1000, earnings: -100 }),
    ]);
    expect(agg.weightedPer).toBeCloseTo(3000 / 100);
  });

  it('バスケット全体が赤字（分母<=0）なら null', () => {
    const agg = aggregateBasketDay([
      item({ code: 'A', mcap: 2000, earnings: -200 }),
      item({ code: 'B', mcap: 1000, earnings: 100 }),
    ]);
    expect(agg.weightedPer).toBeNull();
  });

  it('指標欠損銘柄は分子分母の両方から外して再正規化する', () => {
    const agg = aggregateBasketDay([
      item({ code: 'A', mcap: 2000, earnings: 100, book: 1000 }),
      item({ code: 'B', mcap: 1000, earnings: 100, book: null }), // bps 欠損
    ]);
    expect(agg.weightedPer).toBeCloseTo(3000 / 200);
    expect(agg.weightedPbr).toBeCloseTo(2000 / 1000); // A のみで計算
  });

  it('配当利回りは Σ(f×配当総額)/Σ(f×mcap) の%表記', () => {
    const agg = aggregateBasketDay([
      item({ code: 'A', mcap: 2000, dividendTotal: 40 }),
      item({ code: 'B', mcap: 2000, dividendTotal: 40 }),
    ]);
    expect(agg.weightedDivYield).toBeCloseTo(2);
  });

  it('ウエートは f×mcap の正規化、カバレッジは officialWeight の合計', () => {
    const agg = aggregateBasketDay([
      item({ code: 'A', factor: 2, mcap: 1000, officialWeight: 15 }),
      item({ code: 'B', factor: 1, mcap: 2000, officialWeight: 10 }),
    ]);
    expect(agg.weights.get('A')).toBeCloseTo(0.5);
    expect(agg.weights.get('B')).toBeCloseTo(0.5);
    expect(agg.coveragePct).toBeCloseTo(25);
  });

  it('空入力は per null・カバレッジ0', () => {
    const agg = aggregateBasketDay([]);
    expect(agg.weightedPer).toBeNull();
    expect(agg.coveragePct).toBe(0);
    expect(agg.weights.size).toBe(0);
  });
});

// ============================================================
// 指数連結
// ============================================================

describe('chainIndexSeries', () => {
  const dates = ['2025-11-26', '2025-11-27', '2025-11-28', '2025-12-01'];
  const weights = new Map(
    dates.map((d) => [d, new Map([['A', 0.5], ['B', 0.5]])] as const)
  );
  const adjCloses = new Map<string, Map<string, number>>([
    ['A', new Map([['2025-11-26', 100], ['2025-11-27', 110], ['2025-11-28', 110], ['2025-12-01', 121]])],
    ['B', new Map([['2025-11-26', 200], ['2025-11-27', 200], ['2025-11-28', 220], ['2025-12-01', 220]])],
  ]);

  it('アンカー日を基準に前後へ連結する（手計算一致）', () => {
    const levels = chainIndexSeries(dates, weights, adjCloses, '2025-11-28', 1000);
    expect(levels.get('2025-11-28')).toBe(1000);
    // 12-01: r = 0.5×(121/110) + 0.5×(220/220) = 1.05
    expect(levels.get('2025-12-01')).toBeCloseTo(1050);
    // 11-28 の対 11-27 リターン r = 0.5×(110/110) + 0.5×(220/200) = 1.05 → 前日 = 1000/1.05
    expect(levels.get('2025-11-27')).toBeCloseTo(1000 / 1.05);
    // 11-27 の対 11-26 リターン r = 0.5×(110/100) + 0.5×(200/200) = 1.05
    expect(levels.get('2025-11-26')).toBeCloseTo(1000 / 1.05 / 1.05);
  });

  it('片方の銘柄が欠損した日はもう片方で再正規化する', () => {
    const partial = new Map<string, Map<string, number>>([
      ['A', new Map([['2025-11-28', 100], ['2025-12-01', 110]])],
      ['B', new Map([['2025-11-28', 200]])], // 12-01 欠損
    ]);
    const levels = chainIndexSeries(
      ['2025-11-28', '2025-12-01'],
      new Map([
        ['2025-11-28', new Map([['A', 0.5], ['B', 0.5]])],
        ['2025-12-01', new Map([['A', 1]])],
      ]),
      partial,
      '2025-11-28',
      1000
    );
    expect(levels.get('2025-12-01')).toBeCloseTo(1100);
  });

  it('アンカー日が系列に無ければエラー', () => {
    expect(() => chainIndexSeries(['2025-01-01'], new Map(), new Map(), '2025-11-28', 1000)).toThrow();
  });
});

// ============================================================
// 検証ユーティリティ
// ============================================================

describe('pearsonCorrelation / annualizedTrackingError', () => {
  it('完全相関は 1、逆相関は -1', () => {
    expect(pearsonCorrelation([1, 2, 3], [2, 4, 6])).toBeCloseTo(1);
    expect(pearsonCorrelation([1, 2, 3], [-1, -2, -3])).toBeCloseTo(-1);
  });

  it('分散0や短い系列は null', () => {
    expect(pearsonCorrelation([1, 1, 1], [1, 2, 3])).toBeNull();
    expect(pearsonCorrelation([1], [1])).toBeNull();
  });

  it('TE: 同一系列は 0%、乖離があれば正', () => {
    expect(annualizedTrackingError([0.01, -0.02, 0.03], [0.01, -0.02, 0.03])).toBeCloseTo(0);
    const te = annualizedTrackingError([0.01, -0.02], [0.02, 0.01]);
    expect(te).not.toBeNull();
    expect(te!).toBeGreaterThan(0);
  });
});
