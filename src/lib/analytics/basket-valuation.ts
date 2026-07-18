/**
 * テーマバスケット割安判定の計算ロジック（純関数）
 *
 * @description scripts/seed/basket-valuation.ts（初回バックフィル）から使用する。
 * 計画書: docs/PLANS-basket-valuation-2026-07.md（ルートリポ）、DDL: 00105
 *
 * 主要な規約:
 * - 分割補正の向き: J-Quants AdjustmentFactor は分割効力発生日（権利落ち日）に記録され、
 *   1→5 分割で 0.2 が入る（実データ検証済み: 日東紡績 31100 2026-06-29 = 0.2、
 *   ティラド 72360 2026-06-29 = 0.1）。開示日 d0 の一株あたり値を価格日 t と比較するには
 *   (d0, t] の係数の累積積 cum を「乗算」し、株式数には「除算」する（00094 と同じ向き。
 *   時価総額・純利益総額などの積は不変量になる）。
 * - 加重バリュエーションは調和集計: PER = Σ(f_i×mcap_i) / Σ(f_i×earnings_i)。
 *   赤字銘柄も分母に含め、分母 <= 0 なら null（00105 のコメント準拠）。
 * - キャップは水充填法: 上限超過銘柄を上限に固定し、超過分を未キャップ銘柄へ
 *   時価総額シェア比例で再配分、収束まで反復（日経の定期見直しキャップの機械近似）。
 */

// ============================================================
// 型
// ============================================================

/** J-Quants 日足のスリム表現（バックフィルのローカルキャッシュ形式） */
export interface SlimBar {
  date: string;
  /** 生の終値（分割未調整） */
  close: number | null;
  /** 調整後終値（取得時点基準で全期間調整済み） */
  adjClose: number | null;
  /** 調整係数（分割効力発生日以外は 1） */
  adjFactor: number | null;
}

/** 分割・併合イベント（adjFactor <> 1 の日） */
export interface SplitEvent {
  date: string;
  factor: number;
}

/** jquants_core.financial_disclosure の必要列 */
export interface RawDisclosure {
  disclosed_date: string | null;
  disclosed_time: string | null;
  period_type: string | null;
  sales: number | null;
  net_income: number | null;
  eps: number | null;
  bps: number | null;
  dividend_annual: number | null;
  forecast_eps: number | null;
  next_forecast_eps: number | null;
  shares_outstanding_fy: number | null;
  fiscal_year_end: string | null;
}

/** PIT 参照用の FY 実績レコード */
export interface FyRecord {
  disclosedDate: string;
  disclosedTime: string;
  fiscalYearEnd: string;
  sales: number | null;
  netIncome: number | null;
  eps: number | null;
  bps: number | null;
  dividendAnnual: number | null;
  sharesOutstanding: number | null;
}

/** PIT 参照用の予想 EPS レコード */
export interface ForwardRecord {
  disclosedDate: string;
  disclosedTime: string;
  /** この予想が対象とする会計年度末 */
  targetFyEnd: string;
  forecastEps: number;
}

export interface PitFinancials {
  fy: FyRecord[];
  forward: ForwardRecord[];
}

/** キャップ水充填の入力 */
export interface CapInput {
  code: string;
  /** アンカー日の時価総額シェア（合計 1） */
  rawShare: number;
  /** ウエート上限（例 0.15 / 0.05） */
  capLimit: number;
}

/** 1銘柄×1日の集計素材（すべて価格日 t 基準へ換算済み） */
export interface ConstituentDay {
  code: string;
  /** アンカー日に固定したウエート係数 */
  factor: number;
  /** アンカー日の公式（機械キャップ）ウエート%（カバレッジ算出用） */
  officialWeight: number;
  /** 生 close × PIT 株数（t 基準） */
  mcap: number;
  /** 実績純利益 = eps × 株数（開示不変量。赤字は負値のまま） */
  earnings: number | null;
  /** 予想純利益 = 予想EPS(t基準) × PIT株数(t基準) */
  forwardEarnings: number | null;
  /** 純資産 = bps × 株数（開示不変量） */
  book: number | null;
  /** 売上高（開示不変量） */
  sales: number | null;
  /** 年間配当総額 = dividend_annual × 株数（開示不変量） */
  dividendTotal: number | null;
}

export interface BasketDayAggregate {
  weightedPer: number | null;
  weightedPerForward: number | null;
  weightedPbr: number | null;
  weightedPsr: number | null;
  /** % */
  weightedDivYield: number | null;
  /** 当日利用できた構成銘柄の公式ウエート合計% */
  coveragePct: number;
  /** w_i(t) = f_i × mcap_i / Σ（翌日の指数リターン計算に使用） */
  weights: Map<string, number>;
}

// ============================================================
// 分割補正
// ============================================================

/** 日足列から分割・併合イベントを抽出（factor=1/null を除外、同日重複は先勝ち） */
export function extractSplitEvents(bars: SlimBar[]): SplitEvent[] {
  const seen = new Set<string>();
  const events: SplitEvent[] = [];
  for (const bar of bars) {
    if (bar.adjFactor == null || bar.adjFactor === 1 || bar.adjFactor <= 0) continue;
    if (seen.has(bar.date)) continue;
    seen.add(bar.date);
    events.push({ date: bar.date, factor: bar.adjFactor });
  }
  return events.sort((a, b) => (a.date < b.date ? -1 : 1));
}

/**
 * (fromExclusive, toInclusive] に効力発生した調整係数の累積積。
 * イベントが無ければ 1（未調整）。開示日当日の分割は開示値に織り込み済みとみなし含めない。
 */
export function cumulativeAdjustmentFactor(
  events: SplitEvent[],
  fromExclusive: string,
  toInclusive: string
): number {
  let cum = 1;
  for (const ev of events) {
    if (ev.date > fromExclusive && ev.date <= toInclusive) {
      cum *= ev.factor;
    }
  }
  return cum;
}

// ============================================================
// PIT 財務系列
// ============================================================

/** fiscal_year_end の 1 年後（YYYY-MM-DD、月末はクランプ） */
export function addOneYear(dateStr: string): string {
  const [y, m, d] = dateStr.split('-').map(Number);
  const next = new Date(Date.UTC(y + 1, m - 1, 1));
  const lastDay = new Date(Date.UTC(y + 1, m, 0)).getUTCDate();
  next.setUTCDate(Math.min(d, lastDay));
  return next.toISOString().slice(0, 10);
}

/**
 * 開示行から PIT 参照用の系列を構築する。
 *
 * 実績（FY 系列）の選定基準（00049/00094 の ranked_fin と同一思想）:
 * - period_type='FY' かつ sales 非 NULL（決算短信本体のみ。予想修正等を除外）
 * - 参照時は disclosed_date <= t の中から (fiscal_year_end, disclosed_date, disclosed_time)
 *   最大の行を採用（同一年度の訂正開示は新しい方が勝つ）
 *
 * 予想 EPS 系列の選定基準:
 * - forecast_eps 非 NULL の行 → 対象年度 = その行の fiscal_year_end
 *   （1Q/2Q/3Q 短信と EarnForecastRevision が該当。FY 短信本体は forecast_eps が NULL）
 * - forecast_eps が NULL で next_forecast_eps 非 NULL の行（FY 短信本体）
 *   → 対象年度 = fiscal_year_end + 1年
 * - 参照時は「対象年度 > その時点の最新実績年度」の行のみ有効（実績化した予想の残留を防ぐ）
 */
export function buildPitFinancials(rows: RawDisclosure[]): PitFinancials {
  const fy: FyRecord[] = [];
  const forward: ForwardRecord[] = [];

  for (const row of rows) {
    if (!row.disclosed_date || !row.fiscal_year_end) continue;
    const time = row.disclosed_time ?? '';

    if (row.period_type === 'FY' && row.sales != null) {
      fy.push({
        disclosedDate: row.disclosed_date,
        disclosedTime: time,
        fiscalYearEnd: row.fiscal_year_end,
        sales: row.sales,
        netIncome: row.net_income,
        eps: row.eps,
        bps: row.bps,
        dividendAnnual: row.dividend_annual,
        sharesOutstanding: row.shares_outstanding_fy,
      });
    }

    if (row.forecast_eps != null) {
      forward.push({
        disclosedDate: row.disclosed_date,
        disclosedTime: time,
        targetFyEnd: row.fiscal_year_end,
        forecastEps: row.forecast_eps,
      });
    } else if (row.next_forecast_eps != null) {
      forward.push({
        disclosedDate: row.disclosed_date,
        disclosedTime: time,
        targetFyEnd: addOneYear(row.fiscal_year_end),
        forecastEps: row.next_forecast_eps,
      });
    }
  }

  const byDisclosed = (a: { disclosedDate: string; disclosedTime: string }, b: typeof a) =>
    a.disclosedDate === b.disclosedDate
      ? a.disclosedTime < b.disclosedTime ? -1 : 1
      : a.disclosedDate < b.disclosedDate ? -1 : 1;
  fy.sort(byDisclosed);
  forward.sort(byDisclosed);

  return { fy, forward };
}

/** date 時点で有効な FY 実績（無ければ null） */
export function pitFy(fyRecords: FyRecord[], date: string): FyRecord | null {
  let best: FyRecord | null = null;
  for (const rec of fyRecords) {
    if (rec.disclosedDate > date) break;
    if (
      !best ||
      rec.fiscalYearEnd > best.fiscalYearEnd ||
      (rec.fiscalYearEnd === best.fiscalYearEnd && rec.disclosedDate >= best.disclosedDate)
    ) {
      best = rec;
    }
  }
  return best;
}

/** date 時点で有効な予想 EPS（実績化済み・データ無しは null） */
export function pitForwardEps(
  pit: PitFinancials,
  date: string
): { forecastEps: number; disclosedDate: string } | null {
  const latestActualFyEnd = pitFy(pit.fy, date)?.fiscalYearEnd ?? '';
  let best: ForwardRecord | null = null;
  for (const rec of pit.forward) {
    if (rec.disclosedDate > date) break;
    if (rec.targetFyEnd <= latestActualFyEnd) continue;
    best = rec; // disclosed 昇順走査なので常に最新開示が残る
  }
  return best ? { forecastEps: best.forecastEps, disclosedDate: best.disclosedDate } : null;
}

// ============================================================
// キャップ水充填
// ============================================================

/**
 * 時価総額シェアにウエート上限を適用（水充填法）。
 * 上限超過銘柄を上限に固定し、残余を未キャップ銘柄へシェア比例配分、収束まで反復。
 *
 * @returns code → キャップ後ウエート（合計 1）
 */
export function waterFillCap(inputs: CapInput[]): Map<string, number> {
  if (inputs.length === 0) return new Map();
  const totalShare = inputs.reduce((sum, i) => sum + i.rawShare, 0);
  if (totalShare <= 0) throw new Error('waterFillCap: total raw share must be positive');

  const capped = new Map<string, number>();
  let uncapped = inputs.map((i) => ({ ...i, rawShare: i.rawShare / totalShare }));

  for (let iter = 0; iter <= inputs.length; iter++) {
    const cappedSum = [...capped.values()].reduce((s, w) => s + w, 0);
    const remaining = 1 - cappedSum;
    const uncappedShareSum = uncapped.reduce((s, i) => s + i.rawShare, 0);
    if (uncapped.length === 0 || uncappedShareSum <= 0) break;

    const violations = uncapped.filter(
      (i) => (i.rawShare / uncappedShareSum) * remaining > i.capLimit + 1e-12
    );
    if (violations.length === 0) {
      for (const i of uncapped) {
        capped.set(i.code, (i.rawShare / uncappedShareSum) * remaining);
      }
      return capped;
    }
    for (const v of violations) {
      capped.set(v.code, v.capLimit);
    }
    const violated = new Set(violations.map((v) => v.code));
    uncapped = uncapped.filter((i) => !violated.has(i.code));
  }

  // 全銘柄がキャップに張り付いた場合（合計<1）は上限比で正規化して返す
  const sum = [...capped.values()].reduce((s, w) => s + w, 0);
  if (sum > 0 && Math.abs(sum - 1) > 1e-9) {
    for (const [code, w] of capped) capped.set(code, w / sum);
  }
  return capped;
}

// ============================================================
// 日次集計（調和集計）
// ============================================================

/**
 * 分子（f×mcap）と分母（f×指標総額）を「両方算出可能な銘柄のみ」で組んで比を取る。
 * 分母が 0 以下（例: バスケット全体で赤字）の場合は null。
 */
function harmonicRatio(
  items: ConstituentDay[],
  pick: (i: ConstituentDay) => number | null
): number | null {
  let num = 0;
  let den = 0;
  for (const item of items) {
    const value = pick(item);
    if (value == null) continue;
    num += item.factor * item.mcap;
    den += item.factor * value;
  }
  if (num <= 0 || den <= 0) return null;
  return num / den;
}

/** 当日の加重バリュエーションとウエートを集計 */
export function aggregateBasketDay(items: ConstituentDay[]): BasketDayAggregate {
  const totalFm = items.reduce((s, i) => s + i.factor * i.mcap, 0);
  const weights = new Map<string, number>();
  if (totalFm > 0) {
    for (const item of items) {
      weights.set(item.code, (item.factor * item.mcap) / totalFm);
    }
  }

  // 配当利回りは Σ(f×配当総額) / Σ(f×mcap) の逆数側なので専用に計算
  let divNum = 0;
  let divDen = 0;
  for (const item of items) {
    if (item.dividendTotal == null) continue;
    divNum += item.factor * item.dividendTotal;
    divDen += item.factor * item.mcap;
  }

  return {
    weightedPer: harmonicRatio(items, (i) => i.earnings),
    weightedPerForward: harmonicRatio(items, (i) => i.forwardEarnings),
    weightedPbr: harmonicRatio(items, (i) => i.book),
    weightedPsr: harmonicRatio(items, (i) => i.sales),
    weightedDivYield: divDen > 0 ? (divNum / divDen) * 100 : null,
    coveragePct: items.reduce((s, i) => s + i.officialWeight, 0),
    weights,
  };
}

// ============================================================
// 模擬指数の連結
// ============================================================

/**
 * アンカー日 = anchorLevel として、日次加重リターン
 * r(t) = Σ w_i(t-1) × adjClose_i(t)/adjClose_i(t-1)（両日そろう銘柄で再正規化）
 * で前後に連結した指数系列を返す。リターンが計算できない断絶があれば
 * その方向の連結はそこで打ち切る（以降 null 扱い＝Map に載せない）。
 */
export function chainIndexSeries(
  dates: string[],
  weightsByDate: Map<string, Map<string, number>>,
  adjCloseByCode: Map<string, Map<string, number>>,
  anchorDate: string,
  anchorLevel: number
): Map<string, number> {
  const anchorIdx = dates.indexOf(anchorDate);
  if (anchorIdx < 0) {
    throw new Error(`chainIndexSeries: anchorDate ${anchorDate} not in dates`);
  }

  const dailyReturn = (prevDate: string, curDate: string): number | null => {
    const weights = weightsByDate.get(prevDate);
    if (!weights || weights.size === 0) return null;
    let weightSum = 0;
    let acc = 0;
    for (const [code, w] of weights) {
      const prev = adjCloseByCode.get(code)?.get(prevDate);
      const cur = adjCloseByCode.get(code)?.get(curDate);
      if (prev == null || cur == null || prev <= 0) continue;
      weightSum += w;
      acc += w * (cur / prev);
    }
    if (weightSum <= 0) return null;
    return acc / weightSum;
  };

  const levels = new Map<string, number>();
  levels.set(anchorDate, anchorLevel);

  let level = anchorLevel;
  for (let k = anchorIdx + 1; k < dates.length; k++) {
    const r = dailyReturn(dates[k - 1], dates[k]);
    if (r == null) break;
    level *= r;
    levels.set(dates[k], level);
  }

  level = anchorLevel;
  for (let k = anchorIdx; k > 0; k--) {
    const r = dailyReturn(dates[k - 1], dates[k]);
    if (r == null) break;
    level /= r;
    levels.set(dates[k - 1], level);
  }

  return levels;
}

// ============================================================
// 検証ユーティリティ
// ============================================================

/** ピアソン相関係数（対応する 2 系列、長さ 2 未満は null） */
export function pearsonCorrelation(xs: number[], ys: number[]): number | null {
  const n = Math.min(xs.length, ys.length);
  if (n < 2) return null;
  const meanX = xs.slice(0, n).reduce((s, v) => s + v, 0) / n;
  const meanY = ys.slice(0, n).reduce((s, v) => s + v, 0) / n;
  let cov = 0;
  let varX = 0;
  let varY = 0;
  for (let i = 0; i < n; i++) {
    const dx = xs[i] - meanX;
    const dy = ys[i] - meanY;
    cov += dx * dy;
    varX += dx * dx;
    varY += dy * dy;
  }
  if (varX <= 0 || varY <= 0) return null;
  return cov / Math.sqrt(varX * varY);
}

/** 年率トラッキングエラー（日次リターン差の標準偏差 × √252、%） */
export function annualizedTrackingError(retA: number[], retB: number[]): number | null {
  const n = Math.min(retA.length, retB.length);
  if (n < 2) return null;
  const diffs: number[] = [];
  for (let i = 0; i < n; i++) diffs.push(retA[i] - retB[i]);
  const mean = diffs.reduce((s, v) => s + v, 0) / n;
  const variance = diffs.reduce((s, v) => s + (v - mean) ** 2, 0) / (n - 1);
  return Math.sqrt(variance) * Math.sqrt(252) * 100;
}
