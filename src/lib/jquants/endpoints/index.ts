/**
 * J-Quants API V2 エンドポイントモジュール
 *
 * @description 各エンドポイントの取得・保存機能をまとめたモジュール群
 *
 * @example
 * ```typescript
 * import {
 *   syncTradingCalendar,
 *   syncEquityMaster,
 *   syncEquityBarsDaily,
 *   syncTopixBarsDaily,
 *   syncFinancialSummary,
 *   syncEarningsCalendar,
 *   syncInvestorTypes,
 * } from '@/lib/jquants/endpoints';
 * ```
 */

// 取引カレンダー
export {
  syncTradingCalendar,
  syncTradingCalendarRange,
  fetchTradingCalendar,
  toTradingCalendarRecord,
  isBusinessDay,
  getTradingCalendarFromDB,
  getBusinessDaysFromDB,
  isBusinessDayFromDB,
  type FetchTradingCalendarParams,
  type SyncTradingCalendarResult,
} from './trading-calendar';

// 上場銘柄マスタ
export {
  syncEquityMaster,
  syncEquityMasterForDate,
  syncEquityMasterSCD,
  fetchEquityMaster,
  toEquityMasterRecord,
  toEquityMasterSCDRecord,
  isSameEquityMaster,
  getEquityMasterFromDB,
  getEquityMasterByDateFromDB,
  getAllEquityMasterByDateFromDB,
  getLatestEquityMasterDateFromDB,
  getEquityMasterAsOfDate,
  getAllCurrentEquityMaster,
  getEquityMasterHistory,
  type FetchEquityMasterParams,
  type SyncEquityMasterResult,
  type SyncEquityMasterSCDResult,
} from './equity-master';

// 株価四本値（日足）
export {
  syncEquityBarsDaily,
  syncEquityBarsDailyForDate,
  syncEquityBarsDailyForCode,
  syncEquityBarsDailySinglePage,
  fetchEquityBarsDaily,
  fetchEquityBarsDailyPaginated,
  toEquityBarDailyRecord,
  toEquityBarDailyRecords,
  getEquityBarFromDB,
  getEquityBarsFromDB,
  getAllEquityBarsByDateFromDB,
  getLatestEquityBarDateFromDB,
  type SessionType,
  type FetchEquityBarsDailyParams,
  type SyncEquityBarsDailyResult,
  type SyncEquityBarsDailySinglePageResult,
} from './equity-bars-daily';

// TOPIX
export {
  syncTopixBarsDaily,
  syncTopixBarsDailyForRange,
  syncTopixBarsDailyForDate,
  fetchTopixBarsDaily,
  toTopixBarDailyRecord,
  getTopixBarFromDB,
  getTopixBarsFromDB,
  getLatestTopixBarDateFromDB,
  getRecentTopixBarsFromDB,
  type FetchTopixBarsDailyParams,
  type SyncTopixBarsDailyResult,
} from './index-topix';

// 財務サマリー
export {
  syncFinancialSummary,
  syncFinancialSummaryForDate,
  syncFinancialSummaryForCode,
  fetchFinancialSummary,
  fetchFinancialSummaryPaginated,
  toFinancialDisclosureRecord,
  generateDisclosureId,
  getFinancialDisclosureFromDB,
  getFinancialDisclosuresByCodeFromDB,
  getFinancialDisclosuresByDateFromDB,
  getLatestFinancialDisclosureDateFromDB,
  extractFinancialData,
  type FetchFinancialSummaryParams,
  type SyncFinancialSummaryResult,
} from './fins-summary';

// 決算発表予定
export {
  syncEarningsCalendar,
  fetchEarningsCalendar,
  toEarningsCalendarRecord,
  getEarningsCalendarByDateFromDB,
  getEarningsCalendarByCodeFromDB,
  getUpcomingEarningsFromDB,
  getEarningsCalendarDateRangeFromDB,
  countEarningsByDateFromDB,
  extractEarningsData,
  hasEarningsCalendarForDate,
  type SyncEarningsCalendarResult,
} from './earnings-calendar';

// 投資部門別売買状況
export {
  syncInvestorTypes,
  syncInvestorTypesWithWindow,
  fetchInvestorTypes,
  toInvestorTypeTradingRecords,
  getInvestorTypesFromDB,
  getInvestorTypeDataFromDB,
  getLatestInvestorTypesPublishedDateFromDB,
  getAvailableSections,
  getAvailableSectionsFromDB,
  getForeignInvestorTrendFromDB,
  INVESTOR_TYPES,
  METRICS,
  METRIC_NAMES,
  SECTIONS,
  type InvestorType,
  type Metric,
  type Section,
  type FetchInvestorTypesParams,
  type SyncInvestorTypesResult,
} from './investor-types';

// 信用取引週末残高
export {
  syncWeeklyMarginInterest,
  syncWeeklyMarginInterestWithWindow,
  fetchWeeklyMarginInterest,
  toWeeklyMarginInterestRecord,
  fetchProtectedLocalCodes,
  pruneWeeklyMarginInterest,
  getLatestWeeklyMarginInterestDateFromDB,
  type FetchWeeklyMarginInterestParams,
  type SyncWeeklyMarginInterestResult,
  type PruneWeeklyMarginInterestResult,
} from './weekly-margin-interest';
