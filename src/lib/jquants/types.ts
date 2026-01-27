/**
 * J-Quants API V2 型定義
 *
 * @description J-Quants API V2 のレスポンス型定義
 * @see https://jpx-jquants.com/en/spec
 *
 * V2では:
 * - すべてのエンドポイントが "data" キーでデータを返す
 * - フィールド名は省略形を使用 (Open→O, High→H, etc.)
 */

// ============================================
// 共通型
// ============================================

/**
 * ページネーション付きレスポンスの基本型
 */
export interface PaginatedResponse<T> {
  data: T[];
  pagination_key?: string;
}

// ============================================
// 取引カレンダー
// ============================================

/**
 * 取引カレンダー（1日分）
 * V2フィールド: Date, HolDiv
 */
export interface TradingCalendarItem {
  /** 日付 (YYYY-MM-DD) */
  Date: string;
  /** 休日区分: 0=非営業日, 1=営業日, 2=半日取引 */
  HolDiv: string;
}

/**
 * GET /v2/markets/calendar レスポンス
 */
export interface TradingCalendarResponse {
  data: TradingCalendarItem[];
}

// ============================================
// 上場銘柄マスタ
// ============================================

/**
 * 上場銘柄マスタ（1銘柄分）
 * V2フィールド: Date, Code, CoName, CoNameEn, S17, S17Nm, S33, S33Nm, ScaleCat, Mkt, MktNm, MarginCode, MarginCodeNm
 */
export interface EquityMasterItem {
  /** 日付 (YYYY-MM-DD) */
  Date: string;
  /** ローカルコード (5桁) */
  Code: string;
  /** 会社名（日本語） */
  CoName?: string;
  /** 会社名（英語） */
  CoNameEn?: string;
  /** 17業種コード */
  S17?: string;
  /** 17業種名 */
  S17Nm?: string;
  /** 33業種コード */
  S33?: string;
  /** 33業種名 */
  S33Nm?: string;
  /** 規模区分 */
  ScaleCat?: string;
  /** 市場コード */
  Mkt?: string;
  /** 市場名 */
  MktNm?: string;
  /** 信用区分 */
  MarginCode?: string;
  /** 信用区分名 */
  MarginCodeNm?: string;
}

/**
 * GET /v2/equities/master レスポンス
 */
export interface EquityMasterResponse {
  data: EquityMasterItem[];
}

// ============================================
// 株価四本値（日足）
// ============================================

/**
 * 株価四本値（1レコード分）
 * V2フィールド: Date, Code, O, H, L, C, Vo, Va, UL, LL, AdjFactor, AdjO, AdjH, AdjL, AdjC, AdjVo
 * + Morning/Afternoon session fields: MO, MH, ML, MC, MVo, MVa, AO, AH, AL, AC, AVo, AVa
 */
export interface EquityBarDailyItem {
  /** 日付 (YYYY-MM-DD) */
  Date: string;
  /** ローカルコード (5桁) */
  Code: string;
  /** 始値 */
  O?: number;
  /** 高値 */
  H?: number;
  /** 安値 */
  L?: number;
  /** 終値 */
  C?: number;
  /** ストップ高フラグ */
  UL?: string;
  /** ストップ安フラグ */
  LL?: string;
  /** 出来高 */
  Vo?: number;
  /** 売買代金 */
  Va?: number;
  /** 調整係数 */
  AdjFactor?: number;
  /** 調整後始値 */
  AdjO?: number;
  /** 調整後高値 */
  AdjH?: number;
  /** 調整後安値 */
  AdjL?: number;
  /** 調整後終値 */
  AdjC?: number;
  /** 調整後出来高 */
  AdjVo?: number;
  /** 前場始値 */
  MO?: number;
  /** 前場高値 */
  MH?: number;
  /** 前場安値 */
  ML?: number;
  /** 前場終値 */
  MC?: number;
  /** 前場ストップ高フラグ */
  MUL?: string;
  /** 前場ストップ安フラグ */
  MLL?: string;
  /** 前場出来高 */
  MVo?: number;
  /** 前場売買代金 */
  MVa?: number;
  /** 前場調整後始値 */
  MAdjO?: number;
  /** 前場調整後高値 */
  MAdjH?: number;
  /** 前場調整後安値 */
  MAdjL?: number;
  /** 前場調整後終値 */
  MAdjC?: number;
  /** 前場調整後出来高 */
  MAdjVo?: number;
  /** 後場始値 */
  AO?: number;
  /** 後場高値 */
  AH?: number;
  /** 後場安値 */
  AL?: number;
  /** 後場終値 */
  AC?: number;
  /** 後場ストップ高フラグ */
  AUL?: string;
  /** 後場ストップ安フラグ */
  ALL?: string;
  /** 後場出来高 */
  AVo?: number;
  /** 後場売買代金 */
  AVa?: number;
  /** 後場調整後始値 */
  AAdjO?: number;
  /** 後場調整後高値 */
  AAdjH?: number;
  /** 後場調整後安値 */
  AAdjL?: number;
  /** 後場調整後終値 */
  AAdjC?: number;
  /** 後場調整後出来高 */
  AAdjVo?: number;
}

/**
 * GET /v2/equities/bars/daily レスポンス
 */
export interface EquityBarsDailyResponse {
  data: EquityBarDailyItem[];
  pagination_key?: string;
}

// ============================================
// TOPIX
// ============================================

/**
 * TOPIX（1日分）
 * V2フィールド: Date, O, H, L, C
 */
export interface TopixBarDailyItem {
  /** 日付 (YYYY-MM-DD) */
  Date: string;
  /** 始値 */
  O?: number;
  /** 高値 */
  H?: number;
  /** 安値 */
  L?: number;
  /** 終値 */
  C?: number;
}

/**
 * GET /v2/indices/bars/daily/topix レスポンス
 */
export interface TopixBarsDailyResponse {
  data: TopixBarDailyItem[];
}

// ============================================
// 財務サマリー
// ============================================

/**
 * 財務サマリー（1開示分）
 * V2フィールド: 省略形を使用（115+フィールド）
 */
export interface FinancialSummaryItem {
  /** 開示日付 (YYYY-MM-DD) */
  DiscDate?: string;
  /** 開示時刻 (HH:MM:SS) */
  DiscTime?: string;
  /** ローカルコード (5桁) */
  Code?: string;
  /** 開示番号（一意識別子として使用） */
  DiscNo?: string;
  /** 書類種別 */
  DocType?: string;
  /** 会計期間種別 */
  CurPerType?: string;
  /** 会計期間開始日 */
  CurPerSt?: string;
  /** 会計期間終了日 */
  CurPerEn?: string;
  /** 会計年度開始日 */
  CurFYSt?: string;
  /** 会計年度終了日 */
  CurFYEn?: string;
  /** 売上高 */
  Sales?: number;
  /** 営業利益 */
  OP?: number;
  /** 経常利益 */
  OdP?: number;
  /** 当期純利益 */
  NP?: number;
  /** EPS */
  EPS?: number;
  /** 希薄化後EPS */
  DEPS?: number;
  /** 総資産 */
  TA?: number;
  /** 純資産 */
  Eq?: number;
  /** 自己資本比率 */
  EqAR?: number;
  /** BPS */
  BPS?: number;
  /** 営業キャッシュフロー */
  CFO?: number;
  /** 投資キャッシュフロー */
  CFI?: number;
  /** 財務キャッシュフロー */
  CFF?: number;
  /** 現金同等物期末残高 */
  CashEq?: number;
  /** ROE */
  ROE?: number;
  /** ROA */
  ROA?: number;
  /** 配当（第1四半期） */
  Div1Q?: number;
  /** 配当（第2四半期） */
  Div2Q?: number;
  /** 配当（第3四半期） */
  Div3Q?: number;
  /** 配当（期末） */
  DivFY?: number;
  /** 配当（年間） */
  DivAnn?: number;
  /** 配当単位 */
  DivUnit?: string;
  /** 予想売上高 */
  FSales?: number;
  /** 予想営業利益 */
  FOP?: number;
  /** 予想経常利益 */
  FOdP?: number;
  /** 予想当期純利益 */
  FNP?: number;
  /** 予想EPS */
  FEPS?: number;
  /** 予想配当（年間） */
  FDivAnn?: number;
  /** 次期予想売上高 */
  NxFSales?: number;
  /** 次期予想営業利益 */
  NxFOP?: number;
  /** 次期予想経常利益 */
  NxFOdP?: number;
  /** 次期予想当期純利益 */
  NxFNP?: number;
  /** 次期予想EPS */
  NxFEPS?: number;
  /** 次期予想配当（年間） */
  NxFDivAnn?: number;
  /** 重要な子会社の異動 */
  MatChgSub?: string;
  /** 経営内容の著しい変化 */
  SigChgInC?: string;
  /** 会計基準変更による変更 */
  ChgByASRev?: string;
  /** 会計基準変更以外の変更 */
  ChgNoASRev?: string;
  /** 会計上の見積もりの変更 */
  ChgAcEst?: string;
  /** 遡及修正 */
  RetroRst?: string;
  /** 期末発行済株式数 */
  ShOutFY?: number;
  /** 期末自己株式数 */
  TrShFY?: number;
  /** 期中平均株式数 */
  AvgSh?: number;
  // 非連結フィールド（NC prefix）
  /** 非連結売上高 */
  NCSales?: number;
  /** 非連結営業利益 */
  NCOP?: number;
  /** 非連結経常利益 */
  NCOdP?: number;
  /** 非連結当期純利益 */
  NCNP?: number;
  /** 非連結EPS */
  NCEPS?: number;
  /** 非連結総資産 */
  NCTA?: number;
  /** 非連結純資産 */
  NCEq?: number;
  /** 非連結自己資本比率 */
  NCEqAR?: number;
  /** 非連結BPS */
  NCBPS?: number;
  // その他多数のフィールドがあるが、raw_json で保存するため省略
  [key: string]: unknown;
}

/**
 * GET /v2/fins/summary レスポンス
 */
export interface FinancialSummaryResponse {
  data: FinancialSummaryItem[];
  pagination_key?: string;
}

// ============================================
// 決算発表予定
// ============================================

/**
 * 決算発表予定（1銘柄分）
 * V2フィールド: Date, Code, CoName, FY, SectorNm, FQ, Section
 */
export interface EarningsCalendarItem {
  /** 決算発表日 (YYYY-MM-DD) */
  Date: string;
  /** ローカルコード (5桁) */
  Code: string;
  /** 会社名 */
  CoName?: string;
  /** 決算年度 */
  FY?: string;
  /** セクター名 */
  SectorNm?: string;
  /** 決算期間種別 (1Q, 2Q, 3Q, FY) */
  FQ?: string;
  /** 決算発表セクション */
  Section?: string;
}

/**
 * GET /v2/equities/earnings-calendar レスポンス
 */
export interface EarningsCalendarResponse {
  data: EarningsCalendarItem[];
}

// ============================================
// 投資部門別売買状況
// ============================================

/**
 * 投資部門別売買状況（1レコード分）
 * V2フィールド: 実際のAPIレスポンスフィールド名を使用
 *
 * NOTE: APIフィールド名の命名規則
 * - 投資主体: Prop, Brk, InvTr, BusCo, OthCo, InsCo, Bank, TrstBnk, OthFin, Ind, Frgn, Tot, SecCo
 * - 指標: Sell, Buy, Tot, Bal
 */
export interface InvestorTypeTradingItem {
  /** 公表日 (YYYY-MM-DD) */
  PubDate: string;
  /** 開始日 (YYYY-MM-DD) */
  StDate: string;
  /** 終了日 (YYYY-MM-DD) */
  EnDate: string;
  /** セクション (TSEPrime, TSEStandard, TSEGrowth 等) */
  Section: string;
  /** 証券会社（自己取引） - 売り */
  PropSell?: number;
  /** 証券会社（自己取引） - 買い */
  PropBuy?: number;
  /** 証券会社（自己取引） - 合計 */
  PropTot?: number;
  /** 証券会社（自己取引） - 差引 */
  PropBal?: number;
  /** 証券会社（委託） - 売り */
  BrkSell?: number;
  /** 証券会社（委託） - 買い */
  BrkBuy?: number;
  /** 証券会社（委託） - 合計 */
  BrkTot?: number;
  /** 証券会社（委託） - 差引 */
  BrkBal?: number;
  /** 投資信託 - 売り */
  InvTrSell?: number;
  /** 投資信託 - 買い */
  InvTrBuy?: number;
  /** 投資信託 - 合計 */
  InvTrTot?: number;
  /** 投資信託 - 差引 */
  InvTrBal?: number;
  /** 事業法人 - 売り */
  BusCoSell?: number;
  /** 事業法人 - 買い */
  BusCoBuy?: number;
  /** 事業法人 - 合計 */
  BusCoTot?: number;
  /** 事業法人 - 差引 */
  BusCoBal?: number;
  /** その他法人 - 売り */
  OthCoSell?: number;
  /** その他法人 - 買い */
  OthCoBuy?: number;
  /** その他法人 - 合計 */
  OthCoTot?: number;
  /** その他法人 - 差引 */
  OthCoBal?: number;
  /** 生保・損保 - 売り */
  InsCoSell?: number;
  /** 生保・損保 - 買い */
  InsCoBuy?: number;
  /** 生保・損保 - 合計 */
  InsCoTot?: number;
  /** 生保・損保 - 差引 */
  InsCoBal?: number;
  /** 都銀・地銀等 - 売り */
  BankSell?: number;
  /** 都銀・地銀等 - 買い */
  BankBuy?: number;
  /** 都銀・地銀等 - 合計 */
  BankTot?: number;
  /** 都銀・地銀等 - 差引 */
  BankBal?: number;
  /** 信託銀行 - 売り */
  TrstBnkSell?: number;
  /** 信託銀行 - 買い */
  TrstBnkBuy?: number;
  /** 信託銀行 - 合計 */
  TrstBnkTot?: number;
  /** 信託銀行 - 差引 */
  TrstBnkBal?: number;
  /** その他金融機関 - 売り */
  OthFinSell?: number;
  /** その他金融機関 - 買い */
  OthFinBuy?: number;
  /** その他金融機関 - 合計 */
  OthFinTot?: number;
  /** その他金融機関 - 差引 */
  OthFinBal?: number;
  /** 個人 - 売り */
  IndSell?: number;
  /** 個人 - 買い */
  IndBuy?: number;
  /** 個人 - 合計 */
  IndTot?: number;
  /** 個人 - 差引 */
  IndBal?: number;
  /** 外国人 - 売り */
  FrgnSell?: number;
  /** 外国人 - 買い */
  FrgnBuy?: number;
  /** 外国人 - 合計 */
  FrgnTot?: number;
  /** 外国人 - 差引 */
  FrgnBal?: number;
  /** 証券会社（受託） - 売り */
  SecCoSell?: number;
  /** 証券会社（受託） - 買い */
  SecCoBuy?: number;
  /** 証券会社（受託） - 合計 */
  SecCoTot?: number;
  /** 証券会社（受託） - 差引 */
  SecCoBal?: number;
  /** 合計 - 売り */
  TotSell?: number;
  /** 合計 - 買い */
  TotBuy?: number;
  /** 合計 - 合計 */
  TotTot?: number;
  /** 合計 - 差引 */
  TotBal?: number;
}

/**
 * GET /v2/equities/investor-types レスポンス
 */
export interface InvestorTypeTradingResponse {
  data: InvestorTypeTradingItem[];
}

// ============================================
// DBテーブル用マッピング型
// ============================================

/**
 * trading_calendar テーブル用
 */
export interface TradingCalendarRecord {
  calendar_date: string;
  hol_div: string;
  is_business_day: boolean;
  raw_json: TradingCalendarItem;
  ingested_at?: string;
}

/**
 * equity_master_snapshot テーブル用
 */
export interface EquityMasterSnapshotRecord {
  as_of_date: string;
  local_code: string;
  company_name?: string;
  company_name_en?: string;
  sector17_code?: string;
  sector17_name?: string;
  sector33_code?: string;
  sector33_name?: string;
  scale_category?: string;
  market_code?: string;
  market_name?: string;
  margin_code?: string;
  margin_code_name?: string;
  raw_json: EquityMasterItem;
  ingested_at?: string;
}

/**
 * equity_bar_daily テーブル用
 *
 * NOTE: expandSessions使用時、raw_jsonはストレージ効率のため
 * 最初のレコード（通常DAYセッション）のみに格納。
 * 他のセッション（AM/PM）のraw_jsonはnullとなる。
 */
export interface EquityBarDailyRecord {
  trade_date: string;
  local_code: string;
  session: string;
  open?: number;
  high?: number;
  low?: number;
  close?: number;
  volume?: number;
  turnover_value?: number;
  adjustment_factor?: number;
  adj_open?: number;
  adj_high?: number;
  adj_low?: number;
  adj_close?: number;
  adj_volume?: number;
  raw_json: EquityBarDailyItem | null;
  ingested_at?: string;
}

/**
 * topix_bar_daily テーブル用
 */
export interface TopixBarDailyRecord {
  trade_date: string;
  open?: number;
  high?: number;
  low?: number;
  close?: number;
  raw_json: TopixBarDailyItem;
  ingested_at?: string;
}

/**
 * financial_disclosure テーブル用
 */
export interface FinancialDisclosureRecord {
  disclosure_id: string;
  disclosed_date?: string;
  disclosed_time?: string;
  local_code?: string;
  raw_json: FinancialSummaryItem;
  ingested_at?: string;
}

/**
 * earnings_calendar テーブル用
 */
export interface EarningsCalendarRecord {
  announcement_date: string;
  local_code: string;
  raw_json: EarningsCalendarItem;
  ingested_at?: string;
}

/**
 * investor_type_trading テーブル用（縦持ち）
 *
 * NOTE: raw_jsonはストレージ効率のため最初のレコードのみに完全格納。
 * 他のレコードは空オブジェクト{}となる。元データが必要な場合は
 * investor_type='proprietary', metric='sales'のレコードを参照すること。
 */
export interface InvestorTypeTradingRecord {
  published_date: string;
  start_date: string;
  end_date: string;
  section: string;
  investor_type: string;
  metric: string;
  value_kjpy?: number;
  raw_json: InvestorTypeTradingItem | Record<string, never>;
  ingested_at?: string;
}

// ============================================
// API リクエストパラメータ
// ============================================

/**
 * 日付範囲パラメータ
 */
export interface DateRangeParams {
  from?: string;
  to?: string;
}

/**
 * ページネーションパラメータ
 */
export interface PaginationParams {
  pagination_key?: string;
}

/**
 * 銘柄指定パラメータ
 */
export interface CodeParams {
  code?: string;
}

/**
 * 日付指定パラメータ
 */
export interface DateParams {
  date?: string;
}

// ============================================
// V2 フィールド名マッピング（参考用）
// ============================================

/**
 * V1 → V2 フィールド名マッピング（株価）
 * 旧フィールド名から新フィールド名への変換用
 */
export const EQUITY_BAR_FIELD_MAP_V1_TO_V2 = {
  Open: 'O',
  High: 'H',
  Low: 'L',
  Close: 'C',
  Volume: 'Vo',
  TurnoverValue: 'Va',
  AdjustmentFactor: 'AdjFactor',
  AdjustmentOpen: 'AdjO',
  AdjustmentHigh: 'AdjH',
  AdjustmentLow: 'AdjL',
  AdjustmentClose: 'AdjC',
  AdjustmentVolume: 'AdjVo',
} as const;

/**
 * V1 → V2 フィールド名マッピング（財務）
 */
export const FINANCIAL_FIELD_MAP_V1_TO_V2 = {
  DisclosedDate: 'DiscDate',
  DisclosedTime: 'DiscTime',
  LocalCode: 'Code',
  DisclosureNumber: 'DiscNo',
  TypeOfDocument: 'DocType',
  TypeOfCurrentPeriod: 'CurPerType',
  CurrentFiscalYearStartDate: 'CurFYSt',
  CurrentFiscalYearEndDate: 'CurFYEn',
  NetSales: 'Sales',
  OperatingProfit: 'OP',
  OrdinaryProfit: 'OdP',
  Profit: 'NP',
  EarningsPerShare: 'EPS',
  DilutedEarningsPerShare: 'DEPS',
  TotalAssets: 'TA',
  Equity: 'Eq',
  EquityToAssetRatio: 'EqAR',
  BookValuePerShare: 'BPS',
  ResultReturnOnEquity: 'ROE',
  ResultDividendPerShareAnnual: 'DivAnn',
} as const;

/**
 * V1 → V2 フィールド名マッピング（銘柄マスタ）
 */
export const EQUITY_MASTER_FIELD_MAP_V1_TO_V2 = {
  CompanyName: 'CoName',
  CompanyNameEnglish: 'CoNameEn',
  Sector17Code: 'S17',
  Sector17CodeName: 'S17Nm',
  Sector33Code: 'S33',
  Sector33CodeName: 'S33Nm',
  ScaleCategory: 'ScaleCat',
  MarketCode: 'Mkt',
  MarketCodeName: 'MktNm',
  MarginCodeName: 'MarginCodeNm',
} as const;

/**
 * V1 → V2 フィールド名マッピング（取引カレンダー）
 */
export const TRADING_CALENDAR_FIELD_MAP_V1_TO_V2 = {
  HolidayDivision: 'HolDiv',
} as const;
