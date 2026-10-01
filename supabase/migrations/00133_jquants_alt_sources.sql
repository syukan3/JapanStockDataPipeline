-- 00133_jquants_alt_sources.sql
-- ============================================================================
-- J-Quants OFF 中の代替データの器
-- ----------------------------------------------------------------------------
-- 設計正本: ../../../docs/PLANS-jquants-off-switch-2026-10.md §4
--
-- 1. source 列: 既存テーブルに代替データを入れるもの（カレンダー / TOPIX / 投資部門別）を見分ける。
--    J-Quants の同期処理は source='jquants' を**明示的に**書くこと（upsert の DO UPDATE では
--    DEFAULT が効かず、代替行を公式値で上書きしても source が代替のまま残るため）。
-- 2. alt_equity_bar_daily / alt_split_events: Yahoo の日足（保有・ウォッチ銘柄＋固定ETF）。
--    equity_bar_daily には混ぜない（max(trade_date) を読む breadth / stock_metrics / technical /
--    鮮度チップが、数銘柄しか無い日を「全市場の最新日」と誤認するため）。
-- 3. v_equity_price_daily: 公式（DAY）＋代替の統合ビュー。同じ（銘柄, 日付）は公式を優先。
--    OFF 中に分割した銘柄は、公式行の adj_* にも分割比率を反映する（公式行は OFF 中に rebase されない）。
-- 4. market_indicators.breadth_source: 騰落レシオ・新高値/新安値の出所。
--
-- この migration を適用しただけでは既存の挙動は変わらない（列追加と新しい器のみ）。
-- ロールバック: supabase/rollbacks/00133_jquants_alt_sources.down.sql
-- ============================================================================

-- ============================================================================
-- 1. source 列（既存行は 'jquants'）
-- ============================================================================

ALTER TABLE jquants_core.trading_calendar
  ADD COLUMN IF NOT EXISTS source text NOT NULL DEFAULT 'jquants';
ALTER TABLE jquants_core.trading_calendar
  DROP CONSTRAINT IF EXISTS trading_calendar_source_chk;
ALTER TABLE jquants_core.trading_calendar
  ADD CONSTRAINT trading_calendar_source_chk CHECK (source IN ('jquants', 'generated'));
COMMENT ON COLUMN jquants_core.trading_calendar.source IS
  'jquants=J-Quants 公式 / generated=内閣府祝日CSV＋土日＋12/31〜1/3 から生成（OFF 中・00133）。生成行は J-Quants 行を上書きしない';

ALTER TABLE jquants_core.topix_bar_daily
  ADD COLUMN IF NOT EXISTS source text NOT NULL DEFAULT 'jquants';
ALTER TABLE jquants_core.topix_bar_daily
  DROP CONSTRAINT IF EXISTS topix_bar_daily_source_chk;
ALTER TABLE jquants_core.topix_bar_daily
  ADD CONSTRAINT topix_bar_daily_source_chk CHECK (source IN ('jquants', 'proxy_etf_13060'));
COMMENT ON COLUMN jquants_core.topix_bar_daily.source IS
  'jquants=J-Quants 公式 / proxy_etf_13060=最後の公式値を 1306 ETF の騰落で延長した推計（OFF 中・00133）。ON 復帰時は jquants-restore が削除して公式値を取り直す';

ALTER TABLE jquants_core.investor_type_trading
  ADD COLUMN IF NOT EXISTS source text NOT NULL DEFAULT 'jquants';
ALTER TABLE jquants_core.investor_type_trading
  DROP CONSTRAINT IF EXISTS investor_type_trading_source_chk;
ALTER TABLE jquants_core.investor_type_trading
  ADD CONSTRAINT investor_type_trading_source_chk CHECK (source IN ('jquants', 'jpx'));
COMMENT ON COLUMN jquants_core.investor_type_trading.source IS
  'jquants=J-Quants / jpx=JPX 週次Excel（OFF 中・00133。published_date は週末後の第4営業日の規則で算出）。ON 復帰時は jquants-restore が削除して取り直す';

-- ============================================================================
-- 2. Yahoo の日足（保有・ウォッチ銘柄＋固定ETF）
-- ============================================================================

CREATE TABLE IF NOT EXISTS jquants_core.alt_equity_bar_daily (
  local_code   text NOT NULL CHECK (local_code ~ '^[0-9]{3}[0-9A-Z][0-9]$'),
  trade_date   date NOT NULL,
  -- 生値（adj_* に「その日より後の分割比率の積」を掛けて戻した値）
  open         numeric(18,6),
  high         numeric(18,6),
  low          numeric(18,6),
  close        numeric(18,6) NOT NULL,
  volume       bigint,
  -- 調整値（Yahoo の quote 系列そのもの。取得時点で分割調整済み・配当未調整。
  -- 2026-09-29 分割の 1925/1904 で J-Quants の adj_close と一致を確認済み）
  adj_open     numeric(18,6),
  adj_high     numeric(18,6),
  adj_low      numeric(18,6),
  adj_close    numeric(18,6) NOT NULL,
  adj_volume   bigint,
  source       text NOT NULL DEFAULT 'yahoo' CHECK (source IN ('yahoo')),
  fetched_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (local_code, trade_date)
);
COMMENT ON TABLE jquants_core.alt_equity_bar_daily IS
  'J-Quants 代替の日足（00133）。保有・ウォッチ銘柄と固定ETFのみ。equity_bar_daily には混ぜない。読むときは v_equity_price_daily を使う';

CREATE TABLE IF NOT EXISTS jquants_core.alt_split_events (
  local_code   text NOT NULL CHECK (local_code ~ '^[0-9]{3}[0-9A-Z][0-9]$'),
  ex_date      date NOT NULL,
  -- 1株→ratio株（例: 1:2 分割なら 2、1:4 なら 4、2:1 併合なら 0.5）
  ratio        numeric(18,10) NOT NULL CHECK (ratio > 0),
  source       text NOT NULL DEFAULT 'yahoo' CHECK (source IN ('yahoo')),
  detected_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (local_code, ex_date)
);
COMMENT ON TABLE jquants_core.alt_split_events IS
  'Yahoo が報告した分割・併合（00133）。公式データの最終日より後の ex_date だけが v_equity_price_daily の公式行の調整に使われる';

ALTER TABLE jquants_core.alt_equity_bar_daily ENABLE ROW LEVEL SECURITY;
ALTER TABLE jquants_core.alt_split_events ENABLE ROW LEVEL SECURITY;

-- 冪等化: CREATE POLICY に IF NOT EXISTS が無いため作り直す（00068と同方針）
DROP POLICY IF EXISTS "authenticated_select" ON jquants_core.alt_equity_bar_daily;
CREATE POLICY "authenticated_select"
  ON jquants_core.alt_equity_bar_daily FOR SELECT TO authenticated USING (true);
DROP POLICY IF EXISTS "service_role_all" ON jquants_core.alt_equity_bar_daily;
CREATE POLICY "service_role_all"
  ON jquants_core.alt_equity_bar_daily FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "authenticated_select" ON jquants_core.alt_split_events;
CREATE POLICY "authenticated_select"
  ON jquants_core.alt_split_events FOR SELECT TO authenticated USING (true);
DROP POLICY IF EXISTS "service_role_all" ON jquants_core.alt_split_events;
CREATE POLICY "service_role_all"
  ON jquants_core.alt_split_events FOR ALL TO service_role USING (true) WITH CHECK (true);

GRANT SELECT ON jquants_core.alt_equity_bar_daily TO authenticated;
GRANT ALL ON jquants_core.alt_equity_bar_daily TO service_role;
GRANT SELECT ON jquants_core.alt_split_events TO authenticated;
GRANT ALL ON jquants_core.alt_split_events TO service_role;

-- ============================================================================
-- 3. 統合ビュー（公式 DAY ＋ 代替）
-- ============================================================================
-- security_invoker: 呼び出し元の権限で下の表を読む（Portfolio の fn_holdings_summary は
-- authenticated で動くため、両表の authenticated SELECT ポリシーがそのまま効く）。
--
-- 公式行の分割調整（OFF 中の分割だけ）:
--   ex_date > その銘柄の公式最終日 の alt_split_events について、trade_date < ex_date の
--   公式行の adj_* を比率の積で割る（adj_volume は掛ける）。ON 中・ON 復帰後は ex_date が
--   公式最終日以下になるので適用されない（rebase 00093 が J-Quants の係数で調整する）。
--   alt_split_events は通常空なので、行ごとの LATERAL は空表の索引探索1回で終わる。

CREATE OR REPLACE VIEW jquants_core.v_equity_price_daily
WITH (security_invoker = true) AS
SELECT
  b.local_code,
  b.trade_date,
  'DAY'::text AS session,
  b.open,
  b.high,
  b.low,
  b.close,
  b.volume,
  b.turnover_value,
  b.adjustment_factor,
  b.adj_open  / COALESCE(f.cum_ratio, 1) AS adj_open,
  b.adj_high  / COALESCE(f.cum_ratio, 1) AS adj_high,
  b.adj_low   / COALESCE(f.cum_ratio, 1) AS adj_low,
  b.adj_close / COALESCE(f.cum_ratio, 1) AS adj_close,
  round(b.adj_volume * COALESCE(f.cum_ratio, 1))::bigint AS adj_volume,
  'jquants'::text AS source
FROM jquants_core.equity_bar_daily b
LEFT JOIN LATERAL (
  SELECT exp(sum(ln(s.ratio))) AS cum_ratio
    FROM jquants_core.alt_split_events s
   WHERE s.local_code = b.local_code
     AND s.ex_date > b.trade_date
     AND s.ex_date > (
       SELECT max(m.trade_date)
         FROM jquants_core.equity_bar_daily m
        WHERE m.local_code = s.local_code
          AND m.session = 'DAY'
     )
) f ON true
WHERE b.session = 'DAY'
UNION ALL
SELECT
  a.local_code,
  a.trade_date,
  'DAY'::text AS session,
  a.open,
  a.high,
  a.low,
  a.close,
  a.volume,
  -- 売買代金は Yahoo に無いので終値×出来高の近似（macro-pair-rotation の流動性判定のみが使う）
  (a.close * a.volume)::numeric(24,6) AS turnover_value,
  -- J-Quants と同じ意味の調整係数（分割の ex-date に 1/比率。週足の集計が使う）
  COALESCE(
    (SELECT (1 / s.ratio)::numeric(18,10)
       FROM jquants_core.alt_split_events s
      WHERE s.local_code = a.local_code AND s.ex_date = a.trade_date),
    1
  )::numeric(18,10) AS adjustment_factor,
  a.adj_open,
  a.adj_high,
  a.adj_low,
  a.adj_close,
  a.adj_volume,
  a.source
FROM jquants_core.alt_equity_bar_daily a
WHERE NOT EXISTS (
  SELECT 1
    FROM jquants_core.equity_bar_daily b2
   WHERE b2.local_code = a.local_code
     AND b2.trade_date = a.trade_date
     AND b2.session = 'DAY'
);

COMMENT ON VIEW jquants_core.v_equity_price_daily IS
  '公式（equity_bar_daily DAY）＋代替（alt_equity_bar_daily）の統合日足（00133）。同じ（銘柄, 日付）は公式優先。session は互換のため常に DAY';

GRANT SELECT ON jquants_core.v_equity_price_daily TO authenticated, service_role;

-- ============================================================================
-- 4. 市場指標の breadth 出所
-- ============================================================================

ALTER TABLE analytics.market_indicators
  ADD COLUMN IF NOT EXISTS breadth_source text;
ALTER TABLE analytics.market_indicators
  DROP CONSTRAINT IF EXISTS market_indicators_breadth_source_chk;
ALTER TABLE analytics.market_indicators
  ADD CONSTRAINT market_indicators_breadth_source_chk
  CHECK (breadth_source IS NULL OR breadth_source IN ('self', 'nikkei225jp'));
COMMENT ON COLUMN analytics.market_indicators.breadth_source IS
  '騰落レシオ25日・新高値・新安値の出所（00133）。NULL/self=equity_bar_daily から自前計算 / nikkei225jp=J-Quants OFF 中の代替（daily2 col7/8/9）';
