-- マクロ経済指標の日次データ格納テーブル
-- FRED API / e-Stat API から取得したデータを統一フォーマットで蓄積

CREATE TABLE jquants_core.macro_indicator_daily (
  indicator_date  DATE        NOT NULL,
  series_id       TEXT        NOT NULL,
  source          TEXT        NOT NULL CHECK (source IN ('fred', 'estat')),
  value           NUMERIC,
  released_at     TIMESTAMPTZ,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (indicator_date, series_id)
);

-- series_id 単体での検索用
CREATE INDEX idx_macro_indicator_series
  ON jquants_core.macro_indicator_daily (series_id, indicator_date DESC);

-- source でのフィルタ用
CREATE INDEX idx_macro_indicator_source
  ON jquants_core.macro_indicator_daily (source, indicator_date DESC);

-- RLS
ALTER TABLE jquants_core.macro_indicator_daily ENABLE ROW LEVEL SECURITY;
ALTER TABLE jquants_core.macro_indicator_daily FORCE ROW LEVEL SECURITY;

CREATE POLICY "authenticated_select" ON jquants_core.macro_indicator_daily
  FOR SELECT TO authenticated USING (TRUE);
GRANT SELECT ON jquants_core.macro_indicator_daily TO authenticated;
GRANT ALL ON jquants_core.macro_indicator_daily TO service_role;

-- メタデータテーブル: 各系列の更新管理
CREATE TABLE jquants_core.macro_series_metadata (
  series_id           TEXT PRIMARY KEY,
  source              TEXT        NOT NULL CHECK (source IN ('fred', 'estat')),
  source_series_id    TEXT        NOT NULL,
  source_filter       JSONB,
  category            TEXT        NOT NULL,
  region              TEXT        NOT NULL,
  name_en             TEXT        NOT NULL,
  name_ja             TEXT        NOT NULL,
  frequency           TEXT        NOT NULL CHECK (frequency IN ('daily', 'weekly', 'monthly', 'quarterly')),
  last_fetched_at     TIMESTAMPTZ,
  last_value_date     DATE,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE jquants_core.macro_series_metadata ENABLE ROW LEVEL SECURITY;
ALTER TABLE jquants_core.macro_series_metadata FORCE ROW LEVEL SECURITY;
CREATE POLICY "authenticated_select" ON jquants_core.macro_series_metadata
  FOR SELECT TO authenticated USING (TRUE);
GRANT SELECT ON jquants_core.macro_series_metadata TO authenticated;
GRANT ALL ON jquants_core.macro_series_metadata TO service_role;

-- updated_at 自動更新トリガー
-- scouter.set_updated_at() は既存（00013で作成済み）だが、スキーマ跨ぎを避けるため jquants_core に同等関数を作成
CREATE OR REPLACE FUNCTION jquants_core.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_macro_indicator_updated_at
  BEFORE UPDATE ON jquants_core.macro_indicator_daily
  FOR EACH ROW EXECUTE FUNCTION jquants_core.set_updated_at();

-- job_runs の job_name CHECK制約に 'cron-d-macro' を追加
-- 既存制約: CHECK (job_name IN ('cron_a', 'cron_b', 'cron_c', 'scouter-high-dividend'))
ALTER TABLE jquants_ingest.job_runs
  DROP CONSTRAINT IF EXISTS job_runs_job_name_check;

ALTER TABLE jquants_ingest.job_runs
  ADD CONSTRAINT job_runs_job_name_check
  CHECK (job_name IN ('cron_a', 'cron_b', 'cron_c', 'scouter-high-dividend', 'cron-d-macro', 'scouter-macro-regime'));

-- 初期データ: 16系列分の macro_series_metadata
INSERT INTO jquants_core.macro_series_metadata (series_id, source, source_series_id, source_filter, category, region, name_en, name_ja, frequency) VALUES
  -- FRED 14系列
  ('NAPM',              'fred', 'NAPM',              NULL, 'business_cycle', 'us', 'ISM Manufacturing PMI',           'ISM製造業PMI',              'monthly'),
  ('UNRATE',            'fred', 'UNRATE',            NULL, 'business_cycle', 'us', 'Unemployment Rate',               '失業率',                    'monthly'),
  ('FEDFUNDS',          'fred', 'FEDFUNDS',          NULL, 'financial',      'us', 'Federal Funds Rate',              'FF金利',                    'monthly'),
  ('NFCI',              'fred', 'NFCI',              NULL, 'financial',      'us', 'Chicago Fed NFCI',                'シカゴ連銀金融環境指数',      'weekly'),
  ('CPIAUCSL',          'fred', 'CPIAUCSL',          NULL, 'inflation',      'us', 'CPI All Urban Consumers',         'CPI（都市部全消費者）',       'monthly'),
  ('PCEPILFE',          'fred', 'PCEPILFE',          NULL, 'inflation',      'us', 'Core PCE Price Index',            'コアPCE',                   'monthly'),
  ('T10YIE',            'fred', 'T10YIE',            NULL, 'inflation',      'us', '10-Year Breakeven Inflation',     '10年BEI',                   'daily'),
  ('BAMLH0A0HYM2',     'fred', 'BAMLH0A0HYM2',     NULL, 'credit',         'us', 'HY OAS Spread',                   'HYスプレッド（OAS）',        'daily'),
  ('BAMLC0A4CBBB',     'fred', 'BAMLC0A4CBBB',     NULL, 'credit',         'us', 'IG BBB OAS Spread',               'IGスプレッド（BBB OAS）',    'daily'),
  ('VIXCLS',            'fred', 'VIXCLS',            NULL, 'market',         'us', 'CBOE VIX',                        'VIX',                       'daily'),
  ('T10Y2Y',            'fred', 'T10Y2Y',            NULL, 'interest_rate',  'us', '10Y-2Y Treasury Spread',          '10Y-2Yスプレッド',           'daily'),
  ('IRSTCI01JPM156N',   'fred', 'IRSTCI01JPM156N',   NULL, 'interest_rate',  'jp', 'BOJ Policy Rate (via FRED)',      '日銀政策金利（FRED経由）',    'monthly'),
  ('IRLTLT01JPM156N',   'fred', 'IRLTLT01JPM156N',   NULL, 'interest_rate',  'jp', 'JGB 10Y Yield (via FRED)',        'JGB10年利回り（FRED経由）',   'monthly'),
  ('DEXJPUS',           'fred', 'DEXJPUS',           NULL, 'fx',             'linkage', 'USD/JPY Exchange Rate',       'USD/JPY',                   'daily'),
  -- e-Stat 2系列
  ('estat_ci_leading',  'estat', '0003473620', '{"cat01": "CI", "cat02": "先行指数"}', 'business_cycle', 'jp', 'Composite Index - Leading',   '景気動向指数CI先行',         'monthly'),
  ('estat_core_cpi',    'estat', '0003421913', '{"cat01": "生鮮食品を除く総合"}',       'inflation',      'jp', 'Core CPI (ex fresh food)',    '消費者物価指数コアCPI',       'monthly');
