-- === financial_disclosure: 全フィールドカラム化 ===
ALTER TABLE jquants_core.financial_disclosure
  -- 会計期間
  ADD COLUMN IF NOT EXISTS cur_per_start text,
  ADD COLUMN IF NOT EXISTS cur_per_end text,
  -- 希薄化EPS
  ADD COLUMN IF NOT EXISTS diluted_eps numeric(18,6),
  -- BS
  ADD COLUMN IF NOT EXISTS total_assets numeric(24,6),
  ADD COLUMN IF NOT EXISTS equity numeric(24,6),
  ADD COLUMN IF NOT EXISTS equity_to_asset_ratio numeric(10,4),
  -- CF
  ADD COLUMN IF NOT EXISTS cf_operating numeric(24,6),
  ADD COLUMN IF NOT EXISTS cf_investing numeric(24,6),
  ADD COLUMN IF NOT EXISTS cf_financing numeric(24,6),
  ADD COLUMN IF NOT EXISTS cash_equivalents numeric(24,6),
  -- ROA
  ADD COLUMN IF NOT EXISTS roa numeric(10,4),
  -- 配当
  ADD COLUMN IF NOT EXISTS dividend_1q numeric(18,6),
  ADD COLUMN IF NOT EXISTS dividend_2q numeric(18,6),
  ADD COLUMN IF NOT EXISTS dividend_3q numeric(18,6),
  ADD COLUMN IF NOT EXISTS dividend_fy numeric(18,6),
  ADD COLUMN IF NOT EXISTS dividend_annual numeric(18,6),
  ADD COLUMN IF NOT EXISTS dividend_unit text,
  -- 今期予想
  ADD COLUMN IF NOT EXISTS forecast_sales numeric(24,6),
  ADD COLUMN IF NOT EXISTS forecast_op numeric(24,6),
  ADD COLUMN IF NOT EXISTS forecast_odp numeric(24,6),
  ADD COLUMN IF NOT EXISTS forecast_np numeric(24,6),
  ADD COLUMN IF NOT EXISTS forecast_eps numeric(18,6),
  ADD COLUMN IF NOT EXISTS forecast_dividend_ann numeric(18,6),
  -- 来期予想
  ADD COLUMN IF NOT EXISTS next_forecast_sales numeric(24,6),
  ADD COLUMN IF NOT EXISTS next_forecast_op numeric(24,6),
  ADD COLUMN IF NOT EXISTS next_forecast_odp numeric(24,6),
  ADD COLUMN IF NOT EXISTS next_forecast_np numeric(24,6),
  ADD COLUMN IF NOT EXISTS next_forecast_eps numeric(18,6),
  ADD COLUMN IF NOT EXISTS next_forecast_dividend_ann numeric(18,6),
  -- 変更・修正フラグ
  ADD COLUMN IF NOT EXISTS material_change_subsidiary text,
  ADD COLUMN IF NOT EXISTS significant_change_content text,
  ADD COLUMN IF NOT EXISTS change_by_as_revision text,
  ADD COLUMN IF NOT EXISTS change_no_as_revision text,
  ADD COLUMN IF NOT EXISTS change_accounting_estimate text,
  ADD COLUMN IF NOT EXISTS retroactive_restatement text,
  -- 株式数
  ADD COLUMN IF NOT EXISTS shares_outstanding_fy numeric(24,0),
  ADD COLUMN IF NOT EXISTS treasury_shares_fy numeric(24,0),
  ADD COLUMN IF NOT EXISTS avg_shares numeric(24,0),
  -- 非連結
  ADD COLUMN IF NOT EXISTS nc_sales numeric(24,6),
  ADD COLUMN IF NOT EXISTS nc_op numeric(24,6),
  ADD COLUMN IF NOT EXISTS nc_odp numeric(24,6),
  ADD COLUMN IF NOT EXISTS nc_np numeric(24,6),
  ADD COLUMN IF NOT EXISTS nc_eps numeric(18,6),
  ADD COLUMN IF NOT EXISTS nc_total_assets numeric(24,6),
  ADD COLUMN IF NOT EXISTS nc_equity numeric(24,6),
  ADD COLUMN IF NOT EXISTS nc_equity_to_asset_ratio numeric(10,4),
  ADD COLUMN IF NOT EXISTS nc_bps numeric(18,6);

-- COMMENTs
COMMENT ON COLUMN jquants_core.financial_disclosure.cur_per_start IS '会計期間開始日 (CurPerSt)';
COMMENT ON COLUMN jquants_core.financial_disclosure.cur_per_end IS '会計期間終了日 (CurPerEn)';
COMMENT ON COLUMN jquants_core.financial_disclosure.diluted_eps IS '希薄化後EPS (DEPS)';
COMMENT ON COLUMN jquants_core.financial_disclosure.total_assets IS '総資産 (TA)';
COMMENT ON COLUMN jquants_core.financial_disclosure.equity IS '純資産 (Eq)';
COMMENT ON COLUMN jquants_core.financial_disclosure.equity_to_asset_ratio IS '自己資本比率 (EqAR)';
COMMENT ON COLUMN jquants_core.financial_disclosure.cf_operating IS '営業CF (CFO)';
COMMENT ON COLUMN jquants_core.financial_disclosure.cf_investing IS '投資CF (CFI)';
COMMENT ON COLUMN jquants_core.financial_disclosure.cf_financing IS '財務CF (CFF)';
COMMENT ON COLUMN jquants_core.financial_disclosure.cash_equivalents IS '現金同等物期末残高 (CashEq)';
COMMENT ON COLUMN jquants_core.financial_disclosure.roa IS 'ROA';
COMMENT ON COLUMN jquants_core.financial_disclosure.dividend_1q IS '第1四半期配当 (Div1Q)';
COMMENT ON COLUMN jquants_core.financial_disclosure.dividend_2q IS '第2四半期配当 (Div2Q)';
COMMENT ON COLUMN jquants_core.financial_disclosure.dividend_3q IS '第3四半期配当 (Div3Q)';
COMMENT ON COLUMN jquants_core.financial_disclosure.dividend_fy IS '期末配当 (DivFY)';
COMMENT ON COLUMN jquants_core.financial_disclosure.dividend_annual IS '年間配当 (DivAnn)';
COMMENT ON COLUMN jquants_core.financial_disclosure.dividend_unit IS '配当単位 (DivUnit)';
COMMENT ON COLUMN jquants_core.financial_disclosure.forecast_sales IS '予想売上高 (FSales)';
COMMENT ON COLUMN jquants_core.financial_disclosure.forecast_op IS '予想営業利益 (FOP)';
COMMENT ON COLUMN jquants_core.financial_disclosure.forecast_odp IS '予想経常利益 (FOdP)';
COMMENT ON COLUMN jquants_core.financial_disclosure.forecast_np IS '予想当期純利益 (FNP)';
COMMENT ON COLUMN jquants_core.financial_disclosure.forecast_eps IS '予想EPS (FEPS)';
COMMENT ON COLUMN jquants_core.financial_disclosure.forecast_dividend_ann IS '予想年間配当 (FDivAnn)';
COMMENT ON COLUMN jquants_core.financial_disclosure.next_forecast_sales IS '次期予想売上高 (NxFSales)';
COMMENT ON COLUMN jquants_core.financial_disclosure.next_forecast_op IS '次期予想営業利益 (NxFOP)';
COMMENT ON COLUMN jquants_core.financial_disclosure.next_forecast_odp IS '次期予想経常利益 (NxFOdP)';
COMMENT ON COLUMN jquants_core.financial_disclosure.next_forecast_np IS '次期予想当期純利益 (NxFNP)';
COMMENT ON COLUMN jquants_core.financial_disclosure.next_forecast_eps IS '次期予想EPS (NxFEPS)';
COMMENT ON COLUMN jquants_core.financial_disclosure.next_forecast_dividend_ann IS '次期予想年間配当 (NxFDivAnn)';
COMMENT ON COLUMN jquants_core.financial_disclosure.material_change_subsidiary IS '重要な子会社の異動 (MatChgSub)';
COMMENT ON COLUMN jquants_core.financial_disclosure.significant_change_content IS '経営内容の著しい変化 (SigChgInC)';
COMMENT ON COLUMN jquants_core.financial_disclosure.change_by_as_revision IS '会計基準変更による変更 (ChgByASRev)';
COMMENT ON COLUMN jquants_core.financial_disclosure.change_no_as_revision IS '会計基準変更以外の変更 (ChgNoASRev)';
COMMENT ON COLUMN jquants_core.financial_disclosure.change_accounting_estimate IS '会計上の見積もりの変更 (ChgAcEst)';
COMMENT ON COLUMN jquants_core.financial_disclosure.retroactive_restatement IS '遡及修正 (RetroRst)';
COMMENT ON COLUMN jquants_core.financial_disclosure.shares_outstanding_fy IS '期末発行済株式数 (ShOutFY)';
COMMENT ON COLUMN jquants_core.financial_disclosure.treasury_shares_fy IS '期末自己株式数 (TrShFY)';
COMMENT ON COLUMN jquants_core.financial_disclosure.avg_shares IS '期中平均株式数 (AvgSh)';
COMMENT ON COLUMN jquants_core.financial_disclosure.nc_sales IS '非連結売上高 (NCSales)';
COMMENT ON COLUMN jquants_core.financial_disclosure.nc_op IS '非連結営業利益 (NCOP)';
COMMENT ON COLUMN jquants_core.financial_disclosure.nc_odp IS '非連結経常利益 (NCOdP)';
COMMENT ON COLUMN jquants_core.financial_disclosure.nc_np IS '非連結当期純利益 (NCNP)';
COMMENT ON COLUMN jquants_core.financial_disclosure.nc_eps IS '非連結EPS (NCEPS)';
COMMENT ON COLUMN jquants_core.financial_disclosure.nc_total_assets IS '非連結総資産 (NCTA)';
COMMENT ON COLUMN jquants_core.financial_disclosure.nc_equity IS '非連結純資産 (NCEq)';
COMMENT ON COLUMN jquants_core.financial_disclosure.nc_equity_to_asset_ratio IS '非連結自己資本比率 (NCEqAR)';
COMMENT ON COLUMN jquants_core.financial_disclosure.nc_bps IS '非連結BPS (NCBPS)';

-- === equity_bar_daily: ストップ高/安フラグ ===
ALTER TABLE jquants_core.equity_bar_daily
  ADD COLUMN IF NOT EXISTS upper_limit text,
  ADD COLUMN IF NOT EXISTS lower_limit text;

COMMENT ON COLUMN jquants_core.equity_bar_daily.upper_limit IS 'ストップ高フラグ (UL/MUL/AUL)';
COMMENT ON COLUMN jquants_core.equity_bar_daily.lower_limit IS 'ストップ安フラグ (LL/MLL/ALL)';
