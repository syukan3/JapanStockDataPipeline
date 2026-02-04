-- 日本M2マネーストックを削除（2017年でデータ更新停止のため）

DELETE FROM jquants_core.macro_series_metadata
WHERE series_id = 'MYAGM2JPM189S';

DELETE FROM jquants_core.macro_indicator_daily
WHERE series_id = 'MYAGM2JPM189S';
