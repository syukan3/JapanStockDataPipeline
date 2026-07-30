-- 00116_create_bottomup_per_bands_rpc.sql
-- /baskets（テーマバスケット割安判定）のボトムアップ軸を、価格系列の全転送から
-- DB側集計1コールへ置き換えるためのRPC。
--
-- 【背景】Portfolio の lib/queries/baskets.ts computeBottomupUpsides は、構成銘柄の
-- 株価2年分（最大バスケットで約6.5万行）と通期実績EPSを PostgREST 経由で全件取得し、
-- TS純関数 computeHistoricalPer で過去trailing PERレンジを計算していた。行数が
-- そのままネットワーク往復とJSONパースになり、コールド描画の最大のボトルネックだった。
-- 本RPCは同じ計算をDB内で行い、銘柄あたり1行（PERレンジ＋現在値＋予想EPS）だけを返す。
--
-- 【最重要】戻り値は TS純関数 computeHistoricalPer / computeBottomupUpsides と
-- **完全に同値**でなければならない（呼び出し側は同値検証テストで全構成銘柄を突き合わせる）。
-- 移植したセマンティクスを明示しておく:
--   1. 価格 = coalesce(adj_close, close)。この値が > 0 の行のみ採用する
--      （adj_close が 0 の行は close が正でも捨てる＝coalesce の評価順そのまま）。
--      同一日に複数セッション行があれば全行がPERサンプルになる。
--   2. FY実績は「決算期(fiscal_year_end)ごとに最新開示1行」へ畳んでから eps > 0 で絞る。
--      畳む順序は PostgREST 側の order と同じ (disclosed_date DESC, disclosure_id ASC)。
--      最新開示の eps が 0 以下ならその決算期は欠測扱いで、古い正のepsで代替しない。
--      なお eps IS NOT NULL だけは畳み込みより前（＝行の取得条件）に置く。これは TS 経路の
--      PostgREST クエリ自体が .eq("period_type","FY").not("eps","is",null) で行を絞っており、
--      EPS未記載の開示は畳み込みの候補にも forward_eps の先頭行にもならないため。
--      「畳み込み後」に効かせるのは eps > 0 のほうだけ（ここを入れ替えると同値が崩れる）。
--   3. 各価格行のPER = 価格 ÷「その日までに開示済みで fiscal_year_end 最大」の実績EPS。
--      過年度の訂正開示が後から来ても trailing EPS が巻き戻らないための決算期順選択。
--   4. サンプル5本未満はレンジ無効（min/median/max/count/sample_from を NULL で返す）。
--   5. current_price は「> 0 で採用した価格行」のうち (trade_date, session) 最大の値。
--      PERレンジが引けない銘柄でも返す（シナリオ目標株価の分母に使うため）。
--   6. forward_eps は (fiscal_year_end DESC, disclosed_date DESC, disclosure_id ASC) の
--      先頭行の next_forecast_eps。畳み込み前・eps>0 フィルタ前の順序である点に注意。
--
-- 【浮動小数】TS は JS の number（IEEE754 double）で除算・min/median/max・丸めを行う。
-- numeric のまま計算すると丸め位置が変わり得るため、PER計算は double precision で行い、
-- 丸めも Math.round(x*100)/100 と同じ floor(x*100 + 0.5)/100 で再現する
-- （PostgreSQL の round(double precision) は偶数丸めなので使わない）。
--
-- 【照合順序】fiscal_year_end は date ではなく text。TS 側の比較は JS の文字列比較
-- （コードポイント順）なので、決算期の大小比較には COLLATE "C" を明示する。一方で
-- 「畳み込み・先頭行の選択」は PostgREST が返す並び順の再現なのでDB既定の照合順序を使う。
-- 実データは全て 'YYYY-MM-DD' 固定長のため両者は一致するが、区別して書いておく。
--
-- 【権限】SECURITY INVOKER。呼び出しは Portfolio の cachedRef（service_role）が主で、
-- service_role キー未設定環境では authenticated にフォールバックするため両方に GRANT する
-- （00101 analytics.similar_stocks と同じ規約）。参照先の jquants_core.equity_bar_daily /
-- financial_disclosure は 00004 で authenticated に SELECT 済み。
--
-- 【タイムアウト】statement_timeout は上書きしない。想定外に重くなった場合は
-- 呼び出し側がエラーを検知して従来のTS経路へフォールバックする（フェイルセーフ）。
--
-- 【実行計画で気をつけた点】
--  a. 銘柄の絞り込みは CTE との結合ではなく local_code = ANY(p_codes) で書く。CTE 結合だと
--     プランナが行数を見誤って equity_bar_daily を Seq Scan し始める（100万行超の表なので致命的）。
--     ANY(...) なら PK (local_code, trade_date, session) の Index Scan に乗る。
--  b. current_price は集計用CTEを再走査せず、PKの後方インデックススキャン（DISTINCT ON）で取る。
--  c. 「その日時点の trailing 実績EPS」は自己結合ではなく dense_rank + 配列の running max で解く
--     （自己結合だとCTEの行数見積りが外れて銘柄横断のネステッドループ＝O(n^2) に落ちる）。
--     配列比較は要素ごとの辞書順なので、[決算期の順位, EPS] の max が「最大決算期のEPS」になる。
--     順位は double 化して照合順序の影響を受けない形にしてある。

CREATE OR REPLACE FUNCTION analytics.get_bottomup_per_bands(
  p_codes        text[],
  p_price_cutoff date
)
RETURNS TABLE (
  local_code    text,
  per_min       numeric,
  per_median    numeric,
  per_max       numeric,
  per_count     integer,
  sample_from   date,
  current_price numeric,
  forward_eps   numeric
)
LANGUAGE sql
STABLE
SECURITY INVOKER
SET search_path = ''
AS $$
WITH codes AS (
  SELECT DISTINCT u.code
  FROM unnest(COALESCE(p_codes, '{}'::text[])) AS u(code)
  WHERE u.code IS NOT NULL
),
-- FY実績の生行。WHERE は TS 経路の PostgREST クエリと同一条件（eps IS NOT NULL もそちら側にある）。
-- 2つの順位付けを同時に求める:
--   rn_fye  = 決算期内の最新開示（畳み込み用）
--   rn_code = 銘柄内の先頭行（forward_eps 用。畳み込み前・eps>0 フィルタ前）
fy AS (
  SELECT
    f.local_code        AS code,
    f.fiscal_year_end   AS fye,
    f.disclosed_date    AS disclosed_date,
    f.eps               AS eps,
    f.next_forecast_eps AS next_forecast_eps,
    row_number() OVER (
      PARTITION BY f.local_code, f.fiscal_year_end
      ORDER BY f.disclosed_date DESC, f.disclosure_id ASC
    ) AS rn_fye,
    row_number() OVER (
      PARTITION BY f.local_code
      ORDER BY f.fiscal_year_end DESC, f.disclosed_date DESC, f.disclosure_id ASC
    ) AS rn_code
  FROM jquants_core.financial_disclosure f
  WHERE f.local_code = ANY(p_codes)
    AND f.period_type = 'FY'
    AND f.eps IS NOT NULL
),
fwd AS (
  SELECT fy.code, fy.next_forecast_eps
  FROM fy
  WHERE fy.rn_code = 1
),
-- 畳み込み後に eps > 0 で絞る（順序を逆にすると赤字期を古い黒字期で代替してしまう）
actuals AS (
  SELECT
    fy.code,
    fy.fye,
    fy.disclosed_date,
    fy.eps::double precision AS eps
  FROM fy
  WHERE fy.rn_fye = 1
    AND fy.eps > 0
    AND fy.fye IS NOT NULL
    AND fy.disclosed_date IS NOT NULL
),
-- 決算期の大小を銘柄内の順位（整数）へ置き換える。以降の比較から text の照合順序を排除するため。
-- 畳み込み済みなので (code, fye) は一意 = 順位も一意。
ranked AS (
  SELECT
    a.code,
    a.disclosed_date,
    a.eps,
    dense_rank() OVER (PARTITION BY a.code ORDER BY a.fye COLLATE "C")::double precision AS fye_rank
  FROM actuals a
),
-- 開示日ごとの「その時点で最大の決算期のEPS」。配列 [順位, EPS] の running max で argmax を取る
-- （順位が一意なので EPS 側が比較に効くことはない）。ピア（同日開示）を必ず含めるため
-- フレームは既定の RANGE UNBOUNDED PRECEDING のままにする（ROWS だと同日行が別結果になり得る）。
runmax AS (
  SELECT DISTINCT
    r.code,
    r.disclosed_date AS eff_from,
    max(ARRAY[r.fye_rank, r.eps]) OVER (PARTITION BY r.code ORDER BY r.disclosed_date) AS best
  FROM ranked r
),
-- trailing EPS が有効な期間 [eff_from, eff_to)
eff_span AS (
  SELECT
    m.code,
    m.eff_from,
    m.best[2] AS eps,
    lead(m.eff_from) OVER (PARTITION BY m.code ORDER BY m.eff_from) AS eff_to
  FROM runmax m
),
-- MATERIALIZED 必須。インライン展開させると eff_span（プランナは数行と誤認する）を外側に
-- 置いたネステッドループになり、価格のインデックススキャンが開示回数ぶん繰り返される
-- （実測でバッファ読みが500倍に膨張した）。1回だけ読んで以降はハッシュ結合させる。
px AS MATERIALIZED (
  SELECT
    b.local_code AS code,
    b.trade_date,
    COALESCE(b.adj_close, b.close) AS price
  FROM jquants_core.equity_bar_daily b
  WHERE b.local_code = ANY(p_codes)
    AND b.trade_date >= p_price_cutoff
    AND COALESCE(b.adj_close, b.close) > 0
),
-- 採用価格の最終行。銘柄ごとの LATERAL + LIMIT 1 にしてあるのは、local_code が等値で固定される
-- ことで ORDER BY trade_date DESC, session DESC が PK (local_code, trade_date, session) の
-- 後方走査とそのまま一致し、1銘柄あたり数バッファで済むため。
-- DISTINCT ON + ORDER BY local_code ASC, trade_date DESC だと方向が混在してPK順に乗らず、
-- 該当行を全件読んでソートするプランになる（実測で価格行全件の外部ソートが発生した）。
cur AS (
  SELECT c.code, p.price
  FROM codes c
  CROSS JOIN LATERAL (
    SELECT COALESCE(b.adj_close, b.close) AS price
    FROM jquants_core.equity_bar_daily b
    WHERE b.local_code = c.code
      AND b.trade_date >= p_price_cutoff
      AND COALESCE(b.adj_close, b.close) > 0
    ORDER BY b.trade_date DESC, b.session DESC
    LIMIT 1
  ) p
),
per_rows AS (
  SELECT
    px.code,
    px.trade_date,
    px.price::double precision / e.eps AS per
  FROM px
  JOIN eff_span e
    ON e.code = px.code
   AND px.trade_date >= e.eff_from
   AND (e.eff_to IS NULL OR px.trade_date < e.eff_to)
),
bands AS (
  SELECT
    per_rows.code,
    count(*)::integer         AS n,
    min(per_rows.trade_date)  AS sample_from,
    array_agg(per_rows.per ORDER BY per_rows.per) AS sorted
  FROM per_rows
  GROUP BY per_rows.code
),
-- 5本未満はここで落とす＝レンジ関連の列がまとめて NULL になる（TS の null 返却と同じ）
stats AS (
  SELECT
    b.code,
    b.n,
    b.sample_from,
    b.sorted[1]   AS lo,
    b.sorted[b.n] AS hi,
    CASE
      WHEN b.n % 2 = 1 THEN b.sorted[(b.n + 1) / 2]
      ELSE (b.sorted[b.n / 2] + b.sorted[b.n / 2 + 1]) / 2::double precision
    END AS med
  FROM bands b
  WHERE b.n >= 5
)
SELECT
  c.code,
  (floor(s.lo  * 100::double precision + 0.5) / 100::double precision)::numeric,
  (floor(s.med * 100::double precision + 0.5) / 100::double precision)::numeric,
  (floor(s.hi  * 100::double precision + 0.5) / 100::double precision)::numeric,
  s.n,
  s.sample_from,
  cur.price,
  fwd.next_forecast_eps
FROM codes c
LEFT JOIN stats s ON s.code = c.code
LEFT JOIN cur   ON cur.code = c.code
LEFT JOIN fwd   ON fwd.code = c.code;
$$;

COMMENT ON FUNCTION analytics.get_bottomup_per_bands(text[], date) IS
  '構成銘柄ごとの過去trailing PERレンジ(min/median/max)・サンプル数・現在値・翌期会社予想EPSを1行にまとめて返す。/baskets のボトムアップ軸の価格系列全転送を置き換える。Portfolio の computeHistoricalPer と同値。';

REVOKE EXECUTE ON FUNCTION analytics.get_bottomup_per_bands(text[], date) FROM public, anon;
GRANT EXECUTE ON FUNCTION analytics.get_bottomup_per_bands(text[], date) TO authenticated, service_role;
