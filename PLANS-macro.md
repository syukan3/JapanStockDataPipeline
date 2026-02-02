# マクロ経済データパイプライン実装計画（Cron D）

## 概要

FRED API と e-Stat API から日米マクロ経済指標を取得し、`jquants_core.macro_indicator_daily` に蓄積する。Scouter のマクロ環境判定の上流データソースとなる。

## 取得指標一覧

### 米国指標（FRED API）

| カテゴリ | series_id | 指標名 | 更新頻度 |
|---------|-----------|--------|---------|
| 景気 | `NAPM` | ISM製造業PMI | 月次 |
| 景気 | `UNRATE` | 失業率 | 月次 |
| 金融 | `FEDFUNDS` | FF金利 | 月次 |
| 金融 | `NFCI` | シカゴ連銀金融環境指数 | 週次 |
| インフレ | `CPIAUCSL` | CPI（都市部全消費者） | 月次 |
| インフレ | `PCEPILFE` | コアPCE | 月次 |
| インフレ | `T10YIE` | 10年BEI（期待インフレ率） | 日次 |
| クレジット | `BAMLH0A0HYM2` | HYスプレッド（OAS） | 日次 |
| クレジット | `BAMLC0A4CBBB` | IGスプレッド（BBB OAS） | 日次 |
| 市場 | `VIXCLS` | VIX | 日次 |
| 金利 | `T10Y2Y` | 10Y-2Yスプレッド（イールドカーブ） | 日次 |
| 日本関連 | `IRSTCI01JPM156N` | 日銀政策金利（FRED経由） | 月次 |
| 日本関連 | `IRLTLT01JPM156N` | JGB10年利回り（FRED経由） | 月次 |
| 為替 | `DEXJPUS` | USD/JPY | 日次 |

### 日本指標（e-Stat API）

| カテゴリ | series_id（canonical） | source_series_id（statsDataId） | 指標名 | 更新頻度 |
|---------|----------------------|-------------------------------|--------|---------|
| 景気 | `estat_ci_leading` | `0003473620`（景気動向指数・CI一致指数を含む統計表。先行指数を抽出） | 景気動向指数CI先行 | 月次 |
| インフレ | `estat_core_cpi` | `0003421913`（消費者物価指数・全国。生鮮食品除く総合を抽出） | 消費者物価指数コアCPI | 月次 |

> **注**: statsDataId は事前調査で確定したもの。e-Stat API では `getStatsData?statsDataId=XXX` で取得し、レスポンスから該当系列をフィルタする。

> **短観（日銀短観）**: e-Stat ではなく日銀統計から取得。四半期。v1 では手動入力 or 後回しとし、v2 で自動化を検討。

### 派生指標（Scouter 側で計算）

| 指標 | 計算方法 | 備考 |
|------|---------|------|
| 日米金利差 | FEDFUNDS - IRSTCI01JPM156N | Scouter が計算 |
| CPI前年比 | CPIAUCSL の12ヶ月変化率 | Scouter が計算 |
| 日本コアCPI前年比 | 同上 | Scouter が計算 |

## テーブル設計

### マイグレーション: `00019_create_macro_indicator_daily.sql`

```sql
-- マクロ経済指標の日次データ格納テーブル
-- FRED API / e-Stat API から取得したデータを統一フォーマットで蓄積

CREATE TABLE jquants_core.macro_indicator_daily (
  indicator_date  DATE        NOT NULL,
  series_id       TEXT        NOT NULL,   -- canonical ID: FRED系列はそのまま('VIXCLS'等)、e-Stat系列は'estat_'プレフィックス('estat_ci_leading'等)
  source          TEXT        NOT NULL CHECK (source IN ('fred', 'estat')),
  value           NUMERIC,                -- NULL許容: FRED APIが'.'(欠損)を返す場合あり
  released_at     TIMESTAMPTZ,            -- 実際の公表日時（先読みバイアス防止用）
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (indicator_date, series_id)
);

-- series_id 命名規約:
--   FRED系列: FREDのseries_idをそのまま使用（例: VIXCLS, CPIAUCSL）
--   e-Stat系列: 'estat_' プレフィックス + 指標名（例: estat_ci_leading, estat_core_cpi）
-- → 異なるsourceで同名series_idが発生しない設計。macro_series_metadataで一元管理。

-- series_id 単体での検索用
CREATE INDEX idx_macro_indicator_series
  ON jquants_core.macro_indicator_daily (series_id, indicator_date DESC);

-- source でのフィルタ用
CREATE INDEX idx_macro_indicator_source
  ON jquants_core.macro_indicator_daily (source, indicator_date DESC);

-- RLS
ALTER TABLE jquants_core.macro_indicator_daily ENABLE ROW LEVEL SECURITY;

CREATE POLICY "authenticated_select" ON jquants_core.macro_indicator_daily
  FOR SELECT TO authenticated USING (TRUE);
GRANT SELECT ON jquants_core.macro_indicator_daily TO authenticated;

-- service_role は RLS バイパスで INSERT/UPDATE 可能

-- メタデータテーブル: 各系列の更新管理
CREATE TABLE jquants_core.macro_series_metadata (
  series_id           TEXT PRIMARY KEY,       -- canonical ID（macro_indicator_daily.series_idと一致）
  source              TEXT        NOT NULL CHECK (source IN ('fred', 'estat')),
  source_series_id    TEXT        NOT NULL,   -- API取得用ID（FRED: series_id, e-Stat: statsDataId）
  source_filter       JSONB,                  -- e-Stat用: レスポンスから特定系列を抽出するフィルタ条件
  category            TEXT        NOT NULL,   -- 'business_cycle', 'financial', 'inflation', etc.
  region              TEXT        NOT NULL,   -- 'us', 'jp', 'linkage'
  name_en             TEXT        NOT NULL,
  name_ja             TEXT        NOT NULL,
  frequency           TEXT        NOT NULL CHECK (frequency IN ('daily', 'weekly', 'monthly', 'quarterly')),
  last_fetched_at     TIMESTAMPTZ,
  last_value_date     DATE,                   -- 最新データの日付
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE jquants_core.macro_series_metadata ENABLE ROW LEVEL SECURITY;
CREATE POLICY "authenticated_select" ON jquants_core.macro_series_metadata
  FOR SELECT TO authenticated USING (TRUE);
GRANT SELECT ON jquants_core.macro_series_metadata TO authenticated;
```

### `released_at` について

- FRED API のレスポンスには `realtime_start` / `realtime_end` がある。これを `released_at` にマッピング
- e-Stat は公表日情報が限定的。取得日時（`now()`）を暫定で記録
- 将来のバックテスト時に「その時点で利用可能だったか」を検証するためのカラム
- **Scouter 側は必ず `released_at <= eval_date` でフィルタし、先読みバイアスを防止する**

### FRED vintage（改定値）の扱い

- FRED の経済指標（CPI, PCE, 失業率等）は後日改定される。現行設計では最新値で上書き（UPSERT）
- **v1 では改定値の上書きを許容する**。理由: リアルタイム判定用途では最新の改定値が最も正確
- **v2 でバックテスト対応を検討**: `realtime_start` を含む複合PK（`indicator_date, series_id, realtime_start`）で vintage を保持する設計に拡張可能
- 月次指標は Cron D 実行時に直近3ヶ月分を毎回再取得し、改定値を反映する

### FRED API の欠損値処理

- FRED API は欠損値を `"."` で返す（祝日等でデータなし）
- **欠損値（`"."`）はスキップして取り込まない**（`value IS NULL` の行は INSERT しない）
- スキップした件数はジョブログに記録し、異常に多い場合はアラート通知

## API クライアント設計

### FRED API クライアント (`src/lib/fred/client.ts`)

```
FRED API v1
- Base URL: https://api.stlouisfed.org/fred
- 認証: api_key クエリパラメータ
- レート制限: 120リクエスト/60秒（公式制限）
- 主要エンドポイント:
  - /series/observations?series_id=XXX&observation_start=YYYY-MM-DD&file_type=json
```

**実装方針**:
- 既存の J-Quants クライアントのパターン（レート制限 + リトライ + ログ）を踏襲
- FRED API キーは環境変数 `FRED_API_KEY` から取得
- 無料で取得可能（https://fred.stlouisfed.org/docs/api/api_key.html）

### e-Stat API クライアント (`src/lib/estat/client.ts`)

```
e-Stat API v3
- Base URL: https://api.e-stat.go.jp/rest/3.0/app
- 認証: appId クエリパラメータ
- 主要エンドポイント:
  - /json/getStatsData?statsDataId=XXX&appId=YYY
```

**実装方針**:
- FRED と同様のパターン
- e-Stat API キーは環境変数 `ESTAT_API_KEY` から取得
- 無料で取得可能（https://www.e-stat.go.jp/api/）

## Cron D 設計

### ルート: `src/app/api/cron/macro/route.ts`

```
POST /api/cron/macro
  Authorization: Bearer {CRON_SECRET}
  Body: { "source": "fred" | "estat" | "all" }
```

**処理フロー**:
1. `macro_series_metadata` から対象系列一覧を取得
2. 各系列について `last_value_date` 以降のデータを API から取得
3. `macro_indicator_daily` に UPSERT
4. `macro_series_metadata.last_fetched_at` / `last_value_date` を更新
5. ジョブログ記録（`jquants_ingest.job_runs`）

**初回実行**: 各系列の過去2年分をバックフィル。2回目以降は差分のみ。

### GitHub Actions: `.github/workflows/cron-d.yml`

```yaml
name: "Cron D: Macro Data"
on:
  schedule:
    - cron: '0 22 * * 0-4'   # UTC日〜木 22:00 = JST月〜金 07:00
  workflow_dispatch:
    inputs:
      source:
        type: choice
        options: [all, fred, estat]
        default: all
      backfill_days:
        type: number
        default: 0
        description: "過去N日分をバックフィル（0=差分のみ）"
```

**実行タイミングの根拠**:
- 米国市場閉場（JST 06:00）後にFREDデータ更新
- JST 07:00 に実行すれば前日の米国データが取得可能
- Scouter の高配当スクリーニング（JST 19:50）より十分前に完了

### Cron ジョブ名

- `job_name`: `'cron-d-macro'`
- `dataset`: `'macro_fred'` / `'macro_estat'`

> `jquants_ingest.job_runs.job_name` の CHECK 制約拡張が必要（マイグレーションに含める）。

## ファイル構成

```
src/
├── app/api/cron/macro/
│   └── route.ts                  # Cron D エンドポイント
├── lib/
│   ├── fred/
│   │   ├── client.ts             # FRED API クライアント
│   │   ├── types.ts              # FRED レスポンス型
│   │   └── series-config.ts      # 取得対象系列の定義
│   ├── estat/
│   │   ├── client.ts             # e-Stat API クライアント
│   │   ├── types.ts              # e-Stat レスポンス型
│   │   └── series-config.ts      # 取得対象系列の定義
│   └── cron/handlers/
│       └── macro.ts              # Cron D ハンドラ（メインロジック）
supabase/migrations/
└── 00019_create_macro_indicator_daily.sql
```

## 実装フェーズ

### Phase 1: DB スキーマ
- [ ] マイグレーション `00019_create_macro_indicator_daily.sql` 作成
- [ ] `job_runs.job_name` CHECK 制約の拡張
- [ ] Supabase にマイグレーション適用

### Phase 2: FRED API クライアント
- [ ] `src/lib/fred/client.ts` — API クライアント（レート制限+リトライ）
- [ ] `src/lib/fred/types.ts` — レスポンス型定義
- [ ] `src/lib/fred/series-config.ts` — 14系列の定義

### Phase 3: e-Stat API クライアント
- [ ] `src/lib/estat/client.ts` — API クライアント
- [ ] `src/lib/estat/types.ts` — レスポンス型定義
- [ ] `src/lib/estat/series-config.ts` — 2系列の定義

### Phase 4: Cron D ハンドラ
- [ ] `src/lib/cron/handlers/macro.ts` — メインロジック
- [ ] `src/app/api/cron/macro/route.ts` — API ルート

### Phase 5: GitHub Actions
- [ ] `.github/workflows/cron-d.yml`

### Phase 6: テスト
- [ ] FRED クライアントのユニットテスト
- [ ] e-Stat クライアントのユニットテスト
- [ ] Cron D ハンドラのユニットテスト
- [ ] ローカル実行でバックフィルテスト

### Phase 7: 検証
- [ ] FRED 全14系列の取得確認
- [ ] e-Stat 全2系列の取得確認
- [ ] `macro_series_metadata` の更新確認
- [ ] GitHub Actions workflow_dispatch で実行確認

## 環境変数（追加分）

| 変数 | 用途 |
|------|------|
| `FRED_API_KEY` | FRED API キー |
| `ESTAT_API_KEY` | e-Stat API キー |

## リスク・懸念事項

1. **e-Stat API の安定性**: レスポンス形式が複雑（XML/JSON のネスト）。パースの実装コストが FRED より高い
2. **統計表ID の特定**: e-Stat の景気動向指数・CPI は統計表IDの事前調査が必要
3. **FRED レート制限**: 14系列 × 差分取得であれば120 req/min に十分収まる
4. **短観の自動取得**: 日銀統計は API が限定的。v1 では対象外とし手動 or スクレイピングは v2 で検討
