# J-Quants API V2 データ同期基盤 実装計画

## 概要

Vercel Cron Jobs (A/B/C) + Next.js App Router + Supabase Postgres で J-Quants API V2 データを日次同期する基盤を構築する。

### 運用要件

- 取り込み漏れが発生しない（停止後のキャッチアップ可能）
- 重複実行しても壊れない（冪等：idempotent）
- レートリミット・ページングを確実に処理する
- Vercel Cron の性質（UTC固定、失敗時の自動リトライ無し、並列起動可能性）を吸収する
- Supabaseに対し、ジョブの死活（ハートビート）を送信し、運用監視に使える状態にする

---

## ディレクトリ構成

```
JapanStockAnalyzer/
├── src/
│   ├── app/
│   │   ├── api/cron/jquants/
│   │   │   ├── a/route.ts          # Cron A: 日次確定データ
│   │   │   ├── b/route.ts          # Cron B: 決算発表予定
│   │   │   └── c/route.ts          # Cron C: 投資部門別
│   │   ├── layout.tsx
│   │   └── page.tsx
│   └── lib/
│       ├── jquants/
│       │   ├── client.ts           # API クライアント
│       │   ├── rate-limiter.ts     # レート制御 (60 req/min)
│       │   ├── types.ts            # 型定義
│       │   └── endpoints/          # 各エンドポイント
│       │       ├── trading-calendar.ts
│       │       ├── equity-master.ts
│       │       ├── equity-bars-daily.ts
│       │       ├── index-topix.ts
│       │       ├── fins-summary.ts
│       │       ├── earnings-calendar.ts
│       │       └── investor-types.ts
│       ├── supabase/
│       │   ├── client.ts           # ブラウザ用
│       │   └── admin.ts            # Service Role (Cron用)
│       ├── cron/
│       │   ├── auth.ts             # CRON_SECRET 検証
│       │   ├── job-lock.ts         # テーブルベースロック
│       │   ├── job-run.ts          # 実行ログ管理
│       │   ├── heartbeat.ts        # 死活監視
│       │   ├── catch-up.ts         # キャッチアップ
│       │   ├── business-day.ts     # 営業日判定
│       │   └── handlers/           # A/B/C ビジネスロジック
│       │       ├── cron-a.ts
│       │       ├── cron-b.ts
│       │       └── cron-c.ts
│       └── utils/
│           ├── date.ts             # JST日付ユーティリティ
│           ├── retry.ts            # 指数バックオフ
│           └── batch.ts            # バッチ処理
├── supabase/migrations/
│   ├── 00001_create_schemas.sql
│   ├── 00002_create_ingest_tables.sql
│   └── 00003_create_core_tables.sql
├── docs/
│   └── operations/
│       ├── README.md               # 運用ドキュメント
│       ├── env-variables.md        # 環境変数一覧
│       ├── cron-schedule.md        # Cron時刻対応表
│       ├── manual-resync.md        # 再実行手順
│       └── troubleshooting.md      # 障害時追跡方法
├── package.json
├── next.config.ts
├── vercel.json
└── .env.local.example
```

---

## Cron設計（A / B / C）

「営業日に毎日実行」の要件は、**Cron自体は毎日起動**し、**処理側で取引カレンダーに基づき営業日だけ処理**する方式で実現する。

### Cron A（翌朝：日次確定データの取り込み）

| 項目 | 値 |
|------|-----|
| 目的 | 前営業日分の「確定」データをまとめて取り込み |
| 推奨時刻 | JST 09:20（**UTC 00:20**） |
| スケジュール | `20 0 * * *` |

**処理対象**：
- 株価四本値（日足）：`date = 前営業日`
- 財務サマリー：`date = 前営業日`
- TOPIX：`from=前営業日&to=前営業日`
- 銘柄マスタスナップショット：`date = 前営業日`
- 取引カレンダー更新：**±370日** の範囲を埋める

**キャッチアップ要件**：
- 前営業日までの間に未処理営業日があれば、最大N日分（環境変数 `SYNC_MAX_CATCHUP_DAYS`、デフォルト5）を一度の実行で順次処理
- 残りは次回以降に繰り越す（Vercel Functionの実行時間制限対策）

### Cron B（営業日夜：翌営業日を返す系の取り込み）

| 項目 | 値 |
|------|-----|
| 目的 | Earnings Calendar（翌営業日分）を安全に取得 |
| 推奨時刻 | JST 19:20（**UTC 10:20**） |
| スケジュール | `20 10 * * *` |

**処理対象**：
- 決算発表予定（Earnings Calendar）：APIが「翌営業日」分を返す

**運用保険**：
- **土日祝も起動して良い**（APIは「次の営業日」を返すため、金曜夜に失敗しても土曜/日曜夜に月曜分を拾える）

### Cron C（日中：週次・不定期系の追随＋整合性チェック）

| 項目 | 値 |
|------|-----|
| 目的 | 投資部門別（週次）の追随、整合性チェック |
| 推奨時刻 | JST 12:10（**UTC 03:10**） |
| スケジュール | `10 3 * * *` |

**処理対象**：
- 投資部門別（Investor Types）
  - `from/to` を「スライディングウィンドウ」（環境変数 `INVESTOR_TYPES_WINDOW_DAYS`、デフォルト60日）で取得しupsert
  - 訂正（再公表）対応：DBキーに `published_date` を含める
- 整合性チェック（trading_calendar の未来分が埋まっているか等）

---

## 営業日判定ロジック

`trading_calendar.hol_div` の値に基づいて判定：

| hol_div | 意味 | 営業日判定 |
|---------|------|-----------|
| 0 | 非営業日 | **false** |
| 1 | 営業日 | **true** |
| 2 | 半日取引 | **true** |

```typescript
// src/lib/cron/business-day.ts
export function isBusinessDay(holDiv: string): boolean {
  return holDiv === '1' || holDiv === '2';
}
```

---

## 実装フェーズ

### Phase 1: プロジェクト初期設定

1. **package.json 作成**
   - Next.js 16.1.4, @supabase/supabase-js, @supabase/ssr, zod
   - TypeScript, Vitest, ESLint

2. **next.config.ts / tsconfig.json**

3. **vercel.json** - Cron スケジュール設定
   ```json
   {
     "crons": [
       { "path": "/api/cron/jquants/a", "schedule": "20 0 * * *" },
       { "path": "/api/cron/jquants/b", "schedule": "20 10 * * *" },
       { "path": "/api/cron/jquants/c", "schedule": "10 3 * * *" }
     ]
   }
   ```

4. **.env.local.example** 作成

---

### Phase 2: Supabase マイグレーション

#### 00001_create_schemas.sql

```sql
create schema if not exists jquants_core;
create schema if not exists jquants_ingest;
```

**注意**: Supabase Dashboard > Settings > API > API Exposed Schemas に `jquants_core`, `jquants_ingest` を追加する（またはpublicに置く運用でも可）。

#### 00002_create_ingest_tables.sql

```sql
-- ジョブ実行ログ
create table if not exists jquants_ingest.job_runs (
  run_id        uuid primary key default gen_random_uuid(),
  job_name      text not null,             -- 'cron_a' | 'cron_b' | 'cron_c'
  target_date   date,                      -- cron_aは「前営業日」、cron_bは「翌営業日」
  status        text not null default 'running',  -- running|success|failed
  started_at    timestamptz not null default now(),
  finished_at   timestamptz,
  error_message text,
  meta          jsonb not null default '{}'::jsonb
);

-- 冪等性：同一ジョブ×同一target_dateは1回に制限（A/B向け）
create unique index if not exists uq_job_runs_job_target
  on jquants_ingest.job_runs (job_name, target_date)
  where target_date is not null;

-- データセット単位のログ
create table if not exists jquants_ingest.job_run_items (
  run_id        uuid not null references jquants_ingest.job_runs(run_id) on delete cascade,
  dataset       text not null,  -- 'equity_bar_daily' 等
  status        text not null default 'running',
  row_count     bigint,
  page_count    bigint,
  started_at    timestamptz not null default now(),
  finished_at   timestamptz,
  error_message text,
  meta          jsonb not null default '{}'::jsonb,
  primary key (run_id, dataset)
);

-- ロック（同時実行防止）- テーブルベースロック
create table if not exists jquants_ingest.job_locks (
  job_name      text primary key,
  locked_until  timestamptz not null,
  lock_token    uuid not null,
  updated_at    timestamptz not null default now()
);

-- 死活監視（ハートビート）
create table if not exists jquants_ingest.job_heartbeat (
  job_name          text primary key,
  last_seen_at      timestamptz not null,
  last_status       text not null,         -- running|success|failed
  last_run_id       uuid,
  last_target_date  date,
  last_error        text,
  meta              jsonb not null default '{}'::jsonb
);
```

#### 00003_create_core_tables.sql

```sql
-- 1) 上場銘柄マスタ（日次スナップショット）
create table if not exists jquants_core.equity_master_snapshot (
  as_of_date       date not null,
  local_code       char(5) not null,
  company_name     text,
  company_name_en  text,
  sector17_code    text,
  sector17_name    text,
  sector33_code    text,
  sector33_name    text,
  scale_category   text,
  market_code      text,
  market_name      text,
  margin_code      text,
  margin_code_name text,
  raw_json         jsonb not null,
  ingested_at      timestamptz not null default now(),
  primary key (as_of_date, local_code)
);

create index if not exists idx_equity_master_snapshot_code
  on jquants_core.equity_master_snapshot (local_code, as_of_date desc);

-- 2) 株価（日足）
create table if not exists jquants_core.equity_bar_daily (
  trade_date        date not null,
  local_code        char(5) not null,
  session           text not null default 'DAY',

  open              numeric(18,6),
  high              numeric(18,6),
  low               numeric(18,6),
  close             numeric(18,6),
  volume            bigint,
  turnover_value    numeric(24,6),

  adjustment_factor numeric(18,10),
  adj_open          numeric(18,6),
  adj_high          numeric(18,6),
  adj_low           numeric(18,6),
  adj_close         numeric(18,6),
  adj_volume        bigint,

  raw_json          jsonb not null,
  ingested_at       timestamptz not null default now(),
  primary key (local_code, trade_date, session)
);

create index if not exists idx_equity_bar_daily_date
  on jquants_core.equity_bar_daily (trade_date);

-- 3) TOPIX（日次）
create table if not exists jquants_core.topix_bar_daily (
  trade_date   date primary key,
  open         numeric(18,6),
  high         numeric(18,6),
  low          numeric(18,6),
  close        numeric(18,6),
  raw_json     jsonb not null,
  ingested_at  timestamptz not null default now()
);

-- 4) 取引カレンダー
create table if not exists jquants_core.trading_calendar (
  calendar_date    date primary key,
  hol_div          text not null,
  is_business_day  boolean not null,
  raw_json         jsonb not null,
  ingested_at      timestamptz not null default now()
);

-- 5) 投資部門別（縦持ち・訂正対応：published_dateを主キーに含める）
create table if not exists jquants_core.investor_type_trading (
  published_date  date not null,
  start_date      date not null,
  end_date        date not null,
  section         text not null,

  investor_type   text not null,
  metric          text not null, -- sales/purchases/total/balance 等
  value_kjpy      numeric(24,6),

  raw_json        jsonb not null,
  ingested_at     timestamptz not null default now(),

  primary key (published_date, section, start_date, end_date, investor_type, metric)
);

create index if not exists idx_investor_type_trading_period
  on jquants_core.investor_type_trading (section, start_date, end_date);

-- 6) 財務（サマリー）
-- ※V2のレスポンスに合わせてPKを確定する必要あり（disclosure_id相当）
-- ※実装時にV2 fins/summaryの実際のレスポンスを確認し、一意キーを特定すること
create table if not exists jquants_core.financial_disclosure (
  disclosure_id     text primary key,  -- V2レスポンスの一意キーに合わせて調整
  disclosed_date    date,
  disclosed_time    time,
  local_code        char(5),

  raw_json          jsonb not null,
  ingested_at       timestamptz not null default now()
);

create index if not exists idx_financial_disclosure_code_date
  on jquants_core.financial_disclosure (local_code, disclosed_date desc);

-- 7) 決算発表予定（翌営業日分）
create table if not exists jquants_core.earnings_calendar (
  announcement_date date not null,
  local_code        char(5) not null,
  raw_json          jsonb not null,
  ingested_at       timestamptz not null default now(),
  primary key (announcement_date, local_code)
);
```

---

### Phase 3: 基盤ライブラリ

#### 3.1 src/lib/supabase/admin.ts
- Service Role Key でサーバー専用クライアント
- `jquants_core`, `jquants_ingest` スキーマへのアクセス

#### 3.2 src/lib/jquants/rate-limiter.ts
- Light: **60 req/min**
- 同時実行数は原則1（ページングも直列）
- 1リクエストごとに最低待機（**1000ms/req**）

#### 3.3 src/lib/utils/retry.ts
- 指数バックオフ: **0.5s → 1s → 2s → 4s → 8s...**
- ジッター付き
- 429/5xx で自動リトライ
- 最大リトライ回数: 5回

```typescript
// src/lib/utils/retry.ts
export interface RetryOptions {
  maxRetries?: number;      // default: 5
  baseDelayMs?: number;     // default: 500
  maxDelayMs?: number;      // default: 32000
  jitterMs?: number;        // default: 100
}

export async function withRetry<T>(
  fn: () => Promise<T>,
  options?: RetryOptions
): Promise<T> {
  const { maxRetries = 5, baseDelayMs = 500, maxDelayMs = 32000, jitterMs = 100 } = options ?? {};
  // 実装...
}
```

#### 3.4 src/lib/jquants/client.ts
- ヘッダ: `x-api-key: <API_KEY>`（V2はAPIキー方式）
- `pagination_key` 全消化
- `requestPaginated()` ジェネレーター

#### 3.5 src/lib/cron/auth.ts
- `Authorization: Bearer ${CRON_SECRET}` 検証
- 不一致時は **401** を返す

#### 3.6 src/lib/cron/job-lock.ts
- **テーブルベースロック**（`job_locks`テーブル）
- `locked_until` が過去なら更新して取得、現在なら取得失敗
- ロック取得できなければ即終了

```typescript
// src/lib/cron/job-lock.ts
export async function acquireLock(
  supabase: SupabaseClient,
  jobName: string,
  ttlSeconds: number = 600
): Promise<{ success: boolean; token?: string }> {
  const token = crypto.randomUUID();
  const lockedUntil = new Date(Date.now() + ttlSeconds * 1000);

  // locked_untilが過去なら更新、そうでなければ失敗
  // ...
}

export async function releaseLock(
  supabase: SupabaseClient,
  jobName: string,
  token: string
): Promise<void> {
  // ...
}
```

#### 3.7 src/lib/cron/job-run.ts & heartbeat.ts
- ジョブ開始/完了/失敗のログ記録
- 死活監視テーブル更新
- 失敗時の `error_message` は必ず保存（長すぎる場合は要約）

---

### Phase 4: J-Quants エンドポイント実装

各エンドポイント (`src/lib/jquants/endpoints/`):

| ファイル | エンドポイント | 備考 |
|---------|---------------|------|
| `trading-calendar.ts` | GET /v2/markets/calendar | `from/to` 指定可能 |
| `equity-master.ts` | GET /v2/equities/master | 非営業日指定時は次営業日の情報を返す |
| `equity-bars-daily.ts` | GET /v2/equities/bars/daily | `code` または `date` 必須、ページング |
| `index-topix.ts` | GET /v2/indices/bars/daily/topix | `from/to` 指定可能 |
| `fins-summary.ts` | GET /v2/fins/summary | `code` または `date` 必須、ページング |
| `earnings-calendar.ts` | GET /v2/equities/earnings-calendar | **翌営業日** の決算発表予定を返す |
| `investor-types.ts` | GET /v2/equities/investor-types | `section` または `from/to` 指定可能 |

---

### Phase 5: Cron ハンドラー実装

#### 5.1 src/lib/cron/handlers/cron-a.ts
- 前営業日の確定データ取得
- 株価、財務、TOPIX、銘柄マスタ
- カレンダー更新（**±370日**）
- キャッチアップロジック（最大 `SYNC_MAX_CATCHUP_DAYS` 営業日）

#### 5.2 src/lib/cron/handlers/cron-b.ts
- 翌営業日の決算発表予定
- 土日祝も起動可

#### 5.3 src/lib/cron/handlers/cron-c.ts
- 投資部門別（スライディングウィンドウ `INVESTOR_TYPES_WINDOW_DAYS` 日）
- 縦持ち変換（投資主体 × 指標）
- 整合性チェック

---

### Phase 6: Route Handler 実装

各ルート (`src/app/api/cron/jquants/{a,b,c}/route.ts`):

```typescript
export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';
export const maxDuration = 300;

export async function GET(request: Request) {
  // 1. CRON_SECRET 認証（不一致で401）
  // 2. テーブルベースロック取得（取得失敗で即終了）
  // 3. job_runs INSERT (status=running)
  // 4. job_heartbeat UPSERT (status=running)
  // 5. ハンドラー実行
  // 6. job_runs UPDATE (success/failed)
  // 7. job_heartbeat UPSERT (success/failed)
  // 8. ロック解放
}
```

---

### Phase 7: 運用ドキュメント作成

| ファイル | 内容 |
|---------|------|
| `docs/operations/README.md` | 運用概要 |
| `docs/operations/env-variables.md` | 環境変数一覧と説明 |
| `docs/operations/cron-schedule.md` | Cron時刻（UTC/JST対応表） |
| `docs/operations/manual-resync.md` | 特定日付の手動再同期手順 |
| `docs/operations/troubleshooting.md` | 障害時の追跡方法（runログの見方） |

---

## 環境変数

| 変数名 | 必須 | 説明 |
|--------|------|------|
| `NEXT_PUBLIC_SUPABASE_URL` | ○ | Supabase URL |
| `NEXT_PUBLIC_SUPABASE_ANON_KEY` | ○ | Supabase Anon Key |
| `SUPABASE_SERVICE_ROLE_KEY` | ○ | Service Role Key（サーバー専用） |
| `JQUANTS_API_KEY` | ○ | J-Quants V2 APIキー |
| `CRON_SECRET` | ○ | Cron認証シークレット |
| `SYNC_MAX_CATCHUP_DAYS` | - | Cron Aの追いつき上限（デフォルト: 5） |
| `INVESTOR_TYPES_WINDOW_DAYS` | - | Cron Cの取得窓（デフォルト: 60） |

---

## 検収条件（完了定義）

### 認証・セキュリティ
- [ ] Authorization未指定では **401** が返る
- [ ] CRON_SECRET一致で正常動作

### データ取得
- [ ] trading_calendarが埋まり、営業日判定に基づきCron Aが前営業日分を格納できる
- [ ] `hol_div=1` と `hol_div=2` を営業日として扱う
- [ ] 主要データセットで `pagination_key` が出る状況を再現し、全件取り切れる

### エラーハンドリング
- [ ] 429を模擬し、バックオフで復帰できる（または翌日キャッチアップできる）
- [ ] 失敗時に `job_runs.error_message` が記録される

### 冪等性
- [ ] 同一日付で手動再実行しても重複や破損がない（upsert・ユニーク制約で担保）

### 可観測性
- [ ] `job_runs` / `job_run_items` / `job_heartbeat` が成功・失敗ともに記録される

### 同時実行制御
- [ ] テーブルベースロックで二重起動が防止される

---

## 成果物一覧

### 1. Next.js（Vercel）側
- Cron A/B/C 各エンドポイント実装（`/api/cron/jquants/a|b|c`）
- J-Quants V2 クライアント（APIキー、ページング、レート制御、リトライ）
- Supabase upsert処理（バッチ分割、ログ記録、ハートビート送信）

### 2. Supabase側
- テーブル・インデックス作成SQL（migrations）
- （任意）監視用ビュー：heartbeat最終時刻から「異常」を判定するビュー

### 3. 運用ドキュメント
- 環境変数一覧
- Cron時刻（UTC/JST対応表）
- 再実行手順（特定日付の手動再同期）
- 障害時の追跡方法（runログの見方）

---

## 主要ファイル一覧

| ファイル | 役割 |
|---------|------|
| `src/lib/jquants/client.ts` | API クライアント (認証/レート/ページング/リトライ) |
| `src/lib/cron/job-lock.ts` | テーブルベースロックで同時実行防止 |
| `src/lib/cron/business-day.ts` | 営業日判定（hol_div=1,2） |
| `src/lib/cron/handlers/cron-a.ts` | メインのデータ同期ロジック |
| `src/app/api/cron/jquants/a/route.ts` | Cron A エントリーポイント |
| `supabase/migrations/00003_create_core_tables.sql` | データスキーマ |

---

## 重要な設計ポイント

1. **冪等性**: 全テーブルで UPSERT (ON CONFLICT DO UPDATE)
2. **排他制御**: テーブルベースロック（`job_locks`）で二重起動防止
3. **レートリミット**: 60 req/min + 1000ms/req + 指数バックオフ（0.5s→1s→2s→4s...）
4. **ページング**: `pagination_key` を完全消化
5. **キャッチアップ**: 未処理営業日を自動検出・順次処理（最大N日/回）
6. **死活監視**: 毎回 heartbeat テーブルを更新
7. **raw_json 保持**: API レスポンスをそのまま保存して取りこぼし防止
8. **営業日判定**: `hol_div=1`（営業日）と `hol_div=2`（半日取引）を営業日として扱う

---

## 実装時の注意事項

### fins/summary の主キー問題
V2 `fins/summary` が返す「開示を一意に識別できる項目」を確認し、`disclosure_id` を適切に設定すること。仕様に確実に合わせるため `raw_json` は必須。

### investor_type_trading の縦持ち変換
APIレスポンスを「投資主体（investor_type）× 指標（metric: sales/purchases/total/balance等）」に分解して保存。`published_date` を主キーに含めることで訂正・再公表に対応。

### Supabase Exposed Schema設定
`jquants_core`, `jquants_ingest` をAPI経由でアクセスする場合は、Supabase Dashboard > Settings > API > API Exposed Schemas に追加が必要。

---

## 技術スタック

| 技術 | バージョン |
|------|-----------|
| Next.js | 16.1.4 |
| TypeScript | 5.7+ |
| @supabase/supabase-js | 2.47+ |
| @supabase/ssr | 0.5+ |
| zod | 3.24+ |
| vitest | 3.0+ |

---

## 検証方法

### ローカル実行テスト
```bash
# 開発サーバー起動
npm run dev

# Cron エンドポイント呼び出し (認証ヘッダー付き)
curl -H "Authorization: Bearer $CRON_SECRET" \
     http://localhost:3000/api/cron/jquants/a

# 認証なしで401確認
curl http://localhost:3000/api/cron/jquants/a
```

### データ確認
```sql
-- 実行ログ確認
SELECT * FROM jquants_ingest.job_runs ORDER BY started_at DESC LIMIT 10;

-- 死活監視確認
SELECT * FROM jquants_ingest.job_heartbeat;

-- ロック状態確認
SELECT * FROM jquants_ingest.job_locks;

-- データ件数確認
SELECT COUNT(*) FROM jquants_core.equity_bar_daily;
```

### Vercel デプロイ後
- Cron Jobs ダッシュボードで実行履歴確認
- 401 が返ることを確認 (認証なしアクセス)
- ログで正常動作確認

---

## 補足（利用条件）

J-Quants APIは「個人向けサービス」として案内されています。アプリを第三者へ提供・再配布する場合は利用条件/規約の確認が必要です。
