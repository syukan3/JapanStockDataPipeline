# J-Quants API V2 データ同期基盤 実装計画

## 概要

**GitHub Actions（Cron）+ Vercel API Routes + Supabase Postgres** で J-Quants API V2 データを日次同期する基盤を構築する。

> **設計変更**: Vercel Hobby プランは Cron Jobs が1日2回までのため、**GitHub Actions** でスケジュール実行し、Vercel の API エンドポイントを HTTP 呼び出しする方式を採用。

### 運用要件

- 取り込み漏れが発生しない（停止後のキャッチアップ可能）
- 重複実行しても壊れない（冪等：idempotent）
- レートリミット・ページングを確実に処理する
- **Vercel Hobby + Supabase Free** で運用可能
- Supabaseに対し、ジョブの死活（ハートビート）を送信し、運用監視に使える状態にする

---

## ディレクトリ構成

```
JapanStockAnalyzer/
├── .github/
│   └── workflows/
│       ├── cron-a.yml              # 日次確定データ（JST 18:40）
│       ├── cron-b.yml              # 決算発表予定（JST 19:20）
│       ├── cron-c.yml              # 投資部門別（JST 12:10）
│       └── backup.yml              # 週次バックアップ
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
│       ├── notification/
│       │   ├── email.ts            # Resend クライアント
│       │   └── templates.ts        # メールテンプレート
│       └── utils/
│           ├── date.ts             # JST日付ユーティリティ
│           ├── retry.ts            # 指数バックオフ
│           ├── batch.ts            # バッチ処理（500-1000行単位）
│           └── logger.ts           # 構造化ロギング
├── supabase/migrations/
│   ├── 00001_create_schemas.sql
│   ├── 00002_create_ingest_tables.sql
│   ├── 00003_create_core_tables.sql
│   ├── 00004_enable_rls.sql
│   └── 00005_create_monitoring_views.sql
├── docs/
│   └── operations/
│       ├── README.md               # 運用ドキュメント
│       ├── env-variables.md        # 環境変数一覧
│       ├── cron-schedule.md        # Cron時刻対応表
│       ├── manual-resync.md        # 再実行手順
│       └── troubleshooting.md      # 障害時追跡方法
├── package.json
├── next.config.ts
├── .env.local.example
├── instrumentation.ts          # Sentry instrumentation hook
├── instrumentation-client.ts   # Sentry クライアント初期化
├── sentry.server.config.ts     # Sentry サーバー初期化
└── sentry.edge.config.ts       # Sentry Edge Runtime 初期化
```

---

## Cron設計（GitHub Actions + Vercel API）

### アーキテクチャ

```
┌─────────────────┐     HTTP POST      ┌─────────────────┐
│  GitHub Actions │ ─────────────────▶ │   Vercel API    │
│  (Scheduler)    │   + CRON_SECRET    │   (Handler)     │
└─────────────────┘                    └─────────────────┘
                                              │
                                              ▼
                                       ┌─────────────────┐
                                       │    Supabase     │
                                       │   (Postgres)    │
                                       └─────────────────┘
```

**選定理由**：
- Vercel Hobby プランは Cron Jobs が **1日2回まで**
- GitHub Actions は **無制限**（Private リポジトリでも月2,000分無料）
- Vercel API は HTTP エンドポイントとして利用（maxDuration 10秒制限あり）

### スケジュール設定

「営業日に毎日実行」の要件は、**Cron自体は毎日起動**し、**処理側で取引カレンダーに基づき営業日だけ処理**する方式で実現する。

### Cron A（翌朝：日次確定データの取り込み）

| 項目 | 値 |
|------|-----|
| 目的 | 前営業日分の「確定」データをまとめて取り込み |
| 推奨時刻 | JST 18:40（**UTC 09:40**） |
| GitHub Actions | `cron: '40 9 * * *'` |

**処理対象**：
- 株価四本値（日足）：`date = 前営業日`
- 財務サマリー：`date = 前営業日`
- TOPIX：`from=前営業日&to=前営業日`
- 銘柄マスタスナップショット：`date = 前営業日`
- 取引カレンダー更新：**±370日** の範囲を埋める

**キャッチアップ要件**：
- 前営業日までの間に未処理営業日があれば、最大N日分（環境変数 `SYNC_MAX_CATCHUP_DAYS`、デフォルト5）を一度の実行で順次処理
- 残りは次回以降に繰り越す

**Vercel maxDuration 10秒対策**：
- 1回のAPI呼び出しでは1データセットのみ処理
- GitHub Actions から複数回連続で呼び出す（A-1, A-2, A-3...）

### Cron B（営業日夜：翌営業日を返す系の取り込み）

| 項目 | 値 |
|------|-----|
| 目的 | Earnings Calendar（翌営業日分）を安全に取得 |
| 推奨時刻 | JST 19:20（**UTC 10:20**） |
| GitHub Actions | `cron: '20 10 * * *'` |

**処理対象**：
- 決算発表予定（Earnings Calendar）：APIが「翌営業日」分を返す

**運用保険**：
- **土日祝も起動して良い**（APIは「次の営業日」を返すため、金曜夜に失敗しても土曜/日曜夜に月曜分を拾える）

### Cron C（日中：週次・不定期系の追随＋整合性チェック）

| 項目 | 値 |
|------|-----|
| 目的 | 投資部門別（週次）の追随、整合性チェック |
| 推奨時刻 | JST 12:10（**UTC 03:10**） |
| GitHub Actions | `cron: '10 3 * * *'` |

**処理対象**：
- 投資部門別（Investor Types）
  - `from/to` を「スライディングウィンドウ」（環境変数 `INVESTOR_TYPES_WINDOW_DAYS`、デフォルト60日）で取得しupsert
  - 訂正（再公表）対応：DBキーに `published_date` を含める
- 整合性チェック（trading_calendar の未来分が埋まっているか等）

### GitHub Actions ワークフロー例

```yaml
# .github/workflows/cron-a.yml
name: Cron A - Daily Data Sync

on:
  schedule:
    - cron: '40 9 * * *'  # JST 18:40
  workflow_dispatch:       # 手動実行も可能

jobs:
  sync:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - name: Trigger Cron A - Calendar
        run: |
          curl -X POST "${{ secrets.VERCEL_URL }}/api/cron/jquants/a" \
            -H "Authorization: Bearer ${{ secrets.CRON_SECRET }}" \
            -H "Content-Type: application/json" \
            -d '{"dataset": "calendar"}' \
            --fail --silent --show-error
        continue-on-error: false

      - name: Trigger Cron A - Equity Bars
        run: |
          curl -X POST "${{ secrets.VERCEL_URL }}/api/cron/jquants/a" \
            -H "Authorization: Bearer ${{ secrets.CRON_SECRET }}" \
            -H "Content-Type: application/json" \
            -d '{"dataset": "equity_bars"}' \
            --fail --silent --show-error

      - name: Trigger Cron A - TOPIX
        run: |
          curl -X POST "${{ secrets.VERCEL_URL }}/api/cron/jquants/a" \
            -H "Authorization: Bearer ${{ secrets.CRON_SECRET }}" \
            -H "Content-Type: application/json" \
            -d '{"dataset": "topix"}' \
            --fail --silent --show-error

      - name: Trigger Cron A - Financial
        run: |
          curl -X POST "${{ secrets.VERCEL_URL }}/api/cron/jquants/a" \
            -H "Authorization: Bearer ${{ secrets.CRON_SECRET }}" \
            -H "Content-Type: application/json" \
            -d '{"dataset": "financial"}' \
            --fail --silent --show-error

      - name: Trigger Cron A - Equity Master
        run: |
          curl -X POST "${{ secrets.VERCEL_URL }}/api/cron/jquants/a" \
            -H "Authorization: Bearer ${{ secrets.CRON_SECRET }}" \
            -H "Content-Type: application/json" \
            -d '{"dataset": "equity_master"}' \
            --fail --silent --show-error

      - name: Notify on failure
        if: failure()
        run: |
          curl -X POST "${{ secrets.VERCEL_URL }}/api/notify/failure" \
            -H "Authorization: Bearer ${{ secrets.CRON_SECRET }}" \
            -H "Content-Type: application/json" \
            -d '{"job": "cron_a", "workflow_run_id": "${{ github.run_id }}"}'
```

### GitHub Actions Secrets 設定

| Secret 名 | 値 |
|-----------|-----|
| `VERCEL_URL` | `https://your-app.vercel.app` |
| `CRON_SECRET` | Vercel と同じ値 |

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
   - Next.js 16.1.4, @supabase/supabase-js, @supabase/ssr, zod, resend
   - TypeScript, Vitest, ESLint

2. **next.config.ts / tsconfig.json**

3. **GitHub Actions ワークフロー作成**
   - `.github/workflows/cron-a.yml` - 日次確定データ
   - `.github/workflows/cron-b.yml` - 決算発表予定
   - `.github/workflows/cron-c.yml` - 投資部門別
   - `.github/workflows/backup.yml` - 週次バックアップ

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

-- 監視クエリ用インデックス（失敗ジョブ検索、実行履歴表示）
create index if not exists idx_job_runs_status
  on jquants_ingest.job_runs (status)
  where status = 'failed';  -- 部分インデックス：失敗のみ

create index if not exists idx_job_runs_started_at
  on jquants_ingest.job_runs (started_at desc);

-- job_name + status + started_at の複合インデックス（監視クエリ最適化）
-- WHERE job_name = ? AND status = ? ORDER BY started_at DESC パターン対応
create index if not exists idx_job_runs_job_status_started
  on jquants_ingest.job_runs (job_name, status, started_at desc);

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

-- ロック期限切れチェック用インデックス（CRITICAL: 期限切れロック検索の高速化）
-- 部分インデックス：期限切れロックのみ対象で検索スペースを削減
create index if not exists idx_job_locks_expired
  on jquants_ingest.job_locks (locked_until, job_name)
  where locked_until < now();

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

-- STALE判定用部分インデックス（監視ビュー最適化）
create index if not exists idx_job_heartbeat_stale
  on jquants_ingest.job_heartbeat (last_seen_at)
  where last_seen_at < now() - interval '25 hours';
```

#### 00003_create_core_tables.sql

```sql
-- 1) 上場銘柄マスタ（日次スナップショット）
-- ※ local_code は text を使用（char(5)はパディングで非効率、Supabase推奨）
create table if not exists jquants_core.equity_master_snapshot (
  as_of_date       date not null,
  local_code       text not null,
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
  local_code        text not null,  -- char(5)→text（Supabase推奨）
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

-- カバリングインデックス：日付検索時にテーブルアクセス削減
create index if not exists idx_equity_bar_daily_date
  on jquants_core.equity_bar_daily (trade_date)
  include (local_code, close, volume);

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
  local_code        text,  -- char(5)→text

  raw_json          jsonb not null,
  ingested_at       timestamptz not null default now()
);

create index if not exists idx_financial_disclosure_code_date
  on jquants_core.financial_disclosure (local_code, disclosed_date desc);

-- 7) 決算発表予定（翌営業日分）
create table if not exists jquants_core.earnings_calendar (
  announcement_date date not null,
  local_code        text not null,  -- char(5)→text
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

V2プラン別レート制限:
| プラン | リクエスト/分 |
|--------|--------------|
| Free | 5 |
| Light | **60**（本システム採用） |
| Standard | 120 |
| Premium | 500 |

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

#### 3.4 src/lib/utils/batch.ts
- **バッチ INSERT サイズ: 500-1000 行/回**
- Supabase の推奨（1MB/リクエスト制限、ネットワークオーバーヘッド考慮）
- 大量データは分割して upsert

```typescript
// src/lib/utils/batch.ts

// テーブル別バッチサイズ最適化（Supabase 1MB/リクエスト制限考慮）
const BATCH_SIZES: Record<string, number> = {
  'equity_bar_daily': 1000,       // ~1.5KB/行 × 1000 = 1.5MB
  'equity_master_snapshot': 500,  // ~3KB/行 × 500 = 1.5MB
  'investor_type_trading': 2000,  // ~0.7KB/行 × 2000 = 1.4MB
  'financial_disclosure': 500,    // ~2KB/行 × 500 = 1MB
  'earnings_calendar': 1000,      // ~1KB/行 × 1000 = 1MB
  'trading_calendar': 2000,       // ~0.5KB/行 × 2000 = 1MB
  'topix_bar_daily': 2000,        // ~0.5KB/行 × 2000 = 1MB
};
const DEFAULT_BATCH_SIZE = 500;

export async function batchUpsert<T extends Record<string, unknown>>(
  supabase: SupabaseClient,
  table: string,
  data: T[],
  onConflict: string,
  options?: { batchSize?: number }
): Promise<{ inserted: number; errors: Error[] }> {
  // テーブル名から最適なバッチサイズを選択
  const tableName = table.split('.').pop() ?? table;
  const batchSize = options?.batchSize ?? BATCH_SIZES[tableName] ?? DEFAULT_BATCH_SIZE;

  const chunks = chunkArray(data, batchSize);
  let inserted = 0;
  const errors: Error[] = [];

  for (const chunk of chunks) {
    const { error, count } = await supabase
      .from(table)
      .upsert(chunk, { onConflict, count: 'exact' });

    if (error) {
      errors.push(error);
    } else {
      inserted += count ?? chunk.length;
    }
  }

  return { inserted, errors };
}

function chunkArray<T>(array: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size));
  }
  return chunks;
}
```

#### 3.5 src/lib/jquants/client.ts
- ヘッダ: `x-api-key: <API_KEY>`（V2はAPIキー方式）
- `pagination_key` 全消化
- `requestPaginated()` ジェネレーター

#### 3.6 src/lib/supabase/admin.ts（Connection Pooling）
- **Supabase Pooler（Transaction mode）を使用**
- `SUPABASE_URL`（Pooler 経由、ポート 6543）を使用
- Direct Connection（ポート 5432）は長時間接続が必要な場合のみ

```typescript
// src/lib/supabase/admin.ts
import { createClient } from '@supabase/supabase-js';

// Pooler 経由（Transaction mode）- 推奨
export const supabaseAdmin = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!,
  {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
    },
    db: {
      schema: 'jquants_core',  // デフォルトスキーマ
    },
  }
);

// 別スキーマへのアクセス
export const supabaseIngest = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!,
  {
    auth: { persistSession: false, autoRefreshToken: false },
    db: { schema: 'jquants_ingest' },
  }
);
```

> **Transaction mode の制限事項**:
> - `SET` コマンド（セッション変数）使用不可
> - `LISTEN/NOTIFY` 使用不可
> - `pg_advisory_lock`（Advisory Lock）使用不可
> - Prepared statements 使用不可

#### 3.7 src/lib/cron/auth.ts
- `Authorization: Bearer ${CRON_SECRET}` 検証
- 不一致時は **401** を返す

#### 3.8 src/lib/cron/job-lock.ts
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

> **Advisory Lock との比較**:
> PostgreSQL の `pg_advisory_lock` は軽量で高速だが、Supabase の Pooler（Transaction mode）経由では**使用不可**（コネクションが固定されないため）。
> Direct Connection を使う場合は Advisory Lock も選択肢だが、本システムでは Pooler 互換性のためテーブルベースロックを採用。
>
> ```sql
> -- Advisory Lock（参考：Direct Connection のみ）
> SELECT pg_try_advisory_lock(hashtext('cron_a'));  -- 取得
> SELECT pg_advisory_unlock(hashtext('cron_a'));    -- 解放
> ```

#### 3.9 src/lib/utils/logger.ts（構造化ロギング）
- Vercel Logs で解析しやすい JSON 形式
- 各ログに `job_name`, `run_id`, `dataset` を付与

```typescript
// src/lib/utils/logger.ts
type LogLevel = 'debug' | 'info' | 'warn' | 'error';

interface LogContext {
  jobName?: string;
  runId?: string;
  dataset?: string;
  targetDate?: string;
  [key: string]: unknown;
}

export function createLogger(defaultContext: LogContext = {}) {
  const log = (level: LogLevel, message: string, context: LogContext = {}) => {
    const payload = {
      timestamp: new Date().toISOString(),
      level,
      message,
      ...defaultContext,
      ...context,
    };

    // Vercel Logs は JSON を解析可能
    if (level === 'error') {
      console.error(JSON.stringify(payload));
    } else if (level === 'warn') {
      console.warn(JSON.stringify(payload));
    } else {
      console.log(JSON.stringify(payload));
    }
  };

  return {
    debug: (msg: string, ctx?: LogContext) => log('debug', msg, ctx),
    info: (msg: string, ctx?: LogContext) => log('info', msg, ctx),
    warn: (msg: string, ctx?: LogContext) => log('warn', msg, ctx),
    error: (msg: string, ctx?: LogContext) => log('error', msg, ctx),
  };
}

// 使用例
// const logger = createLogger({ jobName: 'cron_a', runId: 'xxx' });
// logger.info('Processing dataset', { dataset: 'equity_bars', rowCount: 4000 });
```

#### 3.10 src/lib/cron/job-run.ts & heartbeat.ts
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
import { z } from 'zod';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';
export const maxDuration = 10;  // Vercel Hobby 制限

// リクエストボディのスキーマ（zod バリデーション）
const CronARequestSchema = z.object({
  dataset: z.enum(['calendar', 'equity_bars', 'topix', 'financial', 'equity_master']),
});

// GitHub Actions から POST で呼び出し
export async function POST(request: Request) {
  // 1. CRON_SECRET 認証（不一致で401）
  const authHeader = request.headers.get('authorization');
  if (authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    return Response.json({ error: 'Unauthorized' }, { status: 401 });
  }

  // 2. リクエストボディをパース＆バリデーション
  let body: unknown;
  try {
    body = await request.json();
  } catch {
    return Response.json({ error: 'Invalid JSON' }, { status: 400 });
  }

  const parsed = CronARequestSchema.safeParse(body);
  if (!parsed.success) {
    return Response.json({
      error: 'Validation failed',
      details: parsed.error.flatten(),
    }, { status: 400 });
  }

  const { dataset } = parsed.data;

  // 3. テーブルベースロック取得（取得失敗で即終了）
  // 4. job_runs INSERT (status=running)
  // 5. job_heartbeat UPSERT (status=running)
  // 6. 対象データセットのみ処理（10秒以内に完了）
  // 7. job_runs UPDATE (success/failed)
  // 8. job_heartbeat UPSERT (success/failed)
  // 9. ロック解放

  return Response.json({ success: true, dataset });
}
```

**maxDuration 10秒対策**：
- 1回のAPI呼び出しでは **1データセットのみ** 処理
- GitHub Actions から複数回連続で呼び出す
- 各呼び出しは独立して成功/失敗を返す

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
| @supabase/supabase-js | 2.91+ |
| @supabase/ssr | 0.8+ |
| zod | 4.3+ |
| vitest | 4.0+ |
| resend | 6.8+ |

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

## プラン制約と対策（完全無料運用）

### Vercel Hobby プラン制約

| 項目 | 制限 | 対策 |
|------|------|------|
| Function実行時間 | **最大10秒** | 1回のAPI呼び出しで1データセットのみ処理 |
| Cron Jobs | **1日2回まで** | **GitHub Actions で代替**（本設計で採用） |
| 帯域幅 | 100GB/月 | 問題なし |
| ビルド時間 | 6000分/月 | 問題なし |

### Supabase Free プラン制約

| 項目 | 制限 | 対策 |
|------|------|------|
| データベース容量 | **500MB** | 古いデータのアーカイブ/削除ポリシー |
| ストレージ | 1GB | 問題なし |
| 帯域幅 | 5GB/月 | 問題なし |
| 同時接続数 | 60（Pooler経由200） | 問題なし |
| プロジェクト休止 | **7日間非アクティブで自動休止** | 毎日Cronが動くため問題なし |

### GitHub Actions 無料枠

| 項目 | 制限 |
|------|------|
| Private リポジトリ | **2,000分/月** |
| Public リポジトリ | **無制限** |
| 同時実行 | 20ジョブ |

**本設計での消費見積もり**：
- Cron A/B/C × 30日 × 約2分 = 約180分/月（余裕あり）

### 容量見積もり（500MB制限）

| テーブル | 1日あたり | 1年あたり |
|---------|----------|----------|
| equity_bar_daily | 約4,000行 × 0.5KB = 2MB | 約500MB |
| その他 | 合計約0.5MB/日 | 約125MB |

**対策**：
- 1年程度で容量限界に達する見込み
- **1年以上前のデータを別ストレージにアーカイブ**するか、**Pro プラン**（8GB）にアップグレード
- 初期は問題なし

### 無料運用のまとめ

| サービス | プラン | 月額 | 備考 |
|---------|--------|------|------|
| Vercel | Hobby | **$0** | API エンドポイントのみ使用 |
| Supabase | Free | **$0** | 500MB、1年程度で見直し |
| GitHub Actions | Free | **$0** | Private でも 2,000分/月 |
| Resend | Free | **$0** | 100通/日、3,000通/月 |
| Sentry | Free（任意） | **$0** | 5,000 errors/月 |
| **合計** | | **$0** | |

---

## セキュリティ設計

### Row Level Security (RLS)

本システムは **バックエンド専用**（Cron/Service Role）のため、RLSは以下の方針で設計する。

#### 設計方針

1. **Service Role Key はRLSをバイパス**するため、Cron処理自体はRLS不要
2. 将来的にフロントエンドを追加する場合に備え、**デフォルトで全拒否**のRLSを有効化
3. `jquants_ingest` スキーマは管理用のため、一般ユーザーからはアクセス不可

#### RLS設定（00004_enable_rls.sql）

```sql
-- ================================================
-- jquants_core: データ参照用スキーマ
-- ================================================

-- 1) equity_master_snapshot
alter table jquants_core.equity_master_snapshot enable row level security;
alter table jquants_core.equity_master_snapshot force row level security;

-- 認証済みユーザーは読み取りのみ可能
create policy "authenticated_read_equity_master"
  on jquants_core.equity_master_snapshot
  for select
  to authenticated
  using (true);

-- 2) equity_bar_daily
alter table jquants_core.equity_bar_daily enable row level security;
alter table jquants_core.equity_bar_daily force row level security;

create policy "authenticated_read_equity_bar"
  on jquants_core.equity_bar_daily
  for select
  to authenticated
  using (true);

-- 3) topix_bar_daily
alter table jquants_core.topix_bar_daily enable row level security;
alter table jquants_core.topix_bar_daily force row level security;

create policy "authenticated_read_topix"
  on jquants_core.topix_bar_daily
  for select
  to authenticated
  using (true);

-- 4) trading_calendar
alter table jquants_core.trading_calendar enable row level security;
alter table jquants_core.trading_calendar force row level security;

create policy "authenticated_read_calendar"
  on jquants_core.trading_calendar
  for select
  to authenticated
  using (true);

-- 5) investor_type_trading
alter table jquants_core.investor_type_trading enable row level security;
alter table jquants_core.investor_type_trading force row level security;

create policy "authenticated_read_investor_type"
  on jquants_core.investor_type_trading
  for select
  to authenticated
  using (true);

-- 6) financial_disclosure
alter table jquants_core.financial_disclosure enable row level security;
alter table jquants_core.financial_disclosure force row level security;

create policy "authenticated_read_financial"
  on jquants_core.financial_disclosure
  for select
  to authenticated
  using (true);

-- 7) earnings_calendar
alter table jquants_core.earnings_calendar enable row level security;
alter table jquants_core.earnings_calendar force row level security;

create policy "authenticated_read_earnings"
  on jquants_core.earnings_calendar
  for select
  to authenticated
  using (true);

-- ================================================
-- jquants_ingest: 管理用スキーマ（一般ユーザーアクセス不可）
-- ================================================

-- job_runs: RLS有効化、ポリシーなし = 全拒否
alter table jquants_ingest.job_runs enable row level security;
alter table jquants_ingest.job_runs force row level security;
-- ポリシーなし = anon/authenticated からのアクセス全拒否

-- job_run_items
alter table jquants_ingest.job_run_items enable row level security;
alter table jquants_ingest.job_run_items force row level security;

-- job_locks
alter table jquants_ingest.job_locks enable row level security;
alter table jquants_ingest.job_locks force row level security;

-- job_heartbeat
alter table jquants_ingest.job_heartbeat enable row level security;
alter table jquants_ingest.job_heartbeat force row level security;
```

#### RLSパフォーマンス最適化

```sql
-- RLSで使用するカラムにはインデックスが必須
-- 本システムは単純な `using (true)` のため追加インデックス不要

-- 将来的にユーザー別フィルタリングを行う場合の例：
-- create policy "user_data_only" on some_table
--   for select
--   to authenticated
--   using ((select auth.uid()) = user_id);  -- SELECTでラップしてキャッシュ
```

---

### IP制限

#### Supabase側（Database接続）

Supabase Pro以上で利用可能：
- Dashboard > Project Settings > Database > Network Restrictions
- Vercel の固定IP（Pro以上）を許可リストに追加

**Free プランでの代替策**：
- Service Role Key の厳重管理（環境変数、ローテーション）
- RLSによる多層防御

#### Vercel側（Cron エンドポイント）

```typescript
// src/lib/cron/auth.ts
export function verifyCronRequest(request: Request): boolean {
  // 1. CRON_SECRET 検証（必須）
  const authHeader = request.headers.get('authorization');
  if (authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    return false;
  }

  // 2. User-Agent 検証（オプション、Vercel Cronは特定のUAを送信）
  const userAgent = request.headers.get('user-agent');
  if (!userAgent?.includes('vercel-cron')) {
    // ログ記録（不正アクセスの可能性）
    console.warn('Unexpected User-Agent for cron request');
  }

  return true;
}
```

---

## 異常通知（Resend Email）

### 概要

ジョブ失敗時に Resend API でメール通知を送信する。

### ディレクトリ追加

```
src/lib/
├── notification/
│   ├── email.ts          # Resend クライアント
│   └── templates.ts      # メールテンプレート
```

### 実装（src/lib/notification/email.ts）

```typescript
import { Resend } from 'resend';

const resend = new Resend(process.env.RESEND_API_KEY);

export interface JobFailureNotification {
  jobName: string;
  runId: string;
  targetDate: string;
  error: string;
  timestamp: Date;
}

export async function sendJobFailureEmail(data: JobFailureNotification): Promise<void> {
  try {
    await resend.emails.send({
      from: 'JapanStockAnalyzer <noreply@yourdomain.com>',
      to: process.env.ALERT_EMAIL_TO!,
      subject: `[ALERT] ${data.jobName} 失敗 - ${data.targetDate}`,
      html: `
        <h2>ジョブ失敗通知</h2>
        <table>
          <tr><td><strong>ジョブ名</strong></td><td>${data.jobName}</td></tr>
          <tr><td><strong>Run ID</strong></td><td>${data.runId}</td></tr>
          <tr><td><strong>対象日付</strong></td><td>${data.targetDate}</td></tr>
          <tr><td><strong>発生時刻</strong></td><td>${data.timestamp.toISOString()}</td></tr>
          <tr><td><strong>エラー</strong></td><td><pre>${data.error}</pre></td></tr>
        </table>
        <p>Supabase Dashboard で詳細を確認してください。</p>
      `,
    });
  } catch (error) {
    // 通知失敗はログに記録するが、ジョブ自体は続行
    console.error('Failed to send failure notification email:', error);
  }
}
```

### Cron ハンドラーへの統合

```typescript
// src/lib/cron/handlers/cron-a.ts
import { sendJobFailureEmail } from '@/lib/notification/email';

export async function handleCronA(runId: string, targetDate: string) {
  try {
    // ... 処理
  } catch (error) {
    // DB にエラー記録
    await updateJobRun(runId, 'failed', error.message);

    // メール通知
    await sendJobFailureEmail({
      jobName: 'cron_a',
      runId,
      targetDate,
      error: error.message,
      timestamp: new Date(),
    });

    throw error;
  }
}
```

### 環境変数追加

| 変数名 | 必須 | 説明 |
|--------|------|------|
| `RESEND_API_KEY` | ○ | Resend API キー |
| `ALERT_EMAIL_TO` | ○ | 通知先メールアドレス |

### Resend 無料プラン

| 項目 | 制限 | 本システムでの消費 |
|------|------|------------------|
| 送信数 | 100通/日、3,000通/月 | 失敗時のみ（通常0通） |
| ドメイン | 1ドメイン | 問題なし |

**無料で十分**: ジョブ失敗は稀なため、100通/日の制限に達することはほぼない。

---

## 環境分離（Free プラン対応）

### 概要

Supabase Free プランでは Database Branching は利用不可。
**複数プロジェクト方式**で開発・本番環境を分離する。

### Free プランでの環境分離

| 環境 | Supabase プロジェクト | Vercel 環境 |
|------|----------------------|-------------|
| 開発（local） | ローカル Docker | - |
| 開発（remote） | `jstock-dev` | Preview |
| 本番 | `jstock-prod` | Production |

> **注意**: Free プランは **1アカウント2プロジェクトまで**（Organization 作成で追加可能）

### 環境構築手順

#### 1. Supabase プロジェクト作成

```bash
# 本番用プロジェクト
supabase projects create jstock-prod --org-id xxx --region ap-northeast-1

# 開発用プロジェクト
supabase projects create jstock-dev --org-id xxx --region ap-northeast-1
```

#### 2. ローカル開発環境（Docker）

```bash
# Supabase CLI でローカル起動
supabase start

# マイグレーション適用
supabase db reset

# ローカル URL: http://localhost:54321
```

#### 3. Supabase CLI でプロジェクト切り替え

```bash
# 本番にリンク
supabase link --project-ref <prod-project-id>

# 開発にリンク
supabase link --project-ref <dev-project-id>

# マイグレーション適用
supabase db push
```

#### 4. Vercel 環境変数の分離

```bash
# Production 環境変数（Vercel Dashboard または CLI）
vercel env add NEXT_PUBLIC_SUPABASE_URL production
# → https://xxx-prod.supabase.co

vercel env add SUPABASE_SERVICE_ROLE_KEY production
# → prod-service-role-key

# Preview 環境変数
vercel env add NEXT_PUBLIC_SUPABASE_URL preview
# → https://xxx-dev.supabase.co

vercel env add SUPABASE_SERVICE_ROLE_KEY preview
# → dev-service-role-key
```

#### 5. GitHub Actions 環境の分離

```yaml
# .github/workflows/cron-a.yml
jobs:
  sync:
    runs-on: ubuntu-latest
    environment: production  # GitHub Environments で secrets を分離
    steps:
      - name: Trigger Cron A
        run: |
          curl -X POST "${{ secrets.VERCEL_URL }}/api/cron/jquants/a" \
            -H "Authorization: Bearer ${{ secrets.CRON_SECRET }}"
```

### マイグレーション運用

```bash
# 1. ローカルでマイグレーション作成
supabase migration new add_new_table

# 2. ローカルでテスト
supabase db reset

# 3. 開発環境に適用
supabase link --project-ref <dev-project-id>
supabase db push

# 4. 本番環境に適用（レビュー後）
supabase link --project-ref <prod-project-id>
supabase db push
```

### MCP での Supabase 操作

Supabase MCP Server を使用すると、Claude から直接 SQL 実行やスキーマ確認が可能：

```json
// claude_desktop_config.json
{
  "mcpServers": {
    "supabase-dev": {
      "command": "npx",
      "args": ["-y", "@supabase/mcp-server"],
      "env": {
        "SUPABASE_URL": "https://xxx-dev.supabase.co",
        "SUPABASE_SERVICE_ROLE_KEY": "xxx"
      }
    }
  }
}
```

**注意**: MCP は Database Branching の代替にはならない（単なる SQL クライアント）

---

## デプロイ・ロールバック手順

### デプロイフロー

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   develop   │ ──▶ │   Preview   │ ──▶ │ Production  │
│  (Git)      │     │  (Vercel)   │     │  (Vercel)   │
└─────────────┘     └─────────────┘     └─────────────┘
       │                   │                   │
       ▼                   ▼                   ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Preview    │     │  Preview    │     │ Production  │
│  Branch     │     │  Branch     │     │ Database    │
│  (Supabase) │     │  (Supabase) │     │ (Supabase)  │
└─────────────┘     └─────────────┘     └─────────────┘
```

### 1. 通常デプロイ

```bash
# 1. feature ブランチで開発
git checkout -b feature/add-new-endpoint

# 2. 変更をコミット
git add .
git commit -m "Add new endpoint"

# 3. develop にマージ（Preview 環境で自動デプロイ）
git checkout develop
git merge feature/add-new-endpoint
git push origin develop

# 4. Preview 環境で動作確認
# - Vercel Preview URL でテスト
# - Supabase Preview Branch でデータ確認

# 5. main にマージ（Production デプロイ）
git checkout main
git merge develop
git push origin main
```

### 2. Supabase マイグレーション

```bash
# 1. ローカルで新規マイグレーション作成
supabase migration new add_new_table

# 2. supabase/migrations/xxx_add_new_table.sql を編集

# 3. ローカルで適用テスト
supabase db reset

# 4. コミット＆プッシュ（Preview Branch に自動適用）
git add supabase/migrations/
git commit -m "Add migration: new table"
git push origin develop

# 5. 本番適用（main マージ時に自動）
git checkout main
git merge develop
git push origin main
```

### 3. ロールバック手順

#### Vercel ロールバック

```bash
# 方法1: Git revert
git revert HEAD
git push origin main
# Vercel が自動で再デプロイ

# 方法2: Vercel Dashboard
# Deployments > 対象デプロイ > ... > Redeploy
```

#### Supabase マイグレーション ロールバック

```sql
-- 手動でロールバックSQL実行
-- 例: テーブル追加を取り消す場合
DROP TABLE IF EXISTS new_table;

-- マイグレーション履歴から削除（必要に応じて）
DELETE FROM supabase_migrations.schema_migrations
WHERE version = 'xxx_add_new_table';
```

**注意**: データ破壊を伴うロールバックは慎重に。事前にバックアップ必須。

---

## バックアップ・リストア

### Supabase 自動バックアップ

| プラン | バックアップ頻度 | 保持期間 |
|--------|-----------------|----------|
| Free | 日次 | 7日間 |
| Pro | 日次 | 30日間 |
| Team | 日次 + PITR | 30日間 |

### 手動バックアップ（推奨）

```bash
# 1. pg_dump でバックアップ（週次で実行推奨）
pg_dump "postgresql://postgres:[PASSWORD]@db.[PROJECT].supabase.co:5432/postgres" \
  --schema=jquants_core \
  --schema=jquants_ingest \
  -F c \
  -f backup_$(date +%Y%m%d).dump

# 2. バックアップをクラウドストレージに保存
aws s3 cp backup_$(date +%Y%m%d).dump s3://your-bucket/backups/
```

### リストア手順

```bash
# 1. バックアップファイルを取得
aws s3 cp s3://your-bucket/backups/backup_20260125.dump .

# 2. リストア（既存データを上書き）
pg_restore \
  -d "postgresql://postgres:[PASSWORD]@db.[PROJECT].supabase.co:5432/postgres" \
  --clean \
  --if-exists \
  backup_20260125.dump

# 3. データ整合性確認
psql "postgresql://..." -c "SELECT COUNT(*) FROM jquants_core.equity_bar_daily;"
```

### 自動バックアップスクリプト（GitHub Actions）

```yaml
# .github/workflows/backup.yml
name: Database Backup

on:
  schedule:
    - cron: '0 15 * * 0'  # 毎週日曜 00:00 JST

jobs:
  backup:
    runs-on: ubuntu-latest
    steps:
      - name: Install PostgreSQL client
        run: sudo apt-get install -y postgresql-client

      - name: Create backup
        env:
          DATABASE_URL: ${{ secrets.DATABASE_URL }}
        run: |
          pg_dump "$DATABASE_URL" \
            --schema=jquants_core \
            --schema=jquants_ingest \
            -F c \
            -f backup_$(date +%Y%m%d).dump

      - name: Upload to S3
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: ap-northeast-1

      - run: aws s3 cp backup_*.dump s3://${{ secrets.BACKUP_BUCKET }}/backups/
```

---

## 監視・アラート設計

### 監視用ビュー

```sql
-- 00005_create_monitoring_views.sql

-- 直近24時間の失敗ジョブ
create or replace view jquants_ingest.v_failed_jobs_24h as
select
  job_name,
  target_date,
  status,
  error_message,
  started_at,
  finished_at
from jquants_ingest.job_runs
where status = 'failed'
  and started_at > now() - interval '24 hours'
order by started_at desc;

-- 各ジョブの最終実行状況
create or replace view jquants_ingest.v_job_status as
select
  h.job_name,
  h.last_status,
  h.last_seen_at,
  h.last_target_date,
  h.last_error,
  case
    when h.last_seen_at < now() - interval '25 hours' then 'STALE'
    when h.last_status = 'failed' then 'FAILED'
    else 'OK'
  end as health_status
from jquants_ingest.job_heartbeat h;

-- データ鮮度確認
create or replace view jquants_ingest.v_data_freshness as
select
  'equity_bar_daily' as dataset,
  max(trade_date) as latest_date,
  count(*) as total_rows
from jquants_core.equity_bar_daily
union all
select
  'trading_calendar',
  max(calendar_date),
  count(*)
from jquants_core.trading_calendar;
```

### アラート閾値

| 監視項目 | 閾値 | アクション |
|---------|------|----------|
| heartbeat 未更新 | 25時間 | メール通知 |
| 連続失敗回数 | 3回 | メール通知 |
| equity_bar_daily 更新なし | 2営業日 | メール通知 |

---

## Sentry 導入

### 無料プランで利用可能

| 項目 | 制限 | 本システムでの消費 |
|------|------|------------------|
| Errors | 5,000/月 | Cron失敗時のみ（通常0〜100） |
| Transactions | 10,000/月 | サーバー10% + クライアント1%で約1,200/月 |
| Replays | - | 無効化（0） |
| ユーザー | 1人 | 問題なし |

**無料で十分**: サンプリング設定により制限内に収まる。

### 導入手順

#### Step 1: パッケージインストール

```bash
npm install @sentry/nextjs
```

#### Step 2: Sentry設定ファイル作成（ルート直下）

**sentry.server.config.ts**（サーバーサイド）:
```typescript
import * as Sentry from '@sentry/nextjs';

Sentry.init({
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 0.1,  // サーバーサイド10%サンプリング
  environment: process.env.VERCEL_ENV || process.env.NODE_ENV,
  enabled: process.env.NODE_ENV === 'production',
});
```

**sentry.edge.config.ts**（Edge Runtime）:
```typescript
import * as Sentry from '@sentry/nextjs';

Sentry.init({
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 0.1,
  environment: process.env.VERCEL_ENV || process.env.NODE_ENV,
  enabled: process.env.NODE_ENV === 'production',
});
```

**instrumentation-client.ts**（クライアントサイド）:
```typescript
import * as Sentry from '@sentry/nextjs';

Sentry.init({
  dsn: process.env.NEXT_PUBLIC_SENTRY_DSN,
  tracesSampleRate: 0.01,  // クライアント1%（UI最小限）
  replaysSessionSampleRate: 0,  // Replay無効
  replaysOnErrorSampleRate: 0,
  environment: process.env.NEXT_PUBLIC_VERCEL_ENV || process.env.NODE_ENV,
  enabled: process.env.NODE_ENV === 'production',
});
```

**instrumentation.ts**（Next.js instrumentation hook）:
```typescript
import * as Sentry from '@sentry/nextjs';
import type { Instrumentation } from 'next';

export async function register() {
  if (process.env.NEXT_RUNTIME === 'nodejs') {
    await import('./sentry.server.config');
  }
  if (process.env.NEXT_RUNTIME === 'edge') {
    await import('./sentry.edge.config');
  }
}

export const onRequestError: Instrumentation.onRequestError = (...args) => {
  Sentry.captureRequestError(...args);
};
```

#### Step 3: next.config.ts修正

```typescript
import type { NextConfig } from 'next';
import { withSentryConfig } from '@sentry/nextjs';

const nextConfig: NextConfig = {
  experimental: {
    serverActions: { bodySizeLimit: '2mb' },
    clientTraceMetadata: ['sentry-trace', 'baggage'],
  },
};

export default withSentryConfig(nextConfig, {
  org: process.env.SENTRY_ORG,
  project: process.env.SENTRY_PROJECT,
  sourcemaps: { deleteSourcemapsAfterUpload: true },
  telemetry: false,
  silent: !process.env.CI,
});
```

#### Step 4: エラーバウンダリ作成

**src/app/global-error.tsx**:
```tsx
'use client';
import * as Sentry from '@sentry/nextjs';
import { useEffect } from 'react';

export default function GlobalError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    Sentry.captureException(error);
  }, [error]);

  return (
    <html lang="ja">
      <body>
        <main style={{ padding: '2rem', textAlign: 'center' }}>
          <h1>予期しないエラーが発生しました</h1>
          <button onClick={() => reset()}>再試行</button>
        </main>
      </body>
    </html>
  );
}
```

**src/app/error.tsx**:
```tsx
'use client';
import * as Sentry from '@sentry/nextjs';
import { useEffect } from 'react';

export default function Error({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    Sentry.captureException(error);
  }, [error]);

  return (
    <main style={{ padding: '2rem', textAlign: 'center' }}>
      <h2>エラーが発生しました</h2>
      <button onClick={() => reset()}>再試行</button>
    </main>
  );
}
```

### 環境変数（Sentry）

| 変数名 | 必須 | 説明 |
|--------|------|------|
| `SENTRY_DSN` | ○ | サーバーサイド用 DSN |
| `NEXT_PUBLIC_SENTRY_DSN` | ○ | クライアント用 DSN |
| `SENTRY_ORG` | ○ | Sentry 組織スラッグ |
| `SENTRY_PROJECT` | ○ | Sentry プロジェクト名 |
| `SENTRY_AUTH_TOKEN` | ○ | Source Maps アップロード用 |

### 動作確認方法

```typescript
// src/app/api/sentry-test/route.ts（確認後削除）
import * as Sentry from '@sentry/nextjs';
import { NextResponse } from 'next/server';

export async function GET() {
  throw new Error('Sentry Test Error');
}

export async function POST() {
  Sentry.captureMessage('Sentry Test Message', 'info');
  await Sentry.flush(2000);
  return NextResponse.json({ success: true });
}
```

```bash
# ローカル確認（enabled: true に一時変更）
npm run dev
curl http://localhost:3000/api/sentry-test

# Sentryダッシュボードでエラー確認
```

---

## 環境変数（更新版）

| 変数名 | 必須 | 説明 |
|--------|------|------|
| `NEXT_PUBLIC_SUPABASE_URL` | ○ | Supabase URL |
| `NEXT_PUBLIC_SUPABASE_ANON_KEY` | ○ | Supabase Anon Key |
| `SUPABASE_SERVICE_ROLE_KEY` | ○ | Service Role Key（サーバー専用） |
| `JQUANTS_API_KEY` | ○ | J-Quants V2 APIキー |
| `CRON_SECRET` | ○ | Cron認証シークレット |
| `RESEND_API_KEY` | ○ | Resend API キー |
| `ALERT_EMAIL_TO` | ○ | 通知先メールアドレス |
| `SENTRY_DSN` | ○ | Sentry サーバーサイド DSN |
| `NEXT_PUBLIC_SENTRY_DSN` | ○ | Sentry クライアント DSN |
| `SENTRY_ORG` | ○ | Sentry 組織スラッグ |
| `SENTRY_PROJECT` | ○ | Sentry プロジェクト名 |
| `SENTRY_AUTH_TOKEN` | ○ | Sentry Source Maps 用トークン |
| `SYNC_MAX_CATCHUP_DAYS` | - | Cron Aの追いつき上限（デフォルト: 5） |
| `INVESTOR_TYPES_WINDOW_DAYS` | - | Cron Cの取得窓（デフォルト: 60） |

---

## 補足（利用条件）

J-Quants APIは「個人向けサービス」として案内されています。アプリを第三者へ提供・再配布する場合は利用条件/規約の確認が必要です。
