# J-Quants API V2 データ同期基盤 タスクリスト

## 概要

実装計画: [000001_jquants-sync-implementation.md](../plan/000001_jquants-sync-implementation.md)

## V1→V2 変更点サマリ

J-Quants API V2（2025年12月リリース）の主な変更点:

### 認証方式

| 項目 | V1 | V2 |
|------|----|----|
| 認証方式 | ID Token / Refresh Token | **APIキー（x-api-keyヘッダー）** |
| トークン管理 | 要リフレッシュ | 有効期限なし（ダッシュボードで再発行可能） |

### エンドポイント変更

| データセット | V1 | V2 |
|--------------|----|----|
| 株価四本値 | `/v1/prices/daily_quotes` | `/v2/equities/bars/daily` |
| 前場四本値 | `/v1/prices/prices_am` | `/v2/equities/bars/daily/am` |
| 決算発表予定 | `/v1/fins/announcement` | `/v2/equities/earnings-calendar` |
| 投資部門別 | `/v1/markets/trades_spec` | `/v2/equities/investor-types` |
| 上場銘柄一覧 | `/v1/listed/info` | `/v2/equities/master` |
| 指数四本値 | `/v1/indices` | `/v2/indices/bars/daily` |
| 財務諸表 | `/v1/fins/fs_details` | `/v2/fins/details` |
| 取引カレンダー | `/v1/markets/calendar` | `/v2/markets/calendar` |

### レスポンス形式の変更

- **データキー**: すべて `data` キーで配列として返却
- **フィールド名短縮**: `Open`→`O`, `High`→`H`, `Volume`→`Vo`, `AdjustmentOpen`→`AdjO` 等

### プラン別レート制限

| プラン | リクエスト/分 |
|--------|--------------|
| Free | 5 |
| Light | **60**（本システム採用） |
| Standard | 120 |
| Premium | 500 |

### 廃止エンドポイント

- `/v1/token/auth_user`（トークン発行）
- `/v1/token/auth_refresh`（トークンリフレッシュ）

> 参照: [V1→V2移行ガイド](https://jpx-jquants.com/spec/migration-v1-v2)

## タスク一覧

### Phase 1: プロジェクト初期設定

- [x] package.json 作成
- [x] tsconfig.json 作成
- [x] next.config.ts 作成
- [x] .env.local.example 作成
- [x] src/app/layout.tsx 作成
- [x] src/app/page.tsx 作成
- [x] .github/workflows/cron-a.yml - 日次確定データスケジューラ（JST 18:40）
- [x] .github/workflows/cron-b.yml - 決算発表予定スケジューラ（JST 19:20）
- [x] .github/workflows/cron-c.yml - 投資部門別スケジューラ（JST 12:10）
- [x] .github/workflows/backup.yml - 週次バックアップ

### Phase 2: Supabase マイグレーション

- [x] 00001_create_schemas.sql - スキーマ作成 (jquants_core, jquants_ingest)
- [x] 00002_create_ingest_tables.sql - ジョブ管理テーブル
  - [x] job_locks
  - [x] job_runs
  - [x] job_run_items
  - [x] job_heartbeat
- [x] 00003_create_core_tables.sql - データテーブル
  - [x] trading_calendar
  - [x] equity_master_snapshot
  - [x] equity_bar_daily
  - [x] topix_bar_daily
  - [x] financial_disclosure
  - [x] earnings_calendar
  - [x] investor_type_trading
- [x] 00004_enable_rls.sql - RLS設定
- [x] 00005_create_monitoring_views.sql - 監視用ビュー

### Phase 3: 基盤ライブラリ

- [x] src/lib/supabase/client.ts - ブラウザ用クライアント
- [x] src/lib/supabase/admin.ts - Service Role クライアント
- [x] src/lib/supabase/server.ts - Server Components用クライアント
- [x] src/lib/jquants/types.ts - 型定義
- [x] src/lib/jquants/rate-limiter.ts - トークンバケット (60 req/min)
- [x] src/lib/jquants/client.ts - API クライアント
- [x] src/lib/utils/date.ts - JST日付ユーティリティ
- [x] src/lib/utils/retry.ts - 指数バックオフ
- [x] src/lib/utils/batch.ts - バッチ処理
- [x] src/lib/utils/logger.ts - 構造化ロギング
- [x] src/lib/utils/html.ts - HTML生成ユーティリティ
- [x] src/lib/cron/auth.ts - CRON_SECRET 検証
- [x] src/lib/cron/job-lock.ts - テーブルベースロック
- [x] src/lib/cron/job-run.ts - 実行ログ管理
- [x] src/lib/cron/heartbeat.ts - 死活監視
- [x] src/lib/cron/catch-up.ts - キャッチアップ
- [x] src/lib/cron/business-day.ts - 営業日判定
- [x] src/lib/notification/email.ts - Resend クライアント
- [x] src/lib/notification/templates.ts - メールテンプレート

### Phase 4: J-Quants エンドポイント

- [x] src/lib/jquants/endpoints/trading-calendar.ts
- [x] src/lib/jquants/endpoints/equity-master.ts
- [x] src/lib/jquants/endpoints/equity-bars-daily.ts
- [x] src/lib/jquants/endpoints/index-topix.ts
- [x] src/lib/jquants/endpoints/fins-summary.ts
- [x] src/lib/jquants/endpoints/earnings-calendar.ts
- [x] src/lib/jquants/endpoints/investor-types.ts

### Phase 5: Cron ハンドラー

- [x] src/lib/cron/handlers/cron-a.ts - 日次確定データ
  - 前営業日の株価、財務、TOPIX、銘柄マスタ、カレンダー
  - キャッチアップロジック (最大5営業日)
- [x] src/lib/cron/handlers/cron-b.ts - 決算発表予定
  - 翌営業日の決算発表予定
- [x] src/lib/cron/handlers/cron-c.ts - 投資部門別
  - スライディングウィンドウ60日
  - 整合性チェック
- [x] src/lib/cron/handlers/index.ts - re-exports

### Phase 6: Route Handler

- [x] src/app/api/cron/jquants/a/route.ts
- [x] src/app/api/cron/jquants/b/route.ts
- [x] src/app/api/cron/jquants/c/route.ts

### Phase 7: 運用ドキュメント

- [x] docs/operations/README.md - 運用概要
- [x] docs/operations/env-variables.md - 環境変数一覧
- [x] docs/operations/cron-schedule.md - Cron時刻対応表
- [x] docs/operations/manual-resync.md - 再実行手順
- [x] docs/operations/troubleshooting.md - 障害時追跡方法

### Phase 8: Sentry 導入

- [x] @sentry/nextjs インストール
- [x] sentry.server.config.ts 作成
- [x] sentry.edge.config.ts 作成
- [x] instrumentation.ts 作成
- [x] instrumentation-client.ts 作成
- [x] next.config.ts を withSentryConfig でラップ
- [x] src/app/error.tsx 作成（ルートエラーバウンダリ）
- [x] src/app/global-error.tsx 作成（グローバルエラーバウンダリ）
- [x] .env.local.example に Sentry 環境変数追加
- [ ] Vercel に Sentry 環境変数設定
- [x] 動作確認（テストエンドポイント作成）

## 依存関係

```
Phase 1 → Phase 2 → Phase 3 → Phase 4 → Phase 5 → Phase 6 → Phase 7 → Phase 8
```

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
