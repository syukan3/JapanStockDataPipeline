# JapanStockDataPipeline

J-Quants API V2 から日本株マーケットデータを自動収集し、Supabase (PostgreSQL) へ同期するプロダクショングレードのデータパイプライン。欠損データの自動検知・補完、SCD Type 2 による変更履歴管理、整合性チェック、ジョブロック・ハートビートによる耐障害設計を備え、Vercel Hobby プランの制約下で安定稼働する。

## アーキテクチャ概要

```
GitHub Actions (Cron)
  │  curl / direct execution
  ▼
Vercel (Next.js API Routes) / GitHub Actions Runner
  ├─ Cron A: 日次確定データ (5データセット × 逐次)
  ├─ Cron B: 決算発表予定
  ├─ Cron C: 投資部門別売買 + 整合性チェック
  └─ Cron D: マクロ経済指標 (FRED/e-Stat)
  │
  ├─ J-Quants API V2 ──→ トークンバケット制御 (60req/min)
  ├─ FRED API ─────────→ 米国経済指標 (14系列)
  ├─ e-Stat API ───────→ 日本経済指標 (2系列)
  │                       指数バックオフリトライ (500ms〜32s + jitter)
  ▼
Supabase (PostgreSQL)
  ├─ jquants_core:   9テーブル (ビジネスデータ, RLS保護)
  ├─ jquants_ingest: 4テーブル + 5ビュー (ジョブ管理・監視)
  └─ RLS + 最小権限: anon排除, authenticated=SELECT, service_role=ALL
```

## データセット

### ビジネスデータ (jquants_core)

| テーブル | 説明 | 更新頻度 | 特性 |
|---------|------|---------|------|
| `trading_calendar` | 取引カレンダー (±370日) | 日次 | 営業日判定の基盤 |
| `equity_master` | 上場銘柄マスタ | 日次 | **SCD Type 2**: 変更履歴管理, `is_current` 部分ユニーク制約 |
| `equity_bar_daily` | 株価日足 (OHLCV + 調整済み) | 日次 | 銘柄×日付×セッション複合キー |
| `topix_bar_daily` | TOPIX 指数日足 | 日次 | カバリングインデックス (Index-Only Scan) |
| `financial_disclosure` | 財務サマリー (~50カラム) | 日次 | 空文字→null 自動変換 |
| `earnings_calendar` | 決算発表予定 | 日次 | 翌営業日分を先行取得 |
| `investor_type_trading` | 投資部門別売買動向 | 週次 | `published_date` をPKに含め訂正版を別レコード保存 |
| `macro_indicator_daily` | マクロ経済指標日次 | 日次 | FRED/e-Stat から 16 系列を統一フォーマットで蓄積 |
| `macro_series_metadata` | マクロ系列メタデータ | - | 各系列の更新管理・カテゴリ情報 |

### ジョブ管理 (jquants_ingest)

| テーブル/ビュー | 用途 |
|---------------|------|
| `job_runs` / `job_run_items` | 実行ログ (ジョブ単位 + データセット単位) |
| `job_locks` | テーブルベースロック (二重起動防止) |
| `job_heartbeat` | 死活監視 (stale閾値: 25h) |
| `v_data_freshness` | データセット別の最新日付・概算行数 |
| `v_job_status` / `v_failed_jobs_24h` | ジョブ健全性モニタリング |

## 主要機能

### 自動キャッチアップ (欠損補完)

Cron の失敗や障害で取りこぼしたデータを自動で検知・補完する。

- `job_runs` の成功ログと営業日カレンダーを照合し、未処理日を特定
- 遡り範囲 (デフォルト: 30日) と1回の最大処理日数 (デフォルト: 5営業日) を環境変数で制御
- RPC (`DISTINCT`) で重複行転送を回避し、大量データテーブルでも効率的に欠損検出

### 整合性チェック

Cron C で並列実行される横断的なデータ品質チェック。

| チェック項目 | 閾値 | 動作 |
|------------|------|------|
| カレンダーカバレッジ | ±370日 | 未来の営業日データ不足を検知 |
| 株価データ鮮度 | 3日以内 | 最新日が古すぎる場合に警告 |
| TOPIX 鮮度 | 3日以内 | 同上 |

### SCD Type 2 (銘柄マスタ)

上場銘柄の変更履歴を完全に保持する Slowly Changing Dimension 実装。

- `valid_from` / `valid_to` で有効期間を管理
- `is_current = true` の部分ユニークインデックスで各銘柄1レコードを保証
- 変更検出→旧レコードクローズ→新レコード挿入をバッチ化 (N+1回避)
- 上場廃止の自動検出・クローズ処理
- `equity_master_snapshot` 互換ビューで旧API互換性を維持

### ジョブロック・ハートビート

Vercel のサーバーレス環境で安全な排他制御を実現。

- **テーブルベースロック**: Supabase Pooler (Transaction mode) で Advisory Lock が使えないため、`job_locks` テーブルで実装。TTL 付き自動解放
- **ハートビート**: `job_heartbeat` に最終実行時刻・ステータスを UPSERT。`v_job_status` ビューでリアルタイム監視

### エラーハンドリング・通知

- **リトライ**: 429/5xx に対して指数バックオフ (500ms→32s) + ジッター。401/403 は即座失敗
- **メール通知** (Resend): ジョブ失敗時に自動送信。連続失敗アラート対応
- **エラー監視** (Sentry): フロントエンド例外の自動追跡

### 冪等性設計

全データ同期処理は安全に再実行可能。

- UPSERT (`ON CONFLICT`) で重複レコードを回避
- `job_runs(job_name, target_date)` 複合ユニーク制約
- バッチサイズをテーブル別に最適化 (Supabase 1MB/リクエスト制限考慮)

## セキュリティ

```
jquants_core (ビジネスデータ)
  ├─ anon:          権限なし (完全排除)
  ├─ authenticated: SELECT のみ
  └─ service_role:  ALL (RLS バイパス)

jquants_ingest (管理用)
  ├─ anon:          権限なし
  ├─ authenticated: 権限なし (RLS有効 + ポリシー無し = 全拒否)
  └─ service_role:  ALL (RLS バイパス)
```

- 全テーブルで Row Level Security (RLS) 有効化
- API ルートは `requireCronAuth()` でタイミングセーフ認証
- デフォルト権限設定で新テーブルも自動保護

## Cron スケジュール

| ジョブ | 時刻 (JST) | 内容 | 制約対応 |
|-------|-----------|------|---------|
| **A** | 18:40 | 日次確定データ (カレンダー, 株価, TOPIX, 財務, 銘柄マスタ) | 1回1データセット逐次実行 (Vercel 10秒制限) |
| **B** | 19:20 | 決算発表カレンダー | 翌営業日分を先行取得 |
| **C** | 12:10 | 投資部門別売買 + 整合性チェック | スライディングウィンドウ (60日) + 並列実行 |
| **D** | 07:00 (月〜金) | マクロ経済指標 (FRED/e-Stat) | GitHub Actions 直接実行 (Vercel 経由せず) |

GitHub Actions が Vercel API Routes を curl で順次呼び出し (Cron A/B/C)。Cron D は GitHub Actions Runner で直接実行。レート制限はトークンバケット方式 (最小間隔 1000ms) で制御。

## 技術スタック

| レイヤー | 技術 |
|---------|------|
| フレームワーク | Next.js 16 (Turbopack) / React 19 / TypeScript 5.7 |
| データベース | Supabase (PostgreSQL) |
| データソース | J-Quants API V2 (Light プラン: 60req/min), FRED API, e-Stat API |
| スケジューラ | GitHub Actions (Cron) |
| デプロイ | Vercel (Hobby プラン) |
| 通知 | Resend (メール) |
| エラー監視 | Sentry (@sentry/nextjs) |
| テスト | Vitest (270ケース / 15ファイル, カバレッジ閾値 80%) |

## セットアップ

```bash
npm install
cp .env.local.example .env.local
# .env.local に環境変数を設定
npm run dev
```

環境変数の詳細は [docs/operations/env-variables.md](docs/operations/env-variables.md) を参照。

## スクリプト

| コマンド | 説明 |
|---------|------|
| `npm run dev` | 開発サーバー起動 |
| `npm run build` | プロダクションビルド |
| `npm test` | テスト (watch) |
| `npm run test:run` | テスト (単発) |
| `npm run test:coverage` | カバレッジレポート |
| `npm run typecheck` | 型チェック |
| `npm run lint` | Lint |
| `npm run seed:all` | 初期データ一括投入 |

## ディレクトリ構成

```
src/
├── app/
│   └── api/cron/
│       ├── jquants/          # Cron API Routes (A, A/chunk, B, C)
│       └── macro/            # Cron D API Route
├── lib/
│   ├── jquants/
│   │   ├── client.ts         # API クライアント (トークン管理, レート制限)
│   │   ├── types.ts          # API レスポンス型 + DB レコード型
│   │   └── endpoints/        # データセット別の取得・同期ロジック (7モジュール)
│   ├── fred/
│   │   ├── client.ts         # FRED API クライアント
│   │   ├── types.ts          # FRED 型定義
│   │   └── series-config.ts  # 取得対象系列設定 (14系列)
│   ├── estat/
│   │   ├── client.ts         # e-Stat API クライアント
│   │   ├── types.ts          # e-Stat 型定義
│   │   └── series-config.ts  # 取得対象系列設定 (2系列)
│   ├── supabase/             # Admin / Server クライアント (接続キャッシュ)
│   ├── cron/
│   │   ├── handlers/         # Cron A/B/C/D ハンドラー
│   │   ├── catch-up.ts       # 欠損データ自動検知・補完
│   │   ├── job-run.ts        # 実行ログ管理
│   │   ├── job-lock.ts       # テーブルベースロック
│   │   ├── heartbeat.ts      # 死活監視
│   │   └── business-day.ts   # 営業日判定
│   ├── notification/         # メール通知 (Resend)
│   └── utils/                # バッチ処理, 日付, リトライ, ロガー
├── tests/                    # ユニットテスト (15ファイル, 270ケース)
supabase/migrations/          # DB マイグレーション (24ファイル)
scripts/
├── seed/                     # 初期データ投入 CLI
└── cron/                     # Cron 直接実行スクリプト (cron-d-direct.ts)
docs/                         # 運用・アーキテクチャドキュメント
```

## ドキュメント

- [運用ガイド](docs/operations/README.md)
- [アーキテクチャ図](docs/architecture/architecture-mermaid.md)
- [Cron スケジュール詳細](docs/operations/cron-schedule.md)
- [環境変数一覧](docs/operations/env-variables.md)
- [トラブルシューティング](docs/operations/troubleshooting.md)
- [手動再同期手順](docs/operations/manual-resync.md)
