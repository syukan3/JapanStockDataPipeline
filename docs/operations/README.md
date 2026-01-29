# 運用ガイド

J-Quants API V2 データ同期基盤の運用ドキュメント。

## 概要

本システムは J-Quants API V2 から日本株データを取得し、Supabase に保存するデータ同期基盤です。

### アーキテクチャ

```
GitHub Actions (スケジューラー)
       │
       ▼
Vercel (Next.js Route Handler)
       │
       ├─→ J-Quants API V2 (データ取得)
       │
       └─→ Supabase (データ保存)
              │
              └─→ Resend (障害通知)
```

### 同期ジョブ一覧

| ジョブ | 説明 | 実行時刻 (JST) |
|--------|------|----------------|
| Cron A | 日次確定データ | 18:40 |
| Cron B | 決算発表予定 | 19:20 |
| Cron C | 投資部門別 + 整合性チェック | 12:10 |

### データセット

| データセット | テーブル | 更新頻度 |
|--------------|----------|----------|
| 取引カレンダー | `trading_calendar` | 日次 |
| 銘柄マスタ | `equity_master_snapshot` | 日次 |
| 株価日足 | `equity_bar_daily` | 日次 |
| TOPIX | `topix_bar_daily` | 日次 |
| 財務サマリー | `financial_disclosure` | 日次 |
| 決算発表予定 | `earnings_calendar` | 日次 |
| 投資部門別 | `investor_type_trading` | 週次 |

## ドキュメント一覧

- [環境変数一覧](./env-variables.md) - 必要な環境変数の設定方法
- [Cron時刻対応表](./cron-schedule.md) - スケジュール詳細
- [再実行手順](./manual-resync.md) - 手動での再実行方法
- [障害時追跡方法](./troubleshooting.md) - トラブルシューティング

## スキーマ構成

```
├── jquants_core     # ビジネスデータ
│   ├── trading_calendar
│   ├── equity_master_snapshot
│   ├── equity_bar_daily
│   ├── topix_bar_daily
│   ├── financial_disclosure
│   ├── earnings_calendar
│   └── investor_type_trading
│
└── jquants_ingest   # ジョブ管理・監視
    ├── job_locks
    ├── job_runs
    ├── job_run_items
    ├── job_heartbeat
    └── (監視ビュー)
```

## 監視ビュー

Supabase SQL Editor から以下のビューでシステム状態を確認できます。

```sql
-- ジョブの健全性確認
SELECT * FROM jquants_ingest.v_job_status;

-- 直近24時間の失敗ジョブ
SELECT * FROM jquants_ingest.v_failed_jobs_24h;

-- データ鮮度確認
SELECT * FROM jquants_ingest.v_data_freshness;

-- 直近7日間の実行サマリー
SELECT * FROM jquants_ingest.v_job_runs_summary_7d;

-- アクティブなロック確認
SELECT * FROM jquants_ingest.v_active_locks;
```

## キャッチアップ機能

システム障害や休日などで取り込み漏れが発生した場合、自動的にキャッチアップを実行します。

- **最大キャッチアップ日数**: 環境変数 `SYNC_MAX_CATCHUP_DAYS` で設定（デフォルト: 5日）
- **遡り日数**: 環境変数 `SYNC_LOOKBACK_DAYS` で設定（デフォルト: 30日）

## レート制限

J-Quants API V2 のプラン別レート制限に対応しています。

| プラン | リクエスト/分 |
|--------|--------------|
| Free | 5 |
| Light | **60**（本システム採用） |
| Standard | 120 |
| Premium | 500 |

本システムはトークンバケット方式でレート制限を遵守します。

## Vercel 制限

Vercel Hobby プランの制限に対応するため、以下の設計を採用しています。

- **実行時間**: 最大10秒
- **対策**: 1回の API 呼び出しで1データセットのみ処理
- **GitHub Actions**: データセットごとに順次 API 呼び出し
