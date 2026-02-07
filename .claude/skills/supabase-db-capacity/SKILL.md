---
name: supabase-db-capacity
description: SupabaseのDB容量確認。「DB容量」「ディスク使用量」「Supabase容量チェック」「データベースサイズ」で使用する。
---

# Supabase DB 容量確認

## 接続方法

Supabase Management API を使用してSQLクエリを実行する。

```bash
SUPABASE_ACCESS_TOKEN=$(grep SUPABASE_ACCESS_TOKEN /Users/m-sakae/Source/JapanStock/JapanStockDataPipeline/.env.local | cut -d'=' -f2 | tr -d '"' | tr -d "'" | xargs)
```

### 重要ポイント

- **API**: Supabase Management API の `/v1/projects/{ref}/database/query` エンドポイントを使用
- **プロジェクトRef**: `qceexdbqcpvmyupcnpco`
- **認証**: `SUPABASE_ACCESS_TOKEN`（Management API用のPersonal Access Token）
- **スキーマ**: テーブルは `public` ではなく `jquants_core`, `scouter`, `jquants_ingest` にある
- **Free Plan上限**: 500 MB

### 基本クエリテンプレート

```bash
curl -s -X POST "https://api.supabase.com/v1/projects/qceexdbqcpvmyupcnpco/database/query" \
  -H "Authorization: Bearer ${SUPABASE_ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  --data-raw '{"query": "{SQL}"}'
```

---

## 確認手順

### 1. データベース全体のサイズ

```bash
SUPABASE_ACCESS_TOKEN=$(grep SUPABASE_ACCESS_TOKEN /Users/m-sakae/Source/JapanStock/JapanStockDataPipeline/.env.local | cut -d'=' -f2 | tr -d '"' | tr -d "'" | xargs)

curl -s -X POST "https://api.supabase.com/v1/projects/qceexdbqcpvmyupcnpco/database/query" \
  -H "Authorization: Bearer ${SUPABASE_ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  --data-raw '{"query": "SELECT pg_size_pretty(pg_database_size(current_database())) AS total_size, pg_database_size(current_database()) AS total_bytes"}'
```

### 2. スキーマ別テーブル数

```bash
curl -s -X POST "https://api.supabase.com/v1/projects/qceexdbqcpvmyupcnpco/database/query" \
  -H "Authorization: Bearer ${SUPABASE_ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  --data-raw '{"query": "SELECT schemaname, count(*) AS table_count FROM pg_tables WHERE schemaname IN ('"'"'jquants_core'"'"', '"'"'jquants_ingest'"'"', '"'"'scouter'"'"') GROUP BY schemaname ORDER BY table_count DESC"}'
```

### 3. テーブル別サイズ（アプリスキーマ、容量降順）

```bash
curl -s -X POST "https://api.supabase.com/v1/projects/qceexdbqcpvmyupcnpco/database/query" \
  -H "Authorization: Bearer ${SUPABASE_ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  --data-raw '{"query": "SELECT t.schemaname, t.tablename, pg_size_pretty(pg_total_relation_size(quote_ident(t.schemaname) || '"'"'.'"'"' || quote_ident(t.tablename))) AS total_size, pg_total_relation_size(quote_ident(t.schemaname) || '"'"'.'"'"' || quote_ident(t.tablename)) AS total_bytes, pg_size_pretty(pg_relation_size(quote_ident(t.schemaname) || '"'"'.'"'"' || quote_ident(t.tablename))) AS table_size, pg_size_pretty(pg_indexes_size(quote_ident(t.schemaname) || '"'"'.'"'"' || quote_ident(t.tablename))) AS index_size FROM pg_tables t WHERE t.schemaname IN ('"'"'jquants_core'"'"', '"'"'jquants_ingest'"'"', '"'"'scouter'"'"') ORDER BY pg_total_relation_size(quote_ident(t.schemaname) || '"'"'.'"'"' || quote_ident(t.tablename)) DESC"}'
```

### 4. アプリテーブル合計サイズ

```bash
curl -s -X POST "https://api.supabase.com/v1/projects/qceexdbqcpvmyupcnpco/database/query" \
  -H "Authorization: Bearer ${SUPABASE_ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  --data-raw '{"query": "SELECT pg_size_pretty(sum(pg_total_relation_size(quote_ident(t.schemaname) || '"'"'.'"'"' || quote_ident(t.tablename)))) AS app_tables_total FROM pg_tables t WHERE t.schemaname IN ('"'"'jquants_core'"'"', '"'"'jquants_ingest'"'"', '"'"'scouter'"'"')"}'
```

### 5. 行数の確認（概算）

```bash
curl -s -X POST "https://api.supabase.com/v1/projects/qceexdbqcpvmyupcnpco/database/query" \
  -H "Authorization: Bearer ${SUPABASE_ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  --data-raw '{"query": "SELECT schemaname, relname AS tablename, n_live_tup AS estimated_rows FROM pg_stat_user_tables WHERE schemaname IN ('"'"'jquants_core'"'"', '"'"'jquants_ingest'"'"', '"'"'scouter'"'"') ORDER BY n_live_tup DESC"}'
```

---

## 結果の読み方

- **Free Planの上限は500 MB**。使用率が80%（400 MB）を超えたら要注意
- **`equity_bar_daily`** が最大テーブル（全体の80%以上）。日次で全上場銘柄の株価が蓄積される
- **total_size** = table_size（データ本体）+ index_size（インデックス）+ TOAST等
- **pg_database_size** にはシステムテーブル（`auth`, `storage`, `pg_catalog`等）も含まれる
- 行数は `pg_stat_user_tables.n_live_tup` の概算値。正確な値が必要なら `SELECT count(*) FROM ...` を実行する

## レポート出力形式

結果は以下の形式でまとめて報告すること：

```
## Supabase DB 容量レポート

| 項目 | サイズ |
|---|---|
| データベース全体 | XXX MB |
| アプリテーブル合計 | XXX MB |
| Free Plan 上限 | 500 MB |
| 使用率 | XX% |

### テーブル別（容量降順）

| スキーマ | テーブル | 合計サイズ | テーブル | インデックス |
|---|---|---|---|---|
| ... | ... | ... | ... | ... |
```
