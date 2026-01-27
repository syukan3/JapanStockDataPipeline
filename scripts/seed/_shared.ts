/**
 * Seed スクリプト共通ユーティリティ
 *
 * @description 初期データ投入スクリプト用の共有モジュール
 */

import { config } from 'dotenv';
import { resolve } from 'path';

/**
 * 環境変数ロード済みフラグ（重複ロード防止）
 */
let envLoaded = false;

/**
 * 環境変数エラー
 */
export class EnvironmentError extends Error {
  constructor(missing: string[]) {
    super(
      `Missing required environment variables: ${missing.join(', ')}\n` +
        `Please ensure .env.local exists with these variables.`
    );
    this.name = 'EnvironmentError';
  }
}

/**
 * 環境変数を読み込む
 *
 * @throws {EnvironmentError} 必須環境変数が不足している場合
 */
export function loadEnv(): void {
  // 既にロード済みならスキップ
  if (envLoaded) return;

  // プロジェクトルートの .env.local を読み込む
  const envPath = resolve(process.cwd(), '.env.local');
  config({ path: envPath });

  // 必須環境変数の検証
  const required = [
    'NEXT_PUBLIC_SUPABASE_URL',
    'SUPABASE_SERVICE_ROLE_KEY',
    'JQUANTS_API_KEY',
  ];

  const missing = required.filter((key) => !process.env[key]);
  if (missing.length > 0) {
    throw new EnvironmentError(missing);
  }

  // 検証成功後にフラグを設定
  envLoaded = true;
}

/**
 * CLIオプションを解析
 */
export interface SeedOptions {
  /** 取得開始日 (YYYY-MM-DD) */
  from: string;
  /** 取得終了日 (YYYY-MM-DD) */
  to: string;
  /** 日数（--daysオプション） */
  days?: number;
}

/**
 * 日付フォーマット検証（YYYY-MM-DD）
 *
 * NOTE: src/lib/utils/date.ts の isValidDateFormat と同等だが、
 * 環境変数ロード前に使用するため独立して定義
 */
function isValidDateFormat(dateStr: string): boolean {
  const regex = /^\d{4}-\d{2}-\d{2}$/;
  if (!regex.test(dateStr)) {
    return false;
  }

  const [year, month, day] = dateStr.split('-').map(Number);
  const date = new Date(year, month - 1, day);

  return (
    date.getFullYear() === year &&
    date.getMonth() === month - 1 &&
    date.getDate() === day
  );
}

/**
 * JST日付をフォーマット（YYYY-MM-DD）
 *
 * NOTE: src/lib/utils/date.ts の getJSTDate と同等だが、
 * 環境変数ロード前に使用するため独立して定義
 */
function formatJSTDate(date: Date): string {
  const formatter = new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Asia/Tokyo',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  });
  return formatter.format(date);
}

/**
 * デフォルト値: 1年前から今日まで
 */
function getDefaultDateRange(): { from: string; to: string } {
  const now = new Date();
  const to = formatJSTDate(now);

  const from = new Date(now);
  from.setFullYear(from.getFullYear() - 1);

  return { from: formatJSTDate(from), to };
}

/**
 * CLIオプションを解析
 *
 * @example
 * ```
 * npm run seed:bars -- --from 2024-01-01 --to 2024-12-31
 * npm run seed:bars -- --days 365
 * ```
 */
export function parseArgs(): SeedOptions {
  const args = process.argv.slice(2);
  const defaults = getDefaultDateRange();
  const options: SeedOptions = { ...defaults };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    const nextArg = args[i + 1];

    if (arg === '--from' && nextArg) {
      if (!isValidDateFormat(nextArg)) {
        console.error(`Invalid --from date format: ${nextArg}. Expected YYYY-MM-DD`);
        process.exit(1);
      }
      options.from = nextArg;
      i++;
    } else if (arg === '--to' && nextArg) {
      if (!isValidDateFormat(nextArg)) {
        console.error(`Invalid --to date format: ${nextArg}. Expected YYYY-MM-DD`);
        process.exit(1);
      }
      options.to = nextArg;
      i++;
    } else if (arg === '--days' && nextArg) {
      const days = parseInt(nextArg, 10);
      if (isNaN(days) || days <= 0) {
        console.error(`Invalid --days value: ${nextArg}. Expected positive integer`);
        process.exit(1);
      }
      options.days = days;
      // --days指定時は今日からN日前
      const now = new Date();
      options.to = formatJSTDate(now);
      const from = new Date(now);
      from.setDate(from.getDate() - days);
      options.from = formatJSTDate(from);
      i++;
    }
  }

  // from <= to の検証
  if (options.from > options.to) {
    console.error(`--from (${options.from}) must be before or equal to --to (${options.to})`);
    process.exit(1);
  }

  return options;
}

/**
 * 進捗表示ヘルパー
 */
export interface Progress {
  /** 現在のカウントを増やす */
  increment: (label?: string) => void;
  /** 進捗を手動で設定 */
  set: (current: number, label?: string) => void;
  /** 完了 */
  done: () => void;
}

/**
 * 進捗表示を作成
 *
 * @param total 合計件数
 * @param name データセット名
 */
export function createProgress(total: number, name: string): Progress {
  let current = 0;
  const startTime = Date.now();

  const render = (label?: string) => {
    const elapsed = Math.round((Date.now() - startTime) / 1000);

    // total=0の場合は特別扱い
    if (total <= 0) {
      const barWidth = 30;
      const bar = '─'.repeat(barWidth);
      const status = label ? ` (${label})` : '';
      process.stdout.write(`\r[${name}] ${bar} 0/0 (N/A) | Elapsed: ${elapsed}s${status}   `);
      return;
    }

    const percent = Math.round((current / total) * 100);
    const eta = current > 0 ? Math.round((elapsed / current) * (total - current)) : 0;

    // プログレスバー（幅30文字）
    const barWidth = 30;
    const filled = Math.round((current / total) * barWidth);
    const bar = '█'.repeat(filled) + '░'.repeat(barWidth - filled);

    const status = label ? ` (${label})` : '';
    process.stdout.write(
      `\r[${name}] ${bar} ${current}/${total} (${percent}%) | ` +
        `Elapsed: ${elapsed}s | ETA: ${eta}s${status}   `
    );
  };

  return {
    increment: (label?: string) => {
      current++;
      render(label);
    },
    set: (value: number, label?: string) => {
      current = value;
      render(label);
    },
    done: () => {
      current = total;
      render();
      console.log(); // 改行
    },
  };
}

/**
 * 結果をログ出力
 */
export interface SeedResult {
  name: string;
  fetched: number;
  inserted: number;
  errors: Error[];
  durationMs: number;
}

export function logResult(result: SeedResult): void {
  const { name, fetched, inserted, errors, durationMs } = result;
  const durationSec = (durationMs / 1000).toFixed(1);

  console.log(`\n--- ${name} ---`);
  console.log(`  Fetched:  ${fetched.toLocaleString()} records`);
  console.log(`  Inserted: ${inserted.toLocaleString()} records`);
  console.log(`  Duration: ${durationSec}s`);

  if (errors.length > 0) {
    console.log(`  Errors:   ${errors.length}`);
    errors.slice(0, 3).forEach((err, i) => {
      console.log(`    [${i + 1}] ${err.message}`);
    });
    if (errors.length > 3) {
      console.log(`    ... and ${errors.length - 3} more`);
    }
  }
}

/**
 * 全結果のサマリーを出力
 */
export function logSummary(results: SeedResult[]): void {
  console.log('\n========================================');
  console.log('SEED SUMMARY');
  console.log('========================================');

  let totalFetched = 0;
  let totalInserted = 0;
  let totalErrors = 0;
  let totalDuration = 0;

  for (const r of results) {
    totalFetched += r.fetched;
    totalInserted += r.inserted;
    totalErrors += r.errors.length;
    totalDuration += r.durationMs;
  }

  console.log(`Total Fetched:  ${totalFetched.toLocaleString()} records`);
  console.log(`Total Inserted: ${totalInserted.toLocaleString()} records`);
  console.log(`Total Errors:   ${totalErrors}`);
  console.log(`Total Duration: ${(totalDuration / 1000).toFixed(1)}s`);
  console.log('========================================\n');

  if (totalErrors > 0) {
    console.log('Datasets with errors:');
    results
      .filter((r) => r.errors.length > 0)
      .forEach((r) => {
        console.log(`  - ${r.name}: ${r.errors.length} errors`);
      });
  }
}

/**
 * 実行時間を計測
 */
export function startTimer(): () => number {
  const start = Date.now();
  return () => Date.now() - start;
}

/**
 * 営業日リストを取得（DBから）
 */
export async function getBusinessDays(from: string, to: string): Promise<string[]> {
  const { getBusinessDaysFromDB } = await import('../../src/lib/jquants/endpoints/trading-calendar');
  const records = await getBusinessDaysFromDB(from, to);
  return records.map((r) => r.calendar_date);
}

/**
 * Seedタスク定義
 */
export interface SeedTask {
  name: string;
  fn: () => Promise<SeedResult>;
}

/**
 * Seedタスクを実行（エラーハンドリング付き）
 */
export async function runSeedTask(
  task: SeedTask,
  index: number,
  total: number
): Promise<SeedResult> {
  console.log(`\n[${index + 1}/${total}] ${task.name}\n`);
  try {
    return await task.fn();
  } catch (error) {
    console.error(`${task.name} seed failed:`, error);
    return {
      name: task.name,
      fetched: 0,
      inserted: 0,
      errors: [error instanceof Error ? error : new Error(String(error))],
      durationMs: 0,
    };
  }
}
