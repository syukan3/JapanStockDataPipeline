#!/usr/bin/env tsx
/**
 * 決算発表予定 Seed スクリプト
 *
 * @description J-Quants APIから決算発表予定を取得してDBに保存
 *
 * NOTE: このAPIは「翌営業日」の決算発表予定のみを返す。
 * 過去データの一括取得はできないため、現時点のデータのみ取得。
 *
 * @example
 * ```
 * npm run seed:earnings
 * ```
 */

import { loadEnv, createProgress, logResult, startTimer, type SeedResult } from './_shared';

async function main(): Promise<SeedResult> {
  loadEnv();

  console.log('Starting Earnings Calendar Seed');
  console.log('  Note: This API only returns next business day earnings schedule');

  const timer = startTimer();

  // 動的インポート（環境変数ロード後）。決算カレンダーの
  // writerはCron Bルートに一元化し、lock / coverage fenceを迂回しない。
  const { POST } = await import('../../src/app/api/cron/jquants/b/route');

  // 決算発表予定は1リクエストで取得可能（翌営業日分のみ）
  const progress = createProgress(1, 'earnings');

  try {
    const cronSecret = process.env.CRON_SECRET;
    if (!cronSecret) {
      throw new Error('CRON_SECRET is required for earnings seed');
    }

    const response = await POST(
      new Request('http://localhost/api/cron/jquants/b', {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${cronSecret}`,
          'Content-Type': 'application/json',
        },
      })
    );
    const result = (await response.json()) as {
      success?: boolean;
      announcementDate?: string | null;
      fetched?: number;
      inserted?: number;
      error?: string;
      detail?: string;
    };

    if (!response.ok || result.success !== true) {
      if (response.ok && result.error === 'Job already executed') {
        console.log('  Cron B already completed for the target date');
        progress.done();
        return {
          name: 'Earnings Calendar',
          fetched: 0,
          inserted: 0,
          errors: [],
          durationMs: timer(),
        };
      }
      throw new Error(result.detail ?? result.error ?? `Cron B failed (${response.status})`);
    }

    progress.done();

    if (result.announcementDate) {
      console.log(`  Announcement date: ${result.announcementDate}`);
    }

    const seedResult: SeedResult = {
      name: 'Earnings Calendar',
      fetched: result.fetched ?? 0,
      inserted: result.inserted ?? 0,
      errors: [],
      durationMs: timer(),
    };

    logResult(seedResult);
    return seedResult;
  } catch (error) {
    progress.done();
    console.error('Failed to sync earnings calendar:', error);
    throw error;
  }
}

// 直接実行時のみmain()を呼ぶ（import時は実行しない）
const isDirectRun = process.argv[1]?.endsWith('earnings.ts') || process.argv[1]?.endsWith('earnings');
if (isDirectRun) {
  main()
    .then((result) => {
      if (result.errors.length > 0) {
        process.exit(1);
      }
    })
    .catch(() => {
      process.exit(1);
    });
}

export { main as seedEarnings };
