/**
 * cron/handlers/yutai.ts のユニットテスト
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';

const {
  mockFetchAllYutaiBenefits,
  mockFetchMarginInventoryCsv,
  mockCreateAdminClient,
  mockSendJobFailureEmail,
} = vi.hoisted(() => ({
  mockFetchAllYutaiBenefits: vi.fn(),
  mockFetchMarginInventoryCsv: vi.fn(),
  mockCreateAdminClient: vi.fn(),
  mockSendJobFailureEmail: vi.fn(),
}));

vi.mock('@/lib/yutai/kabuyutai-client', () => ({
  fetchAllYutaiBenefits: mockFetchAllYutaiBenefits,
}));

vi.mock('@/lib/yutai/kabu-csv-client', () => ({
  fetchMarginInventoryCsv: mockFetchMarginInventoryCsv,
}));

vi.mock('@/lib/supabase/admin', () => ({
  createAdminClient: mockCreateAdminClient,
}));

vi.mock('@/lib/notification/email', () => ({
  sendJobFailureEmail: mockSendJobFailureEmail,
}));

vi.mock('@/lib/utils/logger', () => ({
  createLogger: vi.fn(() => ({
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
    startTimer: vi.fn(() => ({ end: vi.fn(), endWithError: vi.fn() })),
  })),
}));

import { handleCronE } from '@/lib/cron/handlers/yutai';
import { NonRetryableError } from '@/lib/utils/retry';

const sampleBenefits = [
  {
    local_code: '29140',
    company_name: 'JT',
    min_shares: 100,
    benefit_content: '自社製品詰め合わせ 2,500円相当',
    benefit_value: 2500,
    record_month: 6,
    record_day: 'end',
    category: '食品',
  },
  {
    local_code: '85910',
    company_name: 'オリックス',
    min_shares: 100,
    benefit_content: 'カタログギフト',
    benefit_value: null,
    record_month: 3,
    record_day: 'end',
    category: 'カタログ',
  },
];

const sampleInventory = [
  {
    local_code: '29140',
    broker: 'esmart',
    inventory_date: '2024-06-01',
    inventory_qty: 500,
    is_available: true,
    loan_type: 'general',
    loan_term: 'infinite',
    premium_fee: null,
    source: 'kabu_csv',
  },
];

describe('handleCronE', () => {
  let mockSupabase: any;

  beforeEach(() => {
    mockFetchAllYutaiBenefits.mockReset();
    mockFetchMarginInventoryCsv.mockReset();
    mockCreateAdminClient.mockReset();
    mockSendJobFailureEmail.mockReset();

    mockSupabase = {
      from: vi.fn().mockReturnValue({
        upsert: vi.fn().mockResolvedValue({ error: null }),
      }),
    };
    mockCreateAdminClient.mockReturnValue(mockSupabase);
  });

  it('source=all → 優待情報と在庫の両方を取得・UPSERT', async () => {
    mockFetchAllYutaiBenefits.mockResolvedValue(sampleBenefits);
    mockFetchMarginInventoryCsv.mockResolvedValue(sampleInventory);

    const result = await handleCronE('all', 'test-run-1');

    expect(result.success).toBe(true);
    expect(result.benefitsUpserted).toBe(2);
    expect(result.inventoryUpserted).toBe(1);
    expect(result.errors).toHaveLength(0);
    expect(mockFetchAllYutaiBenefits).toHaveBeenCalled();
    expect(mockFetchMarginInventoryCsv).toHaveBeenCalled();
  });

  it('source=kabuyutai → 優待情報のみ取得', async () => {
    mockFetchAllYutaiBenefits.mockResolvedValue(sampleBenefits);

    const result = await handleCronE('kabuyutai', 'test-run-2');

    expect(result.success).toBe(true);
    expect(result.benefitsUpserted).toBe(2);
    expect(result.inventoryUpserted).toBe(0);
    expect(mockFetchAllYutaiBenefits).toHaveBeenCalled();
    expect(mockFetchMarginInventoryCsv).not.toHaveBeenCalled();
  });

  it('source=kabu_csv → 在庫情報のみ取得', async () => {
    mockFetchMarginInventoryCsv.mockResolvedValue(sampleInventory);

    const result = await handleCronE('kabu_csv', 'test-run-3');

    expect(result.success).toBe(true);
    expect(result.benefitsUpserted).toBe(0);
    expect(result.inventoryUpserted).toBe(1);
    expect(mockFetchAllYutaiBenefits).not.toHaveBeenCalled();
    expect(mockFetchMarginInventoryCsv).toHaveBeenCalled();
  });

  it('空データ → UPSERTスキップ', async () => {
    mockFetchAllYutaiBenefits.mockResolvedValue([]);
    mockFetchMarginInventoryCsv.mockResolvedValue([]);

    const result = await handleCronE('all', 'test-run-4');

    expect(result.success).toBe(true);
    expect(result.benefitsUpserted).toBe(0);
    expect(result.inventoryUpserted).toBe(0);
    // from() should not be called when data is empty
    expect(mockSupabase.from).not.toHaveBeenCalled();
  });

  it('UPSERTエラー → 失敗', async () => {
    mockFetchAllYutaiBenefits.mockResolvedValue(sampleBenefits);

    mockSupabase.from.mockReturnValue({
      upsert: vi.fn().mockResolvedValue({ error: { message: 'duplicate key' } }),
    });

    const result = await handleCronE('kabuyutai', 'test-run-5');

    // エラーがあれば success=false
    expect(result.success).toBe(false);
    expect(result.errors.length).toBeGreaterThan(0);
  });

  it('fetch例外 → 失敗レスポンス + 通知メール', async () => {
    mockFetchAllYutaiBenefits.mockRejectedValue(new Error('Network timeout'));
    mockSendJobFailureEmail.mockResolvedValue(undefined);

    const result = await handleCronE('kabuyutai', 'test-run-6');

    expect(result.success).toBe(false);
    expect(result.errors).toContain('Network timeout');
    expect(mockSendJobFailureEmail).toHaveBeenCalledWith(
      expect.objectContaining({
        jobName: 'cron-e-yutai',
        error: 'Network timeout',
        runId: 'test-run-6',
      }),
    );
  });

  it('通知メール送信失敗でもクラッシュしない', async () => {
    mockFetchAllYutaiBenefits.mockRejectedValue(new Error('fetch error'));
    mockSendJobFailureEmail.mockRejectedValue(new Error('email error'));

    const result = await handleCronE('kabuyutai', 'test-run-7');

    expect(result.success).toBe(false);
    expect(result.errors).toContain('fetch error');
  });

  it('CSV 404 (非営業日) → 警告のみでジョブ成功', async () => {
    mockFetchAllYutaiBenefits.mockResolvedValue(sampleBenefits);
    mockFetchMarginInventoryCsv.mockRejectedValue(
      new NonRetryableError('HTTP 404: Not Found', 404),
    );

    const result = await handleCronE('all', 'test-run-404');

    expect(result.success).toBe(true);
    expect(result.benefitsUpserted).toBe(2);
    expect(result.inventoryUpserted).toBe(0);
    expect(result.errors).toHaveLength(0);
  });

  it('CSV 404 (source=kabu_csv のみ) → 成功', async () => {
    mockFetchMarginInventoryCsv.mockRejectedValue(
      new NonRetryableError('HTTP 404: Not Found', 404),
    );

    const result = await handleCronE('kabu_csv', 'test-run-404-only');

    expect(result.success).toBe(true);
    expect(result.inventoryUpserted).toBe(0);
    expect(result.errors).toHaveLength(0);
  });

  it('CSV 500エラー → 従来通り失敗', async () => {
    mockFetchAllYutaiBenefits.mockResolvedValue(sampleBenefits);
    mockFetchMarginInventoryCsv.mockRejectedValue(
      new NonRetryableError('HTTP 500: Internal Server Error', 500),
    );
    mockSendJobFailureEmail.mockResolvedValue(undefined);

    const result = await handleCronE('all', 'test-run-500');

    expect(result.success).toBe(false);
    expect(result.errors).toContain('HTTP 500: Internal Server Error');
  });

  it('sourceフィールドが結果に含まれる', async () => {
    mockFetchAllYutaiBenefits.mockResolvedValue([]);

    const result = await handleCronE('kabuyutai', 'test-run-8');
    expect(result.source).toBe('kabuyutai');
  });
});
