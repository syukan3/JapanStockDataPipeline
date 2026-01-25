/**
 * HTMLユーティリティ
 */

/**
 * HTMLエスケープ
 *
 * @param text エスケープする文字列
 * @returns エスケープ済み文字列
 */
export function escapeHtml(text: string): string {
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}
