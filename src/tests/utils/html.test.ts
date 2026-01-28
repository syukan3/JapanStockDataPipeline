import { describe, it, expect } from 'vitest';
import { escapeHtml } from '@/lib/utils/html';

describe('html.ts', () => {
  describe('escapeHtml', () => {
    it('& を &amp; にエスケープする', () => {
      expect(escapeHtml('foo & bar')).toBe('foo &amp; bar');
    });

    it('< を &lt; にエスケープする', () => {
      expect(escapeHtml('foo < bar')).toBe('foo &lt; bar');
    });

    it('> を &gt; にエスケープする', () => {
      expect(escapeHtml('foo > bar')).toBe('foo &gt; bar');
    });

    it('" を &quot; にエスケープする', () => {
      expect(escapeHtml('foo "bar"')).toBe('foo &quot;bar&quot;');
    });

    it("' を &#039; にエスケープする", () => {
      expect(escapeHtml("foo 'bar'")).toBe('foo &#039;bar&#039;');
    });

    it('複合: XSSスクリプトをエスケープする', () => {
      const input = '<script>alert("XSS")</script>';
      const expected = '&lt;script&gt;alert(&quot;XSS&quot;)&lt;/script&gt;';
      expect(escapeHtml(input)).toBe(expected);
    });

    it('エスケープ不要な通常文字列はそのまま返す', () => {
      expect(escapeHtml('Hello World 123')).toBe('Hello World 123');
    });

    it('空文字はそのまま返す', () => {
      expect(escapeHtml('')).toBe('');
    });

    it('二重エスケープ: &lt; → &amp;lt;', () => {
      expect(escapeHtml('&lt;')).toBe('&amp;lt;');
    });
  });
});
