'use client';

import { captureException } from '@sentry/nextjs';
import { useEffect } from 'react';

export default function GlobalError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    captureException(error);
  }, [error]);

  return (
    <html lang="ja">
      <body>
        <main style={{ padding: '2rem', textAlign: 'center' }}>
          <h1>予期しないエラーが発生しました</h1>
          <p style={{ color: '#666', marginTop: '1rem' }}>
            アプリケーションで重大なエラーが発生しました。
          </p>
          <button
            onClick={() => reset()}
            style={{
              marginTop: '1.5rem',
              padding: '0.75rem 1.5rem',
              backgroundColor: '#0070f3',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: 'pointer',
              fontSize: '1rem',
            }}
          >
            再試行
          </button>
        </main>
      </body>
    </html>
  );
}
