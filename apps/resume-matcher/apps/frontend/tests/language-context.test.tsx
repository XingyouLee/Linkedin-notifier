import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';

import { defaultLocale } from '@/i18n/config';
import { LanguageProvider, useLanguage } from '@/lib/context/language-context';

type MockStatus = {
  workspace_id?: string | null;
} | null;

const mockStatusCache = {
  status: null as MockStatus,
  isLoading: true,
};

let storage: Record<string, string> = {};

vi.mock('@/lib/context/status-cache', () => ({
  useStatusCache: () => mockStatusCache,
}));

function LanguageConsumer() {
  const { contentLanguage, uiLanguage, isLoading } = useLanguage();

  return (
    <div>
      <span data-testid="content-language">{contentLanguage}</span>
      <span data-testid="ui-language">{uiLanguage}</span>
      <span data-testid="loading-state">{isLoading ? 'loading' : 'ready'}</span>
    </div>
  );
}

describe('LanguageProvider', () => {
  beforeEach(() => {
    storage = {};
    vi.stubGlobal('localStorage', {
      getItem: (key: string) => (key in storage ? storage[key] : null),
      setItem: (key: string, value: string) => {
        storage[key] = String(value);
      },
      removeItem: (key: string) => {
        delete storage[key];
      },
      clear: () => {
        storage = {};
      },
    });
    mockStatusCache.status = null;
    mockStatusCache.isLoading = true;
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('does not inherit unscoped language keys when a workspace session resolves later', async () => {
    localStorage.setItem('resume_matcher_ui_language', 'ja');
    localStorage.setItem('resume_matcher_content_language', 'ja');

    const { rerender } = render(
      <LanguageProvider>
        <LanguageConsumer />
      </LanguageProvider>
    );

    expect(screen.getByTestId('loading-state')).toHaveTextContent('loading');

    mockStatusCache.status = { workspace_id: 'workspace-b' };
    mockStatusCache.isLoading = false;

    rerender(
      <LanguageProvider>
        <LanguageConsumer />
      </LanguageProvider>
    );

    await waitFor(() => {
      expect(screen.getByTestId('loading-state')).toHaveTextContent('ready');
      expect(screen.getByTestId('ui-language')).toHaveTextContent(defaultLocale);
      expect(screen.getByTestId('content-language')).toHaveTextContent(defaultLocale);
    });
  });

  it('loads workspace-scoped language keys after status resolves', async () => {
    localStorage.setItem('resume_matcher_ui_language', 'ja');
    localStorage.setItem('resume_matcher_content_language', 'ja');
    localStorage.setItem('resume_matcher_ui_language:workspace-b', 'es');
    localStorage.setItem('resume_matcher_content_language:workspace-b', 'zh');

    const { rerender } = render(
      <LanguageProvider>
        <LanguageConsumer />
      </LanguageProvider>
    );

    mockStatusCache.status = { workspace_id: 'workspace-b' };
    mockStatusCache.isLoading = false;

    rerender(
      <LanguageProvider>
        <LanguageConsumer />
      </LanguageProvider>
    );

    await waitFor(() => {
      expect(screen.getByTestId('loading-state')).toHaveTextContent('ready');
      expect(screen.getByTestId('ui-language')).toHaveTextContent('es');
      expect(screen.getByTestId('content-language')).toHaveTextContent('zh');
    });
  });
});
