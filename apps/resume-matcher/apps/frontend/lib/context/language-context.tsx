'use client';

import React, { createContext, useContext, useState, useEffect, useCallback } from 'react';
import { type SupportedLanguage } from '@/lib/api/config';
import { locales, defaultLocale, localeNames, type Locale } from '@/i18n/config';
import { useStatusCache } from '@/lib/context/status-cache';

const CONTENT_STORAGE_KEY = 'resume_matcher_content_language';
const UI_STORAGE_KEY = 'resume_matcher_ui_language';

interface LanguageContextValue {
  contentLanguage: SupportedLanguage;
  uiLanguage: Locale;
  isLoading: boolean;
  setContentLanguage: (lang: SupportedLanguage) => Promise<void>;
  setUiLanguage: (lang: Locale) => void;
  languageNames: typeof localeNames;
  supportedLanguages: readonly Locale[];
}

const LanguageContext = createContext<LanguageContextValue | undefined>(undefined);

export function LanguageProvider({ children }: { children: React.ReactNode }) {
  const { status, isLoading: statusLoading } = useStatusCache();
  const [contentLanguage, setContentLanguageState] = useState<SupportedLanguage>(defaultLocale);
  const [uiLanguage, setUiLanguageState] = useState<Locale>(defaultLocale);
  const [isLoading, setIsLoading] = useState(true);

  const workspaceSuffix = status?.workspace_id ? `:${status.workspace_id}` : '';
  const contentStorageKey = `${CONTENT_STORAGE_KEY}${workspaceSuffix}`;
  const uiStorageKey = `${UI_STORAGE_KEY}${workspaceSuffix}`;

  // Load language preferences from localStorage.
  useEffect(() => {
    if (statusLoading) {
      return;
    }

    const cachedUiLang = localStorage.getItem(uiStorageKey);
    if (cachedUiLang && locales.includes(cachedUiLang as Locale)) {
      setUiLanguageState(cachedUiLang as Locale);
    } else {
      setUiLanguageState(defaultLocale);
    }

    const cachedContentLang = localStorage.getItem(contentStorageKey);
    if (cachedContentLang && locales.includes(cachedContentLang as Locale)) {
      setContentLanguageState(cachedContentLang as SupportedLanguage);
    } else {
      setContentLanguageState(defaultLocale);
    }

    setIsLoading(false);
  }, [contentStorageKey, statusLoading, uiStorageKey]);

  const setContentLanguage = useCallback(
    async (lang: SupportedLanguage) => {
      if (!locales.includes(lang as Locale)) {
        console.error(`Unsupported language: ${lang}`);
        return;
      }

      const previousLang = contentLanguage;
      try {
        // Optimistically update UI
        setContentLanguageState(lang);
        localStorage.setItem(contentStorageKey, lang);
      } catch (error) {
        console.error('Failed to update content language:', error);
        // Revert on error
        setContentLanguageState(previousLang);
        localStorage.setItem(contentStorageKey, previousLang);
      }
    },
    [contentLanguage, contentStorageKey]
  );

  const setUiLanguage = useCallback(
    (lang: Locale) => {
      if (!locales.includes(lang)) {
        console.error(`Unsupported UI language: ${lang}`);
        return;
      }
      setUiLanguageState(lang);
      localStorage.setItem(uiStorageKey, lang);
    },
    [uiStorageKey]
  );

  return (
    <LanguageContext.Provider
      value={{
        contentLanguage,
        uiLanguage,
        isLoading,
        setContentLanguage,
        setUiLanguage,
        languageNames: localeNames,
        supportedLanguages: locales,
      }}
    >
      {children}
    </LanguageContext.Provider>
  );
}

export function useLanguage() {
  const context = useContext(LanguageContext);
  if (context === undefined) {
    throw new Error('useLanguage must be used within a LanguageProvider');
  }
  return context;
}
