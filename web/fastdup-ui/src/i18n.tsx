import { createContext, useContext, useEffect, useMemo, useState, type ReactNode } from "react";
import { english } from "./translations";

export type UiLanguage = "de" | "en";
export function translate(language: UiLanguage, message: string, values: Record<string, string | number> = {}) {
  const translated = language === "en" ? english[message] ?? message : message;
  return translated.replace(/\{(\w+)\}/g, (match, key: string) => String(values[key] ?? match));
}
const I18nContext = createContext({
  language: "de" as UiLanguage,
  locale: "de-DE",
  setLanguage: (_language: UiLanguage) => {},
  t: (message: string, values?: Record<string, string | number>) => translate("de", message, values),
});
export function I18nProvider({ children }: { children: ReactNode }) {
  const [language, setLanguage] = useState<UiLanguage>("de");
  useEffect(() => { document.documentElement.lang = language; }, [language]);
  const value = useMemo(() => ({
    language, setLanguage, locale: language === "de" ? "de-DE" : "en-US",
    t: (message: string, values?: Record<string, string | number>) => translate(language, message, values),
  }), [language]);
  return <I18nContext.Provider value={value}>{children}</I18nContext.Provider>;
}
export const useI18n = () => useContext(I18nContext);
