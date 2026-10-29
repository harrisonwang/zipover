use std::sync::{
    OnceLock,
    atomic::{AtomicU8, Ordering},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Language {
    English,
    Chinese,
}

static LANGUAGE: OnceLock<AtomicU8> = OnceLock::new();

fn selected() -> &'static AtomicU8 {
    LANGUAGE.get_or_init(|| {
        let setting = |name| std::env::var(name).ok();
        AtomicU8::new(resolve(
            setting("ZIPOVER_LANGUAGE").as_deref(),
            setting("LC_ALL").as_deref(),
            setting("LC_MESSAGES").as_deref(),
            setting("LANG").as_deref(),
        ) as u8)
    })
}

pub fn language() -> Language {
    match selected().load(Ordering::Relaxed) {
        1 => Language::Chinese,
        _ => Language::English,
    }
}

pub fn set_language(language: Language) {
    selected().store(language as u8, Ordering::Relaxed);
}

pub fn text(zh: &'static str, en: &'static str) -> &'static str {
    match language() {
        Language::Chinese => zh,
        Language::English => en,
    }
}

fn resolve(
    setting: Option<&str>,
    all: Option<&str>,
    messages: Option<&str>,
    lang: Option<&str>,
) -> Language {
    match setting.map(str::trim) {
        Some(value) if value.eq_ignore_ascii_case("zh") => return Language::Chinese,
        Some(value) if value.eq_ignore_ascii_case("en") => return Language::English,
        _ => {}
    }
    let locale = [all, messages, lang]
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|value| !value.is_empty())
        .unwrap_or("en");
    match locale.split(['_', '-', '.', '@']).next() {
        Some(value) if value.eq_ignore_ascii_case("zh") => Language::Chinese,
        _ => Language::English,
    }
}

// Both format strings remain literals, so Rust checks their placeholders.
macro_rules! localized {
    ($operation:ident, $zh:literal, $en:literal $(, $($arguments:tt)*)?) => {
        match $crate::i18n::language() {
            $crate::i18n::Language::Chinese => $operation!($zh $(, $($arguments)*)?),
            $crate::i18n::Language::English => $operation!($en $(, $($arguments)*)?),
        }
    };
}
pub(crate) use localized;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locale_forms_choose_supported_language_or_english() {
        for locale in [
            "zh",
            "zh_CN.UTF-8",
            "zh_Hans",
            "zh-Hant-TW",
            "ZH_hk@variant",
        ] {
            assert_eq!(
                resolve(None, None, None, Some(locale)),
                Language::Chinese,
                "{locale}"
            );
        }
        for locale in [
            "en_US.UTF-8",
            "en-GB",
            "C",
            "C.UTF-8",
            "POSIX",
            "de_DE",
            "zhang",
            "",
        ] {
            assert_eq!(
                resolve(None, None, None, Some(locale)),
                Language::English,
                "{locale}"
            );
        }
        assert_eq!(resolve(None, None, None, None), Language::English);
    }

    #[test]
    fn explicit_override_and_locale_precedence_ignore_empty_values() {
        assert_eq!(
            resolve(Some("en"), Some("zh_CN"), None, None),
            Language::English
        );
        assert_eq!(
            resolve(Some(" ZH "), Some("C"), None, None),
            Language::Chinese
        );
        assert_eq!(
            resolve(None, Some("C"), Some("zh_CN"), Some("zh_CN")),
            Language::English
        );
        assert_eq!(
            resolve(None, Some(""), Some("zh_Hans"), Some("en_US")),
            Language::Chinese
        );
        assert_eq!(
            resolve(Some("auto"), None, Some("en_US"), Some("zh_CN")),
            Language::English
        );
        assert_eq!(
            resolve(None, None, Some(" "), Some("zh_TW")),
            Language::Chinese
        );
    }
}
