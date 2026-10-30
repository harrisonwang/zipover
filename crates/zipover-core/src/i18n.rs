use std::{borrow::Cow, ops::Deref, sync::Once};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Language {
    English,
    Chinese,
}

static INITIALIZED: Once = Once::new();

pub fn initialize() {
    INITIALIZED
        .call_once(|| rust_i18n::set_locale(&locale_from_env(&rust_i18n::available_locales!())));
}

pub fn locale() -> impl Deref<Target = str> {
    initialize();
    rust_i18n::locale()
}

pub fn set_locale(locale: &str) {
    initialize();
    rust_i18n::set_locale(locale);
}

pub fn language() -> Language {
    if locale()
        .split('-')
        .next()
        .is_some_and(|tag| tag.eq_ignore_ascii_case("zh"))
    {
        Language::Chinese
    } else {
        Language::English
    }
}

pub fn set_language(language: Language) {
    set_locale(match language {
        Language::English => "en",
        Language::Chinese => "zh",
    });
}

pub fn text(zh: &'static str, en: &'static str) -> &'static str {
    match language() {
        Language::Chinese => zh,
        Language::English => en,
    }
}

pub fn tr(key: &'static str) -> Cow<'static, str> {
    initialize();
    rust_i18n::t!(key)
}

pub fn match_locale(value: &str, available: &[Cow<'_, str>]) -> Option<String> {
    let mut tag = value.trim().split(['.', '@']).next()?.replace('_', "-");
    loop {
        if let Some(locale) = available
            .iter()
            .find(|locale| locale.eq_ignore_ascii_case(&tag))
        {
            return Some(locale.to_string());
        }
        let (parent, _) = tag.rsplit_once('-')?;
        tag.truncate(parent.len());
    }
}

pub fn locale_from_env(available: &[Cow<'_, str>]) -> String {
    let setting = |name| std::env::var(name).ok();
    resolve_with(
        setting("ZIPOVER_LANGUAGE").as_deref(),
        setting("LC_ALL").as_deref(),
        setting("LC_MESSAGES").as_deref(),
        setting("LANG").as_deref(),
        available,
    )
}

fn resolve_with(
    setting: Option<&str>,
    all: Option<&str>,
    messages: Option<&str>,
    lang: Option<&str>,
    available: &[Cow<'_, str>],
) -> String {
    if let Some(locale) = setting.and_then(|value| match_locale(value, available)) {
        return locale;
    }
    let locale = [all, messages, lang]
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|value| !value.is_empty())
        .unwrap_or("en");
    match_locale(locale, available).unwrap_or_else(|| "en".into())
}

#[cfg(test)]
fn resolve(
    setting: Option<&str>,
    all: Option<&str>,
    messages: Option<&str>,
    lang: Option<&str>,
) -> String {
    resolve_with(
        setting,
        all,
        messages,
        lang,
        &rust_i18n::available_locales!(),
    )
}

macro_rules! localized {
    ($operation:ident, $key:literal $(, $($arguments:tt)*)?) => {{
        $crate::i18n::initialize();
        $operation!("{}", rust_i18n::t!($key $(, $($arguments)*)?))
    }};
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
            assert_eq!(resolve(None, None, None, Some(locale)), "zh", "{locale}");
        }
        for locale in [
            "en_US.UTF-8",
            "en-GB",
            "C",
            "C.UTF-8",
            "POSIX",
            "zz_ZZ",
            "zhang",
            "",
        ] {
            assert_eq!(resolve(None, None, None, Some(locale)), "en", "{locale}");
        }
        assert_eq!(resolve(None, None, None, None), "en");
    }

    #[test]
    fn explicit_override_and_locale_precedence_ignore_empty_values() {
        assert_eq!(resolve(Some("en"), Some("zh_CN"), None, None), "en");
        assert_eq!(resolve(Some(" ZH "), Some("C"), None, None), "zh");
        assert_eq!(resolve(None, Some("C"), Some("zh_CN"), Some("zh_CN")), "en");
        assert_eq!(
            resolve(None, Some(""), Some("zh_Hans"), Some("en_US")),
            "zh"
        );
        assert_eq!(
            resolve(Some("auto"), None, Some("en_US"), Some("zh_CN")),
            "en"
        );
        assert_eq!(resolve(None, None, Some(" "), Some("zh_TW")), "zh");
    }
}
