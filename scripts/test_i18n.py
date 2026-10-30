import json
from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[1]
CRATES = ("zipover-cli", "zipover-core")
PLACEHOLDER = re.compile(r"%\{([A-Za-z_]\w*)\}")
# 只覆盖项目现有的字符串字面量调用；路径前缀和换行不影响匹配，动态 key 跳过。
STATIC_REFERENCE = re.compile(
    r'\b(?:tr\s*\(\s*|localized\s*!\s*\(\s*\w+\s*,\s*)"([^"\\]+)"'
)


def translations(crate, language):
    path = ROOT / "crates" / crate / "locales" / f"{language}.json"
    messages = json.loads(path.read_text(encoding="utf-8"))
    # rust-i18n 的版本元数据不是翻译消息。
    messages.pop("_version", None)
    return messages


class TranslationTests(unittest.TestCase):
    def test_languages_have_matching_keys_and_placeholders(self):
        for crate in CRATES:
            english = translations(crate, "en")
            chinese = translations(crate, "zh")
            with self.subTest(crate=crate):
                self.assertEqual(set(english), set(chinese))
            for key in english.keys() & chinese.keys():
                with self.subTest(crate=crate, key=key):
                    self.assertIsInstance(english[key], str)
                    self.assertIsInstance(chinese[key], str)
                    self.assertEqual(
                        set(PLACEHOLDER.findall(english[key])),
                        set(PLACEHOLDER.findall(chinese[key])),
                    )

    def test_static_source_keys_exist(self):
        for crate in CRATES:
            messages = translations(crate, "en")
            reference_count = 0
            for path in sorted((ROOT / "crates" / crate / "src").rglob("*.rs")):
                source = path.read_text(encoding="utf-8")
                for match in STATIC_REFERENCE.finditer(source):
                    reference_count += 1
                    line = source.count("\n", 0, match.start()) + 1
                    with self.subTest(file=str(path.relative_to(ROOT)), line=line):
                        key = match.group(1)
                        self.assertTrue(key in messages, f"缺少翻译资源：{key}")
            self.assertGreater(reference_count, 0, f"{crate}: 未识别到翻译调用")

    def test_reference_matching_handles_current_call_forms(self):
        source = '''
            tr("plain");
            crate::i18n::tr(
                "qualified"
            );
            crate::i18n::localized!(
                format,
                "multiline",
                value = value
            );
            tr(key);
            localized!(format, key);
        '''
        self.assertEqual(
            [match.group(1) for match in STATIC_REFERENCE.finditer(source)],
            ["plain", "qualified", "multiline"],
        )


if __name__ == "__main__":
    unittest.main()
