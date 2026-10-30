import os
from pathlib import Path
import subprocess
import tempfile
import unittest


INSTALLER = Path(__file__).with_name('install-linux.sh')


class InstallerLanguageTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.bin = Path(self.directory.name)
        # 在平台检查或版本检查处退出，确保测试不会下载或安装应用。
        self.write_command('uname', '''#!/bin/sh
if [ "$1" = -s ]; then
  printf '%s\\n' "${ZIPOVER_TEST_PLATFORM:-Darwin}"
else
  printf 'x86_64\\n'
fi
''')

    def write_command(self, name, source):
        path = self.bin / name
        path.write_text(source)
        path.chmod(0o755)

    def run_installer(self, *args, **settings):
        environment = dict(os.environ)
        for name in ('ZIPOVER_LANGUAGE', 'LC_ALL', 'LC_MESSAGES', 'LANG'):
            environment.pop(name, None)
        environment.update(PATH=str(self.bin), LANG='C')
        environment.update(settings)
        result = subprocess.run(
            ['/bin/bash', str(INSTALLER), *args],
            env=environment, capture_output=True, text=True, check=False,
        )
        self.assertEqual(result.returncode, 1, result.stderr)
        return result.stderr

    def test_explicit_language_overrides_system_locale(self):
        for override, system, expected in [
            ('en', 'zh_CN.UTF-8', 'currently supports x86_64 Linux'),
            (' ZH_hant_TW.UTF-8 ', 'C', '目前支持 x86_64 Linux'),
        ]:
            with self.subTest(override=override):
                self.assertIn(expected, self.run_installer(
                    ZIPOVER_LANGUAGE=override, LANG=system,
                ))

    def test_unknown_override_uses_system_locale(self):
        for override in ('auto', 'fr', 'zhang'):
            with self.subTest(override=override):
                self.assertIn('目前支持 x86_64 Linux', self.run_installer(
                    ZIPOVER_LANGUAGE=override, LANG='zh_CN.UTF-8',
                ))

    def test_system_locale_precedence_and_english_fallback(self):
        for settings, expected in [
            ({'LC_ALL': 'C', 'LANG': 'zh_CN.UTF-8'}, 'currently supports'),
            ({'LC_ALL': ' ', 'LC_MESSAGES': 'zh_CN', 'LANG': 'en'}, '目前支持'),
            ({'LC_MESSAGES': 'en_US', 'LANG': 'zh'}, 'currently supports'),
            ({'LANG': 'zhang'}, 'currently supports'),
            ({'LANG': 'fr_FR'}, 'currently supports'),
        ]:
            with self.subTest(settings=settings):
                self.assertIn(expected, self.run_installer(**settings))

    def test_missing_dependency_is_localized(self):
        for language, expected in [('en', 'Install curl first.'), ('zh', '请先安装 curl。')]:
            with self.subTest(language=language):
                self.assertIn(expected, self.run_installer(
                    'invalid', ZIPOVER_TEST_PLATFORM='Linux', ZIPOVER_LANGUAGE=language,
                ))

    def test_invalid_version_is_localized(self):
        for name in ('curl', 'sha256sum'):
            self.write_command(name, '#!/bin/sh\nexit 99\n')
        for language, expected in [('en', 'Use a version such as'), ('zh', '版本格式应为')]:
            with self.subTest(language=language):
                self.assertIn(expected, self.run_installer(
                    'invalid', ZIPOVER_TEST_PLATFORM='Linux', ZIPOVER_LANGUAGE=language,
                ))


if __name__ == '__main__':
    unittest.main()
