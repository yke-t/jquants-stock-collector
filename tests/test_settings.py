# -*- coding: utf-8 -*-
import tempfile
import unittest
from pathlib import Path

from src.settings import PROJECT_ROOT, resolve_project_path


class SettingsTest(unittest.TestCase):
    def test_relative_path_is_resolved_from_project_root(self):
        self.assertEqual(
            resolve_project_path("data/example.db"),
            PROJECT_ROOT / "data" / "example.db",
        )

    def test_absolute_path_is_preserved(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            absolute = Path(temp_dir) / "example.db"
            self.assertEqual(resolve_project_path(absolute), absolute)


if __name__ == "__main__":
    unittest.main()
