# -*- coding: utf-8 -*-
import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import requests


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = PROJECT_ROOT / "scripts" / "rotate_jquants_api_key.py"
SPEC = importlib.util.spec_from_file_location("rotate_jquants_api_key", MODULE_PATH)
rotation = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(rotation)


class FakeResponse:
    def __init__(self, status_code):
        self.status_code = status_code


class RotateJQuantsApiKeyTest(unittest.TestCase):
    def test_replaces_existing_key_and_preserves_crlf(self):
        text = "DATABASE_PATH=stock_data.db\r\nJQUANTS_API_KEY=old-key-value-1234567890\r\n"
        updated = rotation.replace_api_key(text, "new-key-value-1234567890")
        self.assertEqual(
            updated,
            "DATABASE_PATH=stock_data.db\r\nJQUANTS_API_KEY=new-key-value-1234567890\r\n",
        )

    def test_appends_key_when_missing(self):
        updated = rotation.replace_api_key(
            "DATABASE_PATH=stock_data.db\n",
            "new-key-value-1234567890",
        )
        self.assertEqual(
            updated,
            "DATABASE_PATH=stock_data.db\n\nJQUANTS_API_KEY=new-key-value-1234567890\n",
        )

    def test_rejects_duplicate_key_definitions(self):
        with self.assertRaises(rotation.RotationError):
            rotation.replace_api_key(
                "JQUANTS_API_KEY=first-key-value-123456\n"
                "JQUANTS_API_KEY=second-key-value-12345\n",
                "new-key-value-1234567890",
            )

    def test_rejects_whitespace_or_dotenv_metacharacters(self):
        for candidate in (
            "key with whitespace 1234567890",
            "key-with-hash-1234567890#",
            "short",
        ):
            with self.subTest(candidate=candidate):
                with self.assertRaises(rotation.RotationError):
                    rotation.validate_key_format(candidate)

    def test_validates_with_read_only_v2_request(self):
        request_get = mock.Mock(return_value=FakeResponse(200))
        rotation.validate_api_key(
            "new-key-value-1234567890",
            request_get=request_get,
        )
        request_get.assert_called_once_with(
            rotation.VALIDATION_URL,
            headers={"x-api-key": "new-key-value-1234567890"},
            params={"code": "86970"},
            timeout=30,
        )

    def test_rejected_or_unavailable_validation_does_not_pass(self):
        for result in (
            FakeResponse(401),
            FakeResponse(503),
            requests.ConnectionError("offline"),
        ):
            with self.subTest(result=result):
                if isinstance(result, Exception):
                    request_get = mock.Mock(side_effect=result)
                else:
                    request_get = mock.Mock(return_value=result)
                with self.assertRaises(rotation.RotationError):
                    rotation.validate_api_key(
                        "new-key-value-1234567890",
                        request_get=request_get,
                    )

    def test_atomic_write_does_not_leave_plaintext_backup(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text(
                "JQUANTS_API_KEY=old-key-value-1234567890\n",
                encoding="utf-8",
            )
            rotation.atomic_write_env(
                env_path,
                "JQUANTS_API_KEY=new-key-value-1234567890\n",
            )
            self.assertEqual(
                env_path.read_text(encoding="utf-8"),
                "JQUANTS_API_KEY=new-key-value-1234567890\n",
            )
            self.assertEqual([path.name for path in Path(temp_dir).iterdir()], [".env"])


if __name__ == "__main__":
    unittest.main()
