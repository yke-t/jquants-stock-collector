# -*- coding: utf-8 -*-
import unittest
from unittest.mock import Mock, patch

import requests

from src.notifier import (
    call_google_api_with_retry,
    google_api_status_code,
    is_transient_google_error,
)
from src import notifier


class FakeGoogleError(Exception):
    def __init__(self, status_code):
        super().__init__(f"HTTP {status_code}")
        self.response = Mock(status_code=status_code)


class GoogleApiRetryTest(unittest.TestCase):
    def test_extracts_gspread_style_status_code(self):
        self.assertEqual(google_api_status_code(FakeGoogleError(503)), 503)

    def test_retries_503_then_succeeds(self):
        operation = Mock(side_effect=[FakeGoogleError(503), "ok"])
        sleep = Mock()

        result = call_google_api_with_retry(
            operation,
            "test operation",
            sleep_fn=sleep,
        )

        self.assertEqual(result, "ok")
        self.assertEqual(operation.call_count, 2)
        sleep.assert_called_once_with(2.0)

    def test_uses_bounded_exponential_backoff(self):
        operation = Mock(side_effect=FakeGoogleError(503))
        sleep = Mock()

        with self.assertRaises(FakeGoogleError):
            call_google_api_with_retry(
                operation,
                "test operation",
                sleep_fn=sleep,
            )

        self.assertEqual(operation.call_count, 5)
        self.assertEqual(
            [call.args[0] for call in sleep.call_args_list],
            [2.0, 4.0, 8.0, 16.0],
        )

    def test_does_not_retry_permission_error(self):
        operation = Mock(side_effect=FakeGoogleError(403))
        sleep = Mock()

        with self.assertRaises(FakeGoogleError):
            call_google_api_with_retry(
                operation,
                "test operation",
                sleep_fn=sleep,
            )

        operation.assert_called_once_with()
        sleep.assert_not_called()

    def test_retries_network_timeout(self):
        self.assertTrue(is_transient_google_error(requests.Timeout("timed out")))
        operation = Mock(side_effect=[requests.Timeout("timed out"), "ok"])
        sleep = Mock()

        self.assertEqual(
            call_google_api_with_retry(
                operation,
                "test operation",
                sleep_fn=sleep,
            ),
            "ok",
        )

    def test_signal_update_routes_idempotent_calls_through_retry(self):
        worksheet = Mock()
        spreadsheet = Mock()
        spreadsheet.worksheet.return_value = worksheet
        client = Mock()
        client.open_by_key.return_value = spreadsheet

        with (
            patch.object(notifier, "get_sheets_client", return_value=client),
            patch.object(notifier, "get_sheet_name", return_value="Signals_20260904"),
            patch.object(
                notifier,
                "call_google_api_with_retry",
                side_effect=lambda operation, _name: operation(),
            ) as retry,
        ):
            result = notifier.update_signal_sheet(
                [{"code": "86970", "name": "JPX", "verdict": "WATCH"}],
                spreadsheet_key="test-spreadsheet",
            )

        self.assertTrue(result)
        self.assertEqual(
            [call.args[1] for call in retry.call_args_list],
            [
                "Open signal spreadsheet",
                "Find signal worksheet",
                "Clear signal worksheet",
                "Update signal worksheet",
            ],
        )
        worksheet.update.assert_called_once()


if __name__ == "__main__":
    unittest.main()
