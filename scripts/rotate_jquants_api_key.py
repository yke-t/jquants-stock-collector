"""Safely replace the local J-Quants V2 API key.

The candidate key is read with hidden input, validated against a read-only
J-Quants endpoint, and then written to .env while holding the same named mutex
used by the scheduled workflows. The script intentionally does not create a
plaintext backup containing the old credential.
"""

from __future__ import annotations

import argparse
import ctypes
import getpass
import os
import re
import stat
import tempfile
from pathlib import Path
from typing import Any

import requests


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_PATH = PROJECT_ROOT / ".env"
VALIDATION_URL = "https://api.jquants.com/v2/equities/master"
PIPELINE_MUTEX_NAME = r"Global\JQuantsStockCollectorPipeline"

WAIT_OBJECT_0 = 0x00000000
WAIT_ABANDONED = 0x00000080
WAIT_TIMEOUT = 0x00000102


class RotationError(RuntimeError):
    """Raised when the credential cannot be rotated safely."""


def validate_key_format(candidate: str) -> None:
    """Reject values that are empty or unsafe in an unquoted dotenv value."""
    if not candidate:
        raise RotationError("The API key is empty.")
    if not 20 <= len(candidate) <= 512:
        raise RotationError("The API key has an unexpected length.")
    if any(character.isspace() for character in candidate):
        raise RotationError("The API key must not contain whitespace.")
    if any(ord(character) < 33 or ord(character) > 126 for character in candidate):
        raise RotationError("The API key must contain printable ASCII characters only.")
    if any(character in "#='\"" for character in candidate):
        raise RotationError("The API key contains a character that is unsafe in .env.")


def _unquote_dotenv_value(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
        return value[1:-1]
    return value


def current_api_key(env_text: str) -> str | None:
    matches = re.findall(
        r"(?m)^\s*JQUANTS_API_KEY\s*=\s*(.*?)\s*$",
        env_text,
    )
    if len(matches) > 1:
        raise RotationError(".env contains multiple JQUANTS_API_KEY definitions.")
    return _unquote_dotenv_value(matches[0]) if matches else None


def replace_api_key(env_text: str, candidate: str) -> str:
    """Return .env text with exactly one JQUANTS_API_KEY definition."""
    newline = "\r\n" if "\r\n" in env_text else "\n"
    had_trailing_newline = env_text.endswith(("\n", "\r"))
    lines = env_text.splitlines()
    key_indexes = [
        index
        for index, line in enumerate(lines)
        if re.match(r"^\s*JQUANTS_API_KEY\s*=", line)
    ]
    if len(key_indexes) > 1:
        raise RotationError(".env contains multiple JQUANTS_API_KEY definitions.")

    replacement = f"JQUANTS_API_KEY={candidate}"
    if key_indexes:
        lines[key_indexes[0]] = replacement
    else:
        if lines and lines[-1].strip():
            lines.append("")
        lines.append(replacement)

    updated = newline.join(lines)
    if had_trailing_newline or not env_text:
        updated += newline
    return updated


def validate_api_key(candidate: str, *, request_get: Any = requests.get) -> None:
    """Validate a key with one read-only request without logging the value."""
    try:
        response = request_get(
            VALIDATION_URL,
            headers={"x-api-key": candidate},
            params={"code": "86970"},
            timeout=30,
        )
    except requests.RequestException as exc:
        raise RotationError(
            f"J-Quants validation request failed ({type(exc).__name__})."
        ) from exc

    if response.status_code in (401, 403):
        raise RotationError(
            f"J-Quants rejected the candidate API key (HTTP {response.status_code})."
        )
    if response.status_code != 200:
        raise RotationError(
            f"J-Quants validation returned HTTP {response.status_code}; .env was not changed."
        )


class PipelineMutex:
    """Non-blocking Windows named-mutex guard shared with scheduled workflows."""

    def __init__(self, name: str = PIPELINE_MUTEX_NAME) -> None:
        self.name = name
        self.handle: int | None = None
        self.acquired = False

    def __enter__(self) -> "PipelineMutex":
        if os.name != "nt":
            raise RotationError("API-key rotation is supported only on Windows.")

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateMutexW.argtypes = [ctypes.c_void_p, ctypes.c_bool, ctypes.c_wchar_p]
        kernel32.CreateMutexW.restype = ctypes.c_void_p
        kernel32.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
        kernel32.WaitForSingleObject.restype = ctypes.c_uint32
        kernel32.ReleaseMutex.argtypes = [ctypes.c_void_p]
        kernel32.ReleaseMutex.restype = ctypes.c_bool
        kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        kernel32.CloseHandle.restype = ctypes.c_bool

        self.handle = kernel32.CreateMutexW(None, False, self.name)
        if not self.handle:
            raise RotationError(
                f"Could not open the pipeline mutex (Windows error {ctypes.get_last_error()})."
            )

        result = kernel32.WaitForSingleObject(self.handle, 0)
        if result not in (WAIT_OBJECT_0, WAIT_ABANDONED):
            kernel32.CloseHandle(self.handle)
            self.handle = None
            if result == WAIT_TIMEOUT:
                raise RotationError(
                    "A scheduled or manual pipeline is running; .env was not changed."
                )
            raise RotationError(f"Could not acquire the pipeline mutex (result {result}).")

        self.acquired = True
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if not self.handle:
            return
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.ReleaseMutex.argtypes = [ctypes.c_void_p]
        kernel32.ReleaseMutex.restype = ctypes.c_bool
        kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        kernel32.CloseHandle.restype = ctypes.c_bool
        if self.acquired:
            kernel32.ReleaseMutex(self.handle)
        kernel32.CloseHandle(self.handle)
        self.handle = None
        self.acquired = False


def atomic_write_env(env_path: Path, updated_text: str) -> None:
    """Replace .env from the same directory without retaining an old-key backup."""
    env_path = env_path.resolve()
    env_path.parent.mkdir(parents=True, exist_ok=True)
    original_mode = stat.S_IMODE(env_path.stat().st_mode) if env_path.exists() else None
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{env_path.name}.rotate-",
        dir=env_path.parent,
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
            handle.write(updated_text)
            handle.flush()
            os.fsync(handle.fileno())
        if original_mode is not None:
            os.chmod(temporary_path, original_mode)
        os.replace(temporary_path, env_path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def rotate(env_path: Path, candidate: str) -> None:
    validate_key_format(candidate)
    if not env_path.exists():
        raise RotationError(f"Environment file not found: {env_path}")

    # Validate before taking the workflow lock so the critical section stays short.
    validate_api_key(candidate)

    with PipelineMutex():
        env_text = env_path.read_text(encoding="utf-8-sig")
        existing = current_api_key(env_text)
        if existing == candidate:
            raise RotationError("The candidate is identical to the current API key.")
        updated_text = replace_api_key(env_text, candidate)
        atomic_write_env(env_path, updated_text)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate and atomically rotate the local J-Quants V2 API key."
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=DEFAULT_ENV_PATH,
        help="Path to the dotenv file (default: repository .env)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    print("Paste the newly issued J-Quants V2 API key; input is hidden.")
    candidate = getpass.getpass("New JQUANTS_API_KEY: ")
    try:
        rotate(args.env_file, candidate)
    except RotationError as exc:
        print(f"[FAIL] {exc}")
        return 1
    finally:
        candidate = ""

    print(f"[OK] Updated {args.env_file.resolve()}")
    print("[OK] The new key passed a read-only J-Quants V2 request.")
    print("[INFO] No plaintext credential backup was created.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
