from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import time
import unittest
import uuid
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNNER = PROJECT_ROOT / "scripts" / "run_with_lock.ps1"
POWERSHELL = shutil.which("powershell")


@unittest.skipUnless(os.name == "nt" and POWERSHELL, "requires Windows PowerShell")
class RunWithLockTests(unittest.TestCase):
    def run_runner(
        self,
        command_path: Path,
        log_path: Path,
        lock_name: str,
        *,
        max_log_bytes: int = 10 * 1024 * 1024,
        retention: int = 5,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                POWERSHELL,
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(RUNNER),
                "-CommandPath",
                str(command_path),
                "-LogPath",
                str(log_path),
                "-LockName",
                lock_name,
                "-MaxLogBytes",
                str(max_log_bytes),
                "-Retention",
                str(retention),
            ],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

    def test_all_batch_entry_points_use_the_shared_guard(self) -> None:
        entry_points = {
            "run_daily.bat": "daily_operation.log",
            "run_dividend_daily.bat": "dividend_operation.log",
            "run_monthly_eval.bat": "monthly_evaluation.log",
        }
        for file_name, log_name in entry_points.items():
            with self.subTest(file_name=file_name):
                text = (PROJECT_ROOT / file_name).read_text(encoding="utf-8")
                self.assertIn("JQUANTS_PIPELINE_LOCK_HELD", text)
                self.assertIn("scripts\\run_with_lock.ps1", text)
                self.assertIn(log_name, text)

    def test_global_mutex_namespace_is_available(self) -> None:
        with tempfile.TemporaryDirectory(prefix="jquants lock ") as temp_dir:
            work_dir = Path(temp_dir)
            command_path = work_dir / "success.cmd"
            command_path.write_text("@echo off\nexit /b 0\n", encoding="utf-8")

            completed = self.run_runner(
                command_path,
                work_dir / "operation.log",
                f"Global\\JQuantsTest-{uuid.uuid4()}",
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_rotates_log_and_preserves_child_exit_code(self) -> None:
        with tempfile.TemporaryDirectory(prefix="jquants lock ") as temp_dir:
            work_dir = Path(temp_dir)
            command_path = work_dir / "dummy command.cmd"
            log_path = work_dir / "operation.log"
            command_path.write_text(
                "@echo off\n"
                'if not "%JQUANTS_PIPELINE_LOCK_HELD%"=="1" exit /b 91\n'
                'echo inner>>"%~dp0operation.log"\n'
                "exit /b 7\n",
                encoding="utf-8",
            )
            log_path.write_bytes(b"x" * 128)
            oldest_archive = work_dir / "operation.20200101-000000-000.log"
            newer_archive = work_dir / "operation.20210101-000000-000.log"
            oldest_archive.write_text("oldest", encoding="utf-8")
            newer_archive.write_text("newer", encoding="utf-8")
            now = time.time()
            os.utime(oldest_archive, (now - 200, now - 200))
            os.utime(newer_archive, (now - 100, now - 100))
            os.utime(log_path, (now, now))

            completed = self.run_runner(
                command_path,
                log_path,
                f"Local\\JQuantsTest-{uuid.uuid4()}",
                max_log_bytes=64,
                retention=2,
            )

            self.assertEqual(completed.returncode, 7, completed.stderr)
            self.assertEqual(log_path.read_text(encoding="utf-8").strip(), "inner")
            archives = list(work_dir.glob("operation.*.log"))
            self.assertEqual(len(archives), 2)
            self.assertFalse(oldest_archive.exists())
            self.assertTrue(newer_archive.exists())
            self.assertTrue(any(path.read_bytes() == b"x" * 128 for path in archives))

    def test_rejects_a_concurrent_workflow(self) -> None:
        with tempfile.TemporaryDirectory(prefix="jquants lock ") as temp_dir:
            work_dir = Path(temp_dir)
            command_path = work_dir / "should not run.cmd"
            marker_path = work_dir / "command-ran.txt"
            log_path = work_dir / "operation.log"
            holder_path = work_dir / "hold-lock.ps1"
            ready_path = work_dir / "lock-ready.txt"
            lock_name = f"Global\\JQuantsTest-{uuid.uuid4()}"
            command_path.write_text(
                "@echo off\n"
                f'>"{marker_path}" echo ran\n'
                "exit /b 0\n",
                encoding="utf-8",
            )
            holder_path.write_text(
                "param([string]$LockName, [string]$ReadyPath)\n"
                "$mutex = New-Object System.Threading.Mutex($false, $LockName)\n"
                "$held = $mutex.WaitOne()\n"
                "try {\n"
                "  Set-Content -LiteralPath $ReadyPath -Value ready -Encoding ASCII\n"
                "  Start-Sleep -Seconds 30\n"
                "}\n"
                "finally {\n"
                "  if ($held) { $mutex.ReleaseMutex() }\n"
                "  $mutex.Dispose()\n"
                "}\n",
                encoding="utf-8",
            )

            holder = subprocess.Popen(
                [
                    POWERSHELL,
                    "-NoProfile",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(holder_path),
                    "-LockName",
                    lock_name,
                    "-ReadyPath",
                    str(ready_path),
                ],
                cwd=PROJECT_ROOT,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            try:
                deadline = time.monotonic() + 5
                while not ready_path.exists() and time.monotonic() < deadline:
                    time.sleep(0.05)
                self.assertTrue(ready_path.exists(), "lock holder did not start")

                completed = self.run_runner(command_path, log_path, lock_name)

                self.assertEqual(completed.returncode, 75, completed.stderr)
                self.assertFalse(marker_path.exists())
                self.assertIn(
                    "[SKIP] Pipeline lock is already held",
                    log_path.read_text(encoding="utf-8-sig"),
                )
            finally:
                holder.terminate()
                try:
                    holder.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    holder.kill()
                    holder.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
