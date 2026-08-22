"""Checker service — runs the actual file checking against extracted archives.

The checker is pluggable: by default it runs an external command (CHECKER_COMMAND),
or falls back to scanning extracted directories for cookie files and producing
a summary. The full Minecraft auth/ban-check logic lives in the Go binary;
this service orchestrates it as a subprocess.
"""

import asyncio
import json
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from backend.config import CHECKER_COMMAND, CHECKER_WORKERS, RESULTS_DIR
from backend.logging_config import ComponentLogger

log = ComponentLogger("Checker")


class CheckerResult:
    """Result from running the checker on an extracted directory."""

    def __init__(self):
        self.total = 0
        self.valid_mc = 0
        self.no_mc = 0
        self.two_fa = 0
        self.errors = 0
        self.hyp_banned = 0
        self.hyp_unbanned = 0
        self.don_banned = 0
        self.don_unbanned = 0
        self.elapsed_seconds = 0.0
        self.results_dir: Optional[str] = None
        self.error_message: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "valid_mc": self.valid_mc,
            "no_mc": self.no_mc,
            "two_fa": self.two_fa,
            "errors": self.errors,
            "hyp_banned": self.hyp_banned,
            "hyp_unbanned": self.hyp_unbanned,
            "don_banned": self.don_banned,
            "don_unbanned": self.don_unbanned,
            "elapsed_seconds": self.elapsed_seconds,
            "results_dir": self.results_dir,
        }


class CheckerService:
    """Manages checker execution."""

    def __init__(self):
        self._running = False
        self._process: Optional[asyncio.subprocess.Process] = None
        self._current_job_id: Optional[str] = None
        self._files_processed = 0
        self._files_total = 0

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def current_job(self) -> Optional[str]:
        return self._current_job_id

    def status(self) -> dict:
        return {
            "running": self._running,
            "current_job": self._current_job_id,
            "files_processed": self._files_processed,
            "files_total": self._files_total,
            "checker_command": CHECKER_COMMAND or "(built-in scanner)",
        }

    async def run_check(
        self,
        extract_dir: str,
        job_id: str,
        progress_callback=None,
    ) -> CheckerResult:
        """Run the checker against extracted files."""
        self._running = True
        self._current_job_id = job_id
        self._files_processed = 0
        result = CheckerResult()
        start_time = time.time()

        log_j = log.with_job(job_id)

        try:
            if CHECKER_COMMAND:
                result = await self._run_external(extract_dir, job_id, progress_callback)
            else:
                result = await self._run_builtin(extract_dir, job_id, progress_callback)

            result.elapsed_seconds = time.time() - start_time

        except Exception as e:
            result.error_message = str(e)
            log_j.error(f"Checker failed: {e}")

        finally:
            self._running = False
            self._current_job_id = None

        return result

    async def _run_external(
        self, extract_dir: str, job_id: str, progress_callback=None
    ) -> CheckerResult:
        """Run an external checker command."""
        result = CheckerResult()
        log_j = log.with_job(job_id)

        # Create a run-specific results directory
        run_dir = RESULTS_DIR / datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        run_dir.mkdir(parents=True, exist_ok=True)
        result.results_dir = str(run_dir)

        cmd = CHECKER_COMMAND.replace("{input}", extract_dir).replace("{output}", str(run_dir))
        log_j.info(f"Running checker: {cmd}")

        try:
            self._process = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(Path(CHECKER_COMMAND.split()[0]).parent) if os.path.exists(CHECKER_COMMAND.split()[0]) else None,
            )

            stdout, stderr = await self._process.communicate()

            if self._process.returncode != 0:
                err_text = stderr.decode("utf-8", errors="replace")[:500]
                result.error_message = f"Checker exited with code {self._process.returncode}: {err_text}"
                log_j.error(result.error_message)
            else:
                # Try to parse output as JSON for result stats
                out_text = stdout.decode("utf-8", errors="replace")
                try:
                    data = json.loads(out_text)
                    result.total = data.get("total", 0)
                    result.valid_mc = data.get("validMC", 0)
                    result.no_mc = data.get("noMC", 0)
                    result.errors = data.get("errors", 0)
                except json.JSONDecodeError:
                    # Count output files as results
                    result.total = sum(1 for _ in run_dir.rglob("*.txt"))
                    result.valid_mc = sum(1 for _ in (run_dir / "valid_mc").rglob("*.txt")) if (run_dir / "valid_mc").exists() else 0

                log_j.info(f"Checker complete: {result.total} checked, {result.valid_mc} valid")

        except asyncio.CancelledError:
            if self._process and self._process.returncode is None:
                self._process.terminate()
            raise
        finally:
            self._process = None

        return result

    async def _run_builtin(
        self, extract_dir: str, job_id: str, progress_callback=None
    ) -> CheckerResult:
        """Built-in scanner — searches every .txt in every subfolder, every line.

        Looks for Microsoft/Minecraft cookie keywords on each line.
        Any file with at least one matching line is flagged as a hit.
        """
        result = CheckerResult()
        log_j = log.with_job(job_id)

        run_dir = RESULTS_DIR / datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        run_dir.mkdir(parents=True, exist_ok=True)
        result.results_dir = str(run_dir)

        extract_path = Path(extract_dir)

        # Gather every .txt in every subfolder
        txt_files = list(extract_path.rglob("*.txt"))
        self._files_total = len(txt_files)
        result.total = len(txt_files)

        log_j.info(f"Scanning {len(txt_files)} .txt files across all subfolders...")

        # Keywords to match (checked against every line, case-insensitive)
        KEYWORDS = (
            ".minecraft.net", "minecraft.net", "mojang",
            "xbox", "xboxlive.com", "live.com",
            "microsoftonline", "login.live.com",
            "login.microsoftonline.com",
            "sisu.xboxlive.com",
        )

        cookie_files = []   # files with at least one matching line
        other_files = []     # .txt files with zero matches
        matched_lines = []   # (filepath, line) tuples for the results file

        for f in txt_files:
            found = False
            try:
                with open(f, "r", errors="replace") as fh:
                    for line in fh:
                        lower_line = line.lower()
                        if any(kw in lower_line for kw in KEYWORDS):
                            found = True
                            matched_lines.append((str(f.relative_to(extract_path)), line.rstrip()))
            except Exception:
                pass

            if found:
                cookie_files.append(f)
            else:
                other_files.append(f)

            self._files_processed += 1
            if progress_callback and self._files_processed % 100 == 0:
                await progress_callback(self._files_processed, self._files_total)

        result.valid_mc = len(cookie_files)
        result.no_mc = len(other_files)

        # Write summary
        summary_file = run_dir / "scan_summary.txt"
        summary_lines = [
            f"Scan Results",
            f"============",
            f"Total .txt files scanned: {result.total}",
            f"Files with MC cookies: {len(cookie_files)}",
            f"Files without MC cookies: {len(other_files)}",
            f"Total matching lines: {len(matched_lines)}",
            f"",
            f"Files with hits:",
        ]
        for f in cookie_files:
            summary_lines.append(f"  {f.relative_to(extract_path)}")

        summary_file.write_text("\n".join(summary_lines))

        # Write all matched lines to a separate file
        if matched_lines:
            hits_file = run_dir / "matched_lines.txt"
            with open(hits_file, "w", errors="replace") as out:
                for filepath, line in matched_lines:
                    out.write(f"{filepath}\t{line}\n")

        # Copy cookie files to results
        found_dir = run_dir / "found_cookies"
        found_dir.mkdir(exist_ok=True)
        for f in cookie_files:
            try:
                dest = found_dir / f.relative_to(extract_path)
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(f, dest)
            except Exception:
                pass

        log_j.info(
            f"Scan complete: {result.total} .txt files, "
            f"{len(cookie_files)} with MC cookies, "
            f"{len(matched_lines)} matching lines"
        )

        return result

    async def stop(self):
        """Stop the currently running checker."""
        if self._process and self._process.returncode is None:
            log.warning("Stopping checker process...")
            self._process.terminate()
            try:
                await asyncio.wait_for(self._process.wait(), timeout=10)
            except asyncio.TimeoutError:
                self._process.kill()

        self._running = False
        self._current_job_id = None


# Global singleton
checker_service = CheckerService()
