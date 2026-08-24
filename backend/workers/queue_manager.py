"""Job queue manager — orchestrates the file processing pipeline.

Pipeline: Telegram → Download → Extract → Check → Discord → Cleanup → Next
Only ONE file is processed at a time.
"""

import asyncio
import io
import os
import shutil
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import DOWNLOAD_DIR, TEMP_DIR, RESULTS_DIR, MAX_RETRIES, RETRY_DELAY
from backend.database import async_session
from backend.models import Job, JobStatus, Statistic
from backend.logging_config import ComponentLogger
from backend.websocket_hub import hub
from backend.services.telegram_service import telegram_service, _sibling_matcher
from backend.services.discord_service import discord_service
from backend.services.checker_service import checker_service
from backend.workers.extractor import extract_archive

log = ComponentLogger("Queue")


class QueueManager:
    """Manages the job processing queue. Processes one job at a time."""

    def __init__(self):
        self._running = False
        self._paused = False
        self._current_job: Optional[Job] = None
        self._current_task: Optional[asyncio.Task] = None  # the job processing task
        self._task: Optional[asyncio.Task] = None  # the loop task
        self._download_progress = {}

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def is_paused(self) -> bool:
        return self._paused

    @property
    def current_job(self) -> Optional[Job]:
        return self._current_job

    def status(self) -> dict:
        return {
            "running": self._running,
            "paused": self._paused,
            "current_job_id": self._current_job.id if self._current_job else None,
            "current_job_filename": self._current_job.filename if self._current_job else None,
            "current_job_status": self._current_job.status.value if self._current_job else None,
        }

    async def start(self):
        """Start the queue processing loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._process_loop())
        log.info("Queue manager started")

    async def stop(self):
        """Stop the queue processing loop."""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        log.info("Queue manager stopped")

    def pause(self):
        """Pause queue processing (finishes current job)."""
        self._paused = True
        log.info("Queue paused")

    def resume(self):
        """Resume queue processing."""
        self._paused = False
        log.info("Queue resumed")

    async def add_job(
        self,
        filename: str,
        file_size: int = 0,
        telegram_channel_id: int | None = None,
        telegram_message_id: int | None = None,
        source_url: str | None = None,
        archive_password: str | None = None,
    ) -> str:
        """Add a new job to the queue. Returns the job ID."""
        async with async_session() as session:
            # Get next queue position
            result = await session.execute(
                select(func.max(Job.queue_position))
            )
            max_pos = result.scalar() or 0

            job = Job(
                filename=filename,
                file_size=file_size,
                status=JobStatus.queued,
                queue_position=max_pos + 1,
                telegram_channel_id=telegram_channel_id,
                telegram_message_id=telegram_message_id,
                source_url=source_url,
                archive_password=archive_password,
                max_retries=MAX_RETRIES,
            )
            session.add(job)
            await session.commit()
            await session.refresh(job)

            pw_note = " (with password)" if archive_password else ""
            log.info(f"Job added: {filename}{pw_note} (ID: {job.id[:8]})")
            await hub.broadcast("queue_update", {"action": "added", "job_id": job.id, "filename": filename})
            return job.id

    async def _process_loop(self):
        """Main processing loop — picks and processes jobs one at a time."""
        log.info("Queue processing loop started")

        while self._running:
            try:
                if self._paused:
                    await asyncio.sleep(2)
                    continue

                # Get next queued job
                job = await self._get_next_job()
                if not job:
                    await asyncio.sleep(3)
                    continue

                self._current_job = job
                # Run the job in its own task so we can cancel it
                self._current_task = asyncio.create_task(self._process_job(job))
                try:
                    await self._current_task
                except asyncio.CancelledError:
                    log.warning(f"Job {job.id[:8]} was cancelled")
                    await self._handle_cancel(job)
                finally:
                    self._current_job = None
                    self._current_task = None

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Queue loop error: {e}")
                self._current_job = None
                self._current_task = None
                await asyncio.sleep(5)

    async def _get_next_job(self) -> Optional[Job]:
        """Get the next job in the queue."""
        async with async_session() as session:
            result = await session.execute(
                select(Job)
                .where(Job.status == JobStatus.queued)
                .order_by(Job.queue_position)
                .limit(1)
            )
            job = result.scalar_one_or_none()
            return job

    async def _process_job(self, job: Job):
        """Process a single job through the full pipeline."""
        log_j = log.with_job(job.id)
        start_time = time.time()

        try:
            # 1. Download
            await self._update_status(job.id, JobStatus.downloading)
            log_j.info(f"Downloading: {job.filename}")
            download_path = await self._download(job)

            if not download_path or not os.path.exists(download_path):
                raise RuntimeError("Download failed: file not found")

            # 2. Verify download
            actual_size = os.path.getsize(download_path)
            log_j.info(f"Download verified: {actual_size} bytes")
            await self._update_job(job.id, {
                "status": JobStatus.downloaded,
                "download_path": download_path,
                "downloaded_bytes": actual_size,
            })

            # 3. Extract (pass archive password if one was found in the message)
            await self._update_status(job.id, JobStatus.extracting)
            # Re-fetch the job to get archive_password (the initial object may be detached)
            archive_password = await self._get_job_password(job.id)
            if archive_password:
                log_j.info(f"Extracting with password: {job.filename}")
            else:
                log_j.info(f"Extracting: {job.filename}")
            extract_dir = str(TEMP_DIR / f"job_{job.id[:8]}")
            files_extracted = await extract_archive(download_path, extract_dir, password=archive_password)
            log_j.info(f"Extracted {files_extracted} files")
            await self._update_job(job.id, {
                "extract_dir": extract_dir,
                "files_extracted": files_extracted,
                "files_total": files_extracted,
            })

            # 4. Check
            await self._update_status(job.id, JobStatus.checking)
            log_j.info("Running checker...")

            async def on_progress(processed, total):
                await self._update_job(job.id, {
                    "files_checked": processed,
                    "files_total": total,
                    "progress": (processed / total * 100) if total > 0 else 0,
                })
                await hub.broadcast("job_progress", {
                    "job_id": job.id,
                    "files_checked": processed,
                    "files_total": total,
                    "progress": (processed / total * 100) if total > 0 else 0,
                })

            checker_result = await checker_service.run_check(extract_dir, job.id, on_progress)

            await self._update_job(job.id, {
                "status": JobStatus.processing_results,
                "results_summary": checker_result.to_dict(),
                "files_checked": checker_result.total,
            })

            # 5. Send results to Discord
            await self._update_status(job.id, JobStatus.sending_discord)
            log_j.info("Sending results to Discord...")

            elapsed = time.time() - start_time
            processing_time = _format_duration(elapsed)

            # Create a lightweight zip for Discord — summary + matched lines only.
            # The full found_cookies/ directory stays on disk but is NOT uploaded
            # (it can be hundreds of MB with thousands of files).
            results_zip_data = None
            if checker_result.results_dir and os.path.exists(checker_result.results_dir):
                results_zip_data = _create_discord_zip(checker_result.results_dir)

            discord_ok = await discord_service.send_job_result(
                filename=job.filename,
                job_id=job.id,
                status="completed",
                results_summary=checker_result.to_dict(),
                processing_time=processing_time,
                results_zip_data=results_zip_data,
            )

            await self._update_job(job.id, {"discord_sent": discord_ok})

            # 6. Cleanup
            await self._update_status(job.id, JobStatus.cleaning_up)
            log_j.info("Cleaning up temporary files...")
            await self._cleanup(download_path, extract_dir)

            # 7. Mark complete
            await self._update_job(job.id, {
                "status": JobStatus.completed,
                "completed_at": datetime.now(timezone.utc),
                "cleanup_done": True,
                "progress": 100.0,
            })

            total_time = time.time() - start_time
            log_j.info(f"Job completed in {_format_duration(total_time)}")
            await hub.broadcast("job_complete", {"job_id": job.id, "filename": job.filename})

            # Update statistics — wrapped so a stats error can't un-complete the job
            try:
                await self._update_stats(job.id, checker_result, total_time)
            except Exception as stats_err:
                log_j.warning(f"Stats update failed (job still completed): {stats_err}")

        except Exception as e:
            log_j.error(f"Job failed: {e}")
            await self._handle_failure(job, str(e))

    async def _download(self, job: Job) -> Optional[str]:
        """Download the file for a job.

        For multi-volume archives (``.part1.rar`` etc.) this also fetches the
        sibling volumes so extraction has every part on disk — done even when the
        first volume is already present from a previous attempt.
        """
        dest_dir = str(DOWNLOAD_DIR)
        os.makedirs(dest_dir, exist_ok=True)

        # ── Disk space check ────────────────────────────────────────────────
        try:
            import shutil as _shutil
            disk = _shutil.disk_usage(dest_dir)
            free_gb = disk.free / (1024 ** 3)
            file_gb = (job.file_size or 0) / (1024 ** 3)
            # Need roughly 3× the file size (download + extraction + results)
            needed_gb = max(file_gb * 3, 2)  # at least 2 GB free
            if free_gb < needed_gb:
                log.warning(
                    f"LOW DISK SPACE: {free_gb:.1f} GB free, need ~{needed_gb:.1f} GB "
                    f"for {job.filename} ({file_gb:.1f} GB)"
                )
            else:
                log.info(f"Disk space OK: {free_gb:.1f} GB free")
        except Exception:
            pass

        # Track download speed via byte deltas — reused for part1 and siblings.
        dl_state = {"last_bytes": 0, "last_time": time.monotonic(), "speed": 0}

        async def progress(current, total):
            now = time.monotonic()
            elapsed = now - dl_state["last_time"]
            if elapsed >= 0.5:  # update speed every 500ms, don't spam DB
                delta = current - dl_state["last_bytes"]
                dl_state["speed"] = int(delta / elapsed) if elapsed > 0 else 0
                dl_state["last_bytes"] = current
                dl_state["last_time"] = now

                pct = (current / total * 100) if total > 0 else 0
                await self._update_job(job.id, {
                    "downloaded_bytes": current,
                    "file_size": total,
                    "progress": pct,
                    "download_speed": dl_state["speed"],
                })
                await hub.broadcast("download_progress", {
                    "job_id": job.id,
                    "downloaded": current,
                    "total": total,
                    "progress": pct,
                    "speed": dl_state["speed"],
                })

        # ── Fetch the first volume ────────────────────────────────────────────
        part1_path: Optional[str] = None

        # Skip re-download if the file already exists on disk (e.g. retry after
        # an extraction failure). Siblings are still ensured below.
        if job.download_path and os.path.exists(job.download_path):
            actual = os.path.getsize(job.download_path)
            if actual > 0 and (job.file_size == 0 or actual == job.file_size):
                log.info(f"File already on disk, skipping download: {job.download_path} ({actual} bytes)")
                part1_path = job.download_path

        if part1_path is None:
            if job.telegram_channel_id and job.telegram_message_id:
                # Don't waste retries on a disconnected Telegram client
                if not telegram_service.is_ready:
                    raise RuntimeError(
                        "Telegram is not connected — waiting for reconnection"
                    )
                part1_path = await telegram_service.download_file(
                    job.telegram_channel_id,
                    job.telegram_message_id,
                    dest_dir,
                    progress_callback=progress,
                )
            elif job.source_url:
                # URL downloads can't resolve sibling volumes — return directly.
                return await self._download_url(job, dest_dir)
            else:
                raise RuntimeError("No download source configured for this job")

        # ── Fetch sibling volumes for multi-part archives ─────────────────────
        if part1_path and job.telegram_channel_id and job.telegram_message_id:
            if not telegram_service.is_ready:
                raise RuntimeError(
                    "Telegram is not connected — cannot fetch archive volumes"
                )
            siblings = await telegram_service.download_archive_volumes(
                job.telegram_channel_id,
                job.telegram_message_id,
                os.path.basename(part1_path),
                dest_dir,
                progress_callback=progress,
            )
            if siblings:
                log.with_job(job.id).info(
                    f"Fetched {len(siblings)} additional volume(s) for {job.filename}"
                )

        return part1_path

    async def _download_url(self, job: Job, dest_dir: str) -> str:
        """Download a file from a URL."""
        import ssl
        import aiohttp
        import certifi
        log_j = log.with_job(job.id)

        ssl_ctx = ssl.create_default_context(cafile=certifi.where())
        connector = aiohttp.TCPConnector(ssl=ssl_ctx)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(job.source_url) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status} downloading {job.source_url}")

                total = int(resp.headers.get("Content-Length", 0))
                filename = job.filename or "download"
                dest_path = os.path.join(dest_dir, filename)

                downloaded = 0
                dl_state = {"last_bytes": 0, "last_time": time.monotonic(), "speed": 0}
                with open(dest_path, "wb") as f:
                    async for chunk in resp.content.iter_chunked(64 * 1024):
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total > 0:
                            now = time.monotonic()
                            elapsed = now - dl_state["last_time"]
                            if elapsed >= 0.5:
                                delta = downloaded - dl_state["last_bytes"]
                                dl_state["speed"] = int(delta / elapsed) if elapsed > 0 else 0
                                dl_state["last_bytes"] = downloaded
                                dl_state["last_time"] = now

                                pct = downloaded / total * 100
                                await self._update_job(job.id, {
                                    "downloaded_bytes": downloaded,
                                    "file_size": total,
                                    "progress": pct,
                                    "download_speed": dl_state["speed"],
                                })
                                await hub.broadcast("download_progress", {
                                    "job_id": job.id,
                                    "downloaded": downloaded,
                                    "total": total,
                                    "progress": pct,
                                    "speed": dl_state["speed"],
                                })

                return dest_path

    async def _should_keep_downloads(self) -> bool:
        """Check the keep_downloads setting from the DB."""
        try:
            from backend.models import Setting
            async with async_session() as session:
                result = await session.execute(
                    select(Setting).where(Setting.key == "keep_downloads")
                )
                setting = result.scalar_one_or_none()
                return setting is not None and setting.value == "true"
        except Exception:
            return False

    async def _cleanup(self, download_path: str, extract_dir: str):
        """Clean up temporary files after processing."""
        keep = await self._should_keep_downloads()

        # Remove downloaded archive + any sibling volumes (unless keep-downloads is on)
        if download_path and os.path.exists(download_path):
            if keep:
                log.info(f"Keeping download (keep-downloads ON): {os.path.basename(download_path)}")
            else:
                targets = [download_path]
                # For multi-volume archives, also remove the sibling volumes.
                matcher = _sibling_matcher(os.path.basename(download_path))
                if matcher is not None:
                    folder = os.path.dirname(download_path)
                    for name in os.listdir(folder):
                        if matcher.match(name):
                            sib = os.path.join(folder, name)
                            if sib not in targets:
                                targets.append(sib)
                for target in targets:
                    try:
                        os.remove(target)
                    except OSError as e:
                        log.warning(f"Failed to remove download {os.path.basename(target)}: {e}")

        # Always remove extracted files (they're just unpacked temp copies)
        if extract_dir and os.path.exists(extract_dir):
            try:
                shutil.rmtree(extract_dir, ignore_errors=True)
            except Exception as e:
                log.warning(f"Failed to remove extract dir: {e}")

    async def _handle_failure(self, job: Job, error: str):
        """Handle a failed job — retry or mark as failed."""
        async with async_session() as session:
            result = await session.execute(select(Job).where(Job.id == job.id))
            db_job = result.scalar_one_or_none()
            if not db_job:
                return

            if db_job.retry_count < db_job.max_retries:
                db_job.retry_count += 1
                db_job.status = JobStatus.queued
                db_job.error_message = error
                log.warning(f"Job {job.id[:8]} failed, retry {db_job.retry_count}/{db_job.max_retries}")
                await session.commit()

                await hub.broadcast("job_retry", {
                    "job_id": job.id,
                    "retry": db_job.retry_count,
                    "max_retries": db_job.max_retries,
                    "error": error,
                })

                await asyncio.sleep(RETRY_DELAY)
            else:
                db_job.status = JobStatus.failed
                db_job.error_message = error
                db_job.completed_at = datetime.now(timezone.utc)
                await session.commit()

                log.error(f"Job {job.id[:8]} permanently failed after {db_job.max_retries} retries")

                # Update failed job stats
                try:
                    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                    async with async_session() as stats_sess:
                        stat_result = await stats_sess.execute(
                            select(Statistic).where(Statistic.date == today)
                        )
                        stat = stat_result.scalar_one_or_none()
                        if not stat:
                            stat = Statistic(date=today)
                            stats_sess.add(stat)
                        stat.jobs_failed = (stat.jobs_failed or 0) + 1
                        await stats_sess.commit()
                except Exception:
                    pass

                # Notify Discord about failure
                await discord_service.send_embed(
                    title="❌ CHECK FAILED",
                    description=f"File: `{job.filename}`\nError: {error[:500]}",
                    color=0xEF4444,
                    fields=[{"name": "Job ID", "value": f"`{job.id[:8]}`", "inline": True}],
                )

                await hub.broadcast("job_failed", {"job_id": job.id, "error": error})

                # Cleanup on failure — respect keep_downloads setting
                try:
                    keep = await self._should_keep_downloads()
                    if db_job.download_path and os.path.exists(db_job.download_path):
                        if keep:
                            log.info(f"Keeping download after failure (keep-downloads ON): {os.path.basename(db_job.download_path)}")
                        else:
                            os.remove(db_job.download_path)
                    if db_job.extract_dir and os.path.exists(db_job.extract_dir):
                        shutil.rmtree(db_job.extract_dir, ignore_errors=True)
                except Exception:
                    pass

    async def _get_job_password(self, job_id: str) -> Optional[str]:
        """Fetch the archive_password for a job from the DB."""
        async with async_session() as session:
            result = await session.execute(select(Job.archive_password).where(Job.id == job_id))
            return result.scalar_one_or_none()

    async def _update_status(self, job_id: str, status: JobStatus):
        """Update just the job status."""
        await self._update_job(job_id, {"status": status})
        await hub.broadcast("job_status", {"job_id": job_id, "status": status.value})

    async def _update_job(self, job_id: str, updates: dict):
        """Update job fields in the database."""
        async with async_session() as session:
            result = await session.execute(select(Job).where(Job.id == job_id))
            job = result.scalar_one_or_none()
            if job:
                for key, value in updates.items():
                    setattr(job, key, value)
                if "status" not in updates:
                    pass
                elif updates.get("status") == JobStatus.downloading:
                    job.started_at = datetime.now(timezone.utc)
                await session.commit()

    async def _update_stats(self, job_id: str, result, elapsed: float):
        """Update daily statistics.

        Accepts ``job_id`` (not a detached Job object) so it can fetch the
        latest state from the database — in particular ``discord_sent`` and
        ``file_size`` which are updated during the pipeline after the
        original object was loaded.
        """
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        async with async_session() as session:
            # Fetch fresh job state so discord_sent / file_size are current
            job_result = await session.execute(select(Job).where(Job.id == job_id))
            fresh_job = job_result.scalar_one_or_none()

            stat_result = await session.execute(
                select(Statistic).where(Statistic.date == today)
            )
            stat = stat_result.scalar_one_or_none()
            if not stat:
                stat = Statistic(date=today)
                session.add(stat)

            stat.jobs_completed = (stat.jobs_completed or 0) + 1
            stat.files_checked = (stat.files_checked or 0) + result.total
            stat.data_downloaded_bytes = (stat.data_downloaded_bytes or 0) + (
                fresh_job.file_size if fresh_job else 0
            )
            stat.total_processing_seconds = (stat.total_processing_seconds or 0) + elapsed
            stat.discord_messages_sent = (stat.discord_messages_sent or 0) + (
                1 if fresh_job and fresh_job.discord_sent else 0
            )
            await session.commit()

    # ── Public job management ───────────────────────────────────────────────

    async def retry_job(self, job_id: str):
        """Retry a failed job."""
        async with async_session() as session:
            result = await session.execute(select(Job).where(Job.id == job_id))
            job = result.scalar_one_or_none()
            if job and job.status == JobStatus.failed:
                job.status = JobStatus.queued
                job.retry_count = 0
                job.error_message = None
                job.progress = 0.0
                await session.commit()
                log.info(f"Job {job_id[:8]} re-queued for retry")

    async def retest_job(self, job_id: str) -> bool:
        """Re-run extract→check→discord on an already-downloaded file.

        Skips the download entirely.  Returns False if no file on disk.
        """
        async with async_session() as session:
            result = await session.execute(select(Job).where(Job.id == job_id))
            job = result.scalar_one_or_none()
            if not job:
                return False

            # Must have a file on disk
            if not job.download_path or not os.path.exists(job.download_path):
                return False

            # Reset the job for reprocessing
            job.status = JobStatus.downloaded  # skip download, start at extract
            job.retry_count = 0
            job.error_message = None
            job.progress = 0.0
            job.files_checked = 0
            job.files_extracted = 0
            job.discord_sent = False
            job.cleanup_done = False
            job.completed_at = None
            job.results_summary = None
            await session.commit()

            log.info(f"Job {job_id[:8]} queued for RETEST (skipping download)")

            # Kick off the retest in a task
            asyncio.create_task(self._retest_pipeline(job_id, job.download_path))
            return True

    async def _retest_pipeline(self, job_id: str, download_path: str):
        """Run extract→check→discord→cleanup without downloading."""
        log_j = log.with_job(job_id)
        start_time = time.time()

        try:
            # Fetch fresh job data
            async with async_session() as session:
                result = await session.execute(select(Job).where(Job.id == job_id))
                job = result.scalar_one_or_none()
                if not job:
                    return

            log_j.info(f"RETEST started for {job.filename} (file on disk)")

            # 1. Extract
            await self._update_status(job_id, JobStatus.extracting)
            archive_password = await self._get_job_password(job_id)
            if archive_password:
                log_j.info(f"Extracting with password: {job.filename}")
            else:
                log_j.info(f"Extracting: {job.filename}")
            extract_dir = str(TEMP_DIR / f"job_{job_id[:8]}")
            files_extracted = await extract_archive(download_path, extract_dir, password=archive_password)
            log_j.info(f"Extracted {files_extracted} files")
            await self._update_job(job_id, {
                "extract_dir": extract_dir,
                "files_extracted": files_extracted,
                "files_total": files_extracted,
            })

            # 2. Check
            await self._update_status(job_id, JobStatus.checking)
            log_j.info("Running checker...")

            async def on_progress(processed, total):
                await self._update_job(job_id, {
                    "files_checked": processed,
                    "files_total": total,
                    "progress": (processed / total * 100) if total > 0 else 0,
                })
                await hub.broadcast("job_progress", {
                    "job_id": job_id,
                    "files_checked": processed,
                    "files_total": total,
                    "progress": (processed / total * 100) if total > 0 else 0,
                })

            checker_result = await checker_service.run_check(extract_dir, job_id, on_progress)

            await self._update_job(job_id, {
                "status": JobStatus.processing_results,
                "results_summary": checker_result.to_dict(),
                "files_checked": checker_result.total,
            })

            # 3. Discord
            await self._update_status(job_id, JobStatus.sending_discord)
            log_j.info("Sending results to Discord...")

            elapsed = time.time() - start_time
            processing_time = _format_duration(elapsed)

            results_zip_data = None
            if checker_result.results_dir and os.path.exists(checker_result.results_dir):
                results_zip_data = _create_discord_zip(checker_result.results_dir)

            discord_ok = await discord_service.send_job_result(
                filename=job.filename,
                job_id=job_id,
                status="completed",
                results_summary=checker_result.to_dict(),
                processing_time=processing_time,
                results_zip_data=results_zip_data,
            )

            await self._update_job(job_id, {"discord_sent": discord_ok})

            # 4. Cleanup (extracted files only — keep the download)
            await self._update_status(job_id, JobStatus.cleaning_up)
            if extract_dir and os.path.exists(extract_dir):
                shutil.rmtree(extract_dir, ignore_errors=True)

            # 5. Done
            await self._update_job(job_id, {
                "status": JobStatus.completed,
                "completed_at": datetime.now(timezone.utc),
                "cleanup_done": True,
                "progress": 100.0,
            })

            total_time = time.time() - start_time
            log_j.info(f"RETEST completed in {_format_duration(total_time)}")
            await hub.broadcast("job_complete", {"job_id": job_id, "filename": job.filename})

            try:
                await self._update_stats(job_id, checker_result, total_time)
            except Exception as stats_err:
                log_j.warning(f"Stats update failed (job still completed): {stats_err}")

        except Exception as e:
            log_j.error(f"RETEST failed: {e}")
            async with async_session() as session:
                result = await session.execute(select(Job).where(Job.id == job_id))
                db_job = result.scalar_one_or_none()
                if db_job:
                    db_job.status = JobStatus.failed
                    db_job.error_message = f"Retest failed: {e}"
                    await session.commit()
            await hub.broadcast("job_failed", {"job_id": job_id, "error": str(e)})

    async def cancel_job(self, job_id: str):
        """Cancel a queued OR in-progress job."""
        # If it's the currently running job, cancel the task
        if self._current_job and self._current_job.id == job_id:
            log.info(f"Cancelling active job {job_id[:8]}...")
            # Tell Telegram to stop downloading
            telegram_service.cancel_download()
            # Cancel the processing task
            if self._current_task and not self._current_task.done():
                self._current_task.cancel()
            return

        # Otherwise cancel it in the DB (queued/waiting)
        async with async_session() as session:
            result = await session.execute(select(Job).where(Job.id == job_id))
            job = result.scalar_one_or_none()
            if job and job.status in (JobStatus.queued, JobStatus.waiting, JobStatus.paused):
                job.status = JobStatus.cancelled
                await session.commit()
                log.info(f"Job {job_id[:8]} cancelled")
                await hub.broadcast("job_status", {"job_id": job_id, "status": "cancelled"})

    async def _handle_cancel(self, job: Job):
        """Clean up after a cancelled job."""
        async with async_session() as session:
            result = await session.execute(select(Job).where(Job.id == job.id))
            db_job = result.scalar_one_or_none()
            if db_job:
                db_job.status = JobStatus.cancelled
                db_job.error_message = "Cancelled by user"
                db_job.progress = 0.0
                db_job.download_speed = 0
                await session.commit()

        # Clean up any partial files
        try:
            if job.download_path and os.path.exists(job.download_path):
                os.remove(job.download_path)
            extract_dir = str(TEMP_DIR / f"job_{job.id[:8]}")
            if os.path.exists(extract_dir):
                shutil.rmtree(extract_dir, ignore_errors=True)
        except Exception:
            pass

        await hub.broadcast("job_status", {"job_id": job.id, "status": "cancelled"})
        log.info(f"Job {job.id[:8]} cancelled and cleaned up")

    async def clear_completed(self):
        """Remove completed jobs from the list."""
        async with async_session() as session:
            result = await session.execute(
                select(Job).where(Job.status.in_([JobStatus.completed, JobStatus.cancelled]))
            )
            jobs = result.scalars().all()
            for job in jobs:
                await session.delete(job)
            await session.commit()
            log.info(f"Cleared {len(jobs)} completed/cancelled jobs")

    async def get_queue(self) -> list[dict]:
        """Get the current queue state."""
        async with async_session() as session:
            result = await session.execute(
                select(Job).order_by(Job.queue_position)
            )
            jobs = result.scalars().all()
            return [
                {
                    "id": j.id,
                    "filename": j.filename,
                    "status": j.status.value,
                    "file_size": j.file_size,
                    "progress": j.progress,
                    "created_at": j.created_at.isoformat() if j.created_at else None,
                    "started_at": j.started_at.isoformat() if j.started_at else None,
                    "completed_at": j.completed_at.isoformat() if j.completed_at else None,
                    "error_message": j.error_message,
                    "retry_count": j.retry_count,
                    "downloaded_bytes": j.downloaded_bytes,
                    "download_speed": j.download_speed,
                    "files_checked": j.files_checked,
                    "files_total": j.files_total,
                    "results_summary": j.results_summary,
                    "discord_sent": j.discord_sent,
                    "queue_position": j.queue_position,
                    "download_path": j.download_path,
                    "has_download": bool(j.download_path and os.path.exists(j.download_path)),
                }
                for j in jobs
            ]


def _format_duration(seconds: float) -> str:
    """Format seconds into a human readable duration."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours}h {mins}m {secs}s"


def _create_discord_zip(directory: str) -> Optional[bytes]:
    """Create a lightweight zip for Discord — summary + matched lines only.

    The full ``found_cookies/`` directory (which can be hundreds of MB) is
    intentionally excluded. Only the small text summaries are sent.
    Returns None if no files to send.
    """
    base = Path(directory)
    buf = io.BytesIO()
    count = 0
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name in ("scan_summary.txt", "matched_lines.txt"):
            fpath = base / name
            if fpath.exists():
                zf.write(fpath, name)
                count += 1
    if count == 0:
        return None
    data = buf.getvalue()
    log.info(f"Discord zip: {count} file(s), {len(data)} bytes")
    return data


# Global singleton
queue_manager = QueueManager()
