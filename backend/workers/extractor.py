"""Archive extraction supporting ZIP, RAR, 7Z, TAR/GZ."""

import os
import shutil
import tarfile
import zipfile
from pathlib import Path
from typing import Optional

from backend.logging_config import ComponentLogger

log = ComponentLogger("Extractor")


async def extract_archive(archive_path: str, dest_dir: str) -> int:
    """
    Extract an archive to dest_dir. Returns number of files extracted.
    Supports: ZIP, RAR, 7Z, TAR, TAR.GZ, TAR.BZ2, TAR.XZ
    """
    import asyncio

    os.makedirs(dest_dir, exist_ok=True)
    path = Path(archive_path)
    lower = path.name.lower()

    try:
        if lower.endswith(".zip"):
            return await asyncio.to_thread(_extract_zip, archive_path, dest_dir)
        elif lower.endswith(".rar"):
            return await asyncio.to_thread(_extract_rar, archive_path, dest_dir)
        elif lower.endswith(".7z"):
            return await asyncio.to_thread(_extract_7z, archive_path, dest_dir)
        elif any(lower.endswith(ext) for ext in (".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tar.xz")):
            return await asyncio.to_thread(_extract_tar, archive_path, dest_dir)
        else:
            # Try ZIP first, then treat as a single file
            try:
                return await asyncio.to_thread(_extract_zip, archive_path, dest_dir)
            except zipfile.BadZipFile:
                # Not a zip — copy it as-is
                dest = os.path.join(dest_dir, path.name)
                shutil.copy2(archive_path, dest)
                return 1

    except Exception as e:
        log.error(f"Extraction failed for {path.name}: {e}")
        raise RuntimeError(f"Failed to extract {path.name}: {e}")


def _extract_zip(archive_path: str, dest_dir: str) -> int:
    """Extract a ZIP archive."""
    count = 0
    with zipfile.ZipFile(archive_path, "r") as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            # Sanitize path to prevent traversal
            safe_path = _safe_path(info.filename)
            if not safe_path:
                continue
            target = os.path.join(dest_dir, safe_path)
            os.makedirs(os.path.dirname(target), exist_ok=True)
            with zf.open(info) as src, open(target, "wb") as dst:
                shutil.copyfileobj(src, dst)
            count += 1
    log.info(f"ZIP extracted: {count} files from {os.path.basename(archive_path)}")
    return count


def _extract_rar(archive_path: str, dest_dir: str) -> int:
    """Extract a RAR archive using rarfile."""
    try:
        import rarfile
    except ImportError:
        # Fall back to unrar command
        return _extract_rar_cli(archive_path, dest_dir)

    count = 0
    with rarfile.RarFile(archive_path, "r") as rf:
        for info in rf.infolist():
            if info.is_dir():
                continue
            safe_path = _safe_path(info.filename)
            if not safe_path:
                continue
            target = os.path.join(dest_dir, safe_path)
            os.makedirs(os.path.dirname(target), exist_ok=True)
            with rf.open(info) as src, open(target, "wb") as dst:
                shutil.copyfileobj(src, dst)
            count += 1
    log.info(f"RAR extracted: {count} files from {os.path.basename(archive_path)}")
    return count


def _extract_rar_cli(archive_path: str, dest_dir: str) -> int:
    """Extract RAR using the unrar command-line tool."""
    import subprocess
    result = subprocess.run(
        ["unrar", "x", "-o+", "-y", archive_path, dest_dir + "/"],
        capture_output=True, text=True, timeout=600,
    )
    if result.returncode != 0:
        raise RuntimeError(f"unrar failed: {result.stderr[:200]}")

    count = sum(1 for _ in Path(dest_dir).rglob("*") if _.is_file())
    log.info(f"RAR (cli) extracted: {count} files from {os.path.basename(archive_path)}")
    return count


def _extract_7z(archive_path: str, dest_dir: str) -> int:
    """Extract a 7z archive."""
    try:
        import py7zr
        count = 0
        with py7zr.SevenZipFile(archive_path, "r") as zf:
            zf.extractall(path=dest_dir)
        count = sum(1 for _ in Path(dest_dir).rglob("*") if _.is_file())
        log.info(f"7Z extracted: {count} files from {os.path.basename(archive_path)}")
        return count
    except ImportError:
        # Fall back to 7z command-line tool
        import subprocess
        result = subprocess.run(
            ["7z", "x", f"-o{dest_dir}", "-y", archive_path],
            capture_output=True, text=True, timeout=600,
        )
        if result.returncode != 0:
            raise RuntimeError(f"7z failed: {result.stderr[:200]}")

        count = sum(1 for _ in Path(dest_dir).rglob("*") if _.is_file())
        log.info(f"7Z (cli) extracted: {count} files from {os.path.basename(archive_path)}")
        return count


def _extract_tar(archive_path: str, dest_dir: str) -> int:
    """Extract a TAR archive (including .tar.gz, .tar.bz2, .tar.xz)."""
    count = 0
    with tarfile.open(archive_path, "r:*") as tf:
        for member in tf.getmembers():
            if not member.isfile():
                continue
            safe_path = _safe_path(member.name)
            if not safe_path:
                continue
            member.name = safe_path
            tf.extract(member, dest_dir, filter="data")
            count += 1
    log.info(f"TAR extracted: {count} files from {os.path.basename(archive_path)}")
    return count


def _safe_path(filename: str) -> Optional[str]:
    """Sanitize a path to prevent directory traversal attacks."""
    # Normalize separators
    clean = filename.replace("\\", "/")

    # Remove leading slashes and .. components
    parts = []
    for part in clean.split("/"):
        if part in ("", ".", ".."):
            continue
        parts.append(part)

    if not parts:
        return None

    return os.path.join(*parts)
