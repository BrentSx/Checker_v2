"""Archive extraction supporting ZIP, RAR, 7Z, TAR/GZ."""

import os
import shutil
import tarfile
import zipfile
from pathlib import Path
from typing import Optional

from backend.logging_config import ComponentLogger

log = ComponentLogger("Extractor")


async def extract_archive(archive_path: str, dest_dir: str, password: Optional[str] = None) -> int:
    """
    Extract an archive to dest_dir. Returns number of files extracted.
    Supports: ZIP, RAR, 7Z, TAR, TAR.GZ, TAR.BZ2, TAR.XZ

    If *password* is provided it will be used to decrypt the archive.
    """
    import asyncio

    os.makedirs(dest_dir, exist_ok=True)
    path = Path(archive_path)
    lower = path.name.lower()

    if password:
        log.info(f"Using password for extraction of {path.name}")

    try:
        if lower.endswith(".zip"):
            return await asyncio.to_thread(_extract_zip, archive_path, dest_dir, password)
        elif lower.endswith(".rar"):
            return await asyncio.to_thread(_extract_rar, archive_path, dest_dir, password)
        elif lower.endswith(".7z"):
            return await asyncio.to_thread(_extract_7z, archive_path, dest_dir, password)
        elif any(lower.endswith(ext) for ext in (".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tar.xz")):
            return await asyncio.to_thread(_extract_tar, archive_path, dest_dir)
        else:
            # Try ZIP first, then treat as a single file
            try:
                return await asyncio.to_thread(_extract_zip, archive_path, dest_dir, password)
            except zipfile.BadZipFile:
                # Not a zip — copy it as-is
                dest = os.path.join(dest_dir, path.name)
                shutil.copy2(archive_path, dest)
                return 1

    except Exception as e:
        log.error(f"Extraction failed for {path.name}: {e}")
        raise RuntimeError(f"Failed to extract {path.name}: {e}")


def _extract_zip(archive_path: str, dest_dir: str, password: Optional[str] = None) -> int:
    """Extract a ZIP archive, optionally with a password.

    Falls back to CLI ``7z`` or ``unzip`` when Python's built-in zipfile
    cannot handle the compression method (Deflate64, Zstandard, etc.).
    """
    pwd_bytes = password.encode("utf-8") if password else None
    count = 0
    try:
        with zipfile.ZipFile(archive_path, "r") as zf:
            if pwd_bytes:
                zf.setpassword(pwd_bytes)
            for info in zf.infolist():
                if info.is_dir():
                    continue
                safe_path = _safe_path(info.filename)
                if not safe_path:
                    continue
                target = os.path.join(dest_dir, safe_path)
                os.makedirs(os.path.dirname(target), exist_ok=True)
                with zf.open(info, pwd=pwd_bytes) as src, open(target, "wb") as dst:
                    shutil.copyfileobj(src, dst)
                count += 1
        log.info(f"ZIP extracted: {count} files from {os.path.basename(archive_path)}")
        return count
    except NotImplementedError:
        # Python's zipfile doesn't support this compression (Deflate64, Zstandard, etc.)
        log.warning(
            f"Python zipfile can't handle compression in {os.path.basename(archive_path)}, "
            "falling back to CLI..."
        )
        return _extract_zip_cli(archive_path, dest_dir, password)


def _extract_zip_cli(archive_path: str, dest_dir: str, password: Optional[str] = None) -> int:
    """Extract a ZIP using CLI tools (7z first, then unzip)."""
    import subprocess

    basename = os.path.basename(archive_path)

    # Try each CLI tool in preference order
    for tool_cmd in _zip_cli_commands(archive_path, dest_dir, password):
        tool_name = tool_cmd[0]
        try:
            result = subprocess.run(
                tool_cmd, capture_output=True, text=True, timeout=7200,
            )
            if result.returncode == 0:
                count = sum(1 for _ in Path(dest_dir).rglob("*") if _.is_file())
                log.info(f"ZIP ({tool_name}) extracted: {count} files from {basename}")
                return count
            else:
                log.warning(f"{tool_name} failed (rc={result.returncode}): {result.stderr[:200]}")
        except FileNotFoundError:
            log.info(f"{tool_name} not found, trying next tool...")
        except Exception as e:
            log.warning(f"{tool_name} error: {e}")

    raise RuntimeError(
        f"No CLI tool could extract {basename}. "
        "Install 7z or unzip on the server."
    )


def _zip_cli_commands(archive_path: str, dest_dir: str, password: Optional[str] = None):
    """Yield CLI commands to try for ZIP extraction, in preference order."""
    # 7z — handles Deflate64, LZMA, Zstandard, etc.
    cmd_7z = ["7z", "x", f"-o{dest_dir}", "-y"]
    if password:
        cmd_7z.append(f"-p{password}")
    cmd_7z.append(archive_path)
    yield cmd_7z

    # unzip — handles most standard ZIPs
    cmd_unzip = ["unzip", "-o", "-q"]
    if password:
        cmd_unzip.extend(["-P", password])
    cmd_unzip.extend([archive_path, "-d", dest_dir])
    yield cmd_unzip


def _extract_rar(archive_path: str, dest_dir: str, password: Optional[str] = None) -> int:
    """Extract a RAR archive.

    For multi-volume archives (.partNN.rar / .rNN) we go straight to the CLI
    ``unrar`` because the Python ``rarfile`` library frequently chokes on them.
    Single-volume archives try ``rarfile`` first, then fall back to CLI.
    """
    import re
    basename = os.path.basename(archive_path).lower()
    is_multivolume = bool(re.search(r"\.part\d+\.rar$", basename)) or bool(
        re.search(r"\.r\d{2,}$", basename)
    )

    if is_multivolume:
        log.info(f"Multi-volume RAR detected — using CLI unrar for {os.path.basename(archive_path)}")
        return _extract_rar_cli(archive_path, dest_dir, password)

    # Single-volume: try Python rarfile first, fall back to CLI
    try:
        import rarfile
    except ImportError:
        return _extract_rar_cli(archive_path, dest_dir, password)

    try:
        count = 0
        with rarfile.RarFile(archive_path, "r") as rf:
            if password:
                rf.setpassword(password)
            for info in rf.infolist():
                if info.is_dir():
                    continue
                safe_path = _safe_path(info.filename)
                if not safe_path:
                    continue
                target = os.path.join(dest_dir, safe_path)
                os.makedirs(os.path.dirname(target), exist_ok=True)
                with rf.open(info, pwd=password) as src, open(target, "wb") as dst:
                    shutil.copyfileobj(src, dst)
                count += 1
        log.info(f"RAR extracted: {count} files from {os.path.basename(archive_path)}")
        return count
    except Exception as e:
        log.warning(f"Python rarfile failed ({e}), falling back to CLI unrar...")
        return _extract_rar_cli(archive_path, dest_dir, password)


def _extract_rar_cli(archive_path: str, dest_dir: str, password: Optional[str] = None) -> int:
    """Extract RAR using the unrar command-line tool."""
    import subprocess
    cmd = ["unrar", "x", "-o+", "-y"]
    if password:
        cmd.append(f"-p{password}")
    else:
        cmd.append("-p-")  # no password prompt
    cmd.extend([archive_path, dest_dir + "/"])
    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=7200,  # 2 hours for multi-volume
    )
    if result.returncode != 0:
        raise RuntimeError(f"unrar failed: {result.stderr[:200]}")

    count = sum(1 for _ in Path(dest_dir).rglob("*") if _.is_file())
    log.info(f"RAR (cli) extracted: {count} files from {os.path.basename(archive_path)}")
    return count


def _extract_7z(archive_path: str, dest_dir: str, password: Optional[str] = None) -> int:
    """Extract a 7z archive."""
    try:
        import py7zr
        kwargs = {}
        if password:
            kwargs["password"] = password
        with py7zr.SevenZipFile(archive_path, "r", **kwargs) as zf:
            zf.extractall(path=dest_dir)
        count = sum(1 for _ in Path(dest_dir).rglob("*") if _.is_file())
        log.info(f"7Z extracted: {count} files from {os.path.basename(archive_path)}")
        return count
    except ImportError:
        # Fall back to 7z command-line tool
        import subprocess
        cmd = ["7z", "x", f"-o{dest_dir}", "-y"]
        if password:
            cmd.append(f"-p{password}")
        else:
            cmd.append("-p")  # don't prompt
        cmd.append(archive_path)
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=7200,
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
