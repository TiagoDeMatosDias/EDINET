"""Bounded, path-safe storage for immutable EDINET ZIP artifacts."""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath


@dataclass(frozen=True)
class ArchivePolicy:
    max_members: int = 20_000
    max_member_bytes: int = 100 * 1024 * 1024
    max_total_bytes: int = 512 * 1024 * 1024


DEFAULT_ARCHIVE_POLICY = ArchivePolicy()


class UnsafeArchiveError(ValueError):
    """Raised when a submitted archive exceeds safe extraction boundaries."""


def _safe_member(name: str) -> str:
    normalized = name.replace("\\", "/")
    raw_parts = normalized.split("/")
    if (
        normalized.startswith("/")
        or any(part in {".", ".."} for part in raw_parts)
        or any(not part for part in raw_parts[:-1])
        or (raw_parts and ":" in raw_parts[0])
    ):
        raise UnsafeArchiveError(f"Unsafe ZIP member path: {name}")
    path = PurePosixPath(normalized)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise UnsafeArchiveError(f"Unsafe ZIP member path: {name}")
    if len(normalized) > 260:
        raise UnsafeArchiveError("ZIP member path is too long")
    return "/".join(path.parts)


def validate_zip_in_memory(content: bytes, policy: ArchivePolicy = DEFAULT_ARCHIVE_POLICY) -> list[zipfile.ZipInfo]:
    """Validate ZIP metadata from raw bytes (no disk I/O)."""
    import io

    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        infos = archive.infolist()
        if len(infos) > policy.max_members:
            raise UnsafeArchiveError("ZIP contains too many members")
        total = 0
        seen_names: set[str] = set()
        for info in infos:
            member_name = _safe_member(info.filename)
            if member_name in seen_names:
                raise UnsafeArchiveError(f"ZIP contains duplicate member: {member_name}")
            seen_names.add(member_name)
            file_mode = (info.external_attr >> 16) & 0o170000
            if file_mode == 0o120000:
                raise UnsafeArchiveError("ZIP symlinks are not allowed")
            if info.file_size > policy.max_member_bytes:
                raise UnsafeArchiveError(f"ZIP member exceeds size limit: {member_name}")
            total += info.file_size
            if total > policy.max_total_bytes:
                raise UnsafeArchiveError("ZIP total declared size exceeds limit")
        return infos


def validate_zip(path: str | Path, policy: ArchivePolicy = DEFAULT_ARCHIVE_POLICY) -> list[zipfile.ZipInfo]:
    """Validate ZIP metadata without extracting any member (from disk path)."""
    content = Path(path).read_bytes()
    return validate_zip_in_memory(content, policy)


def archive_zip(
    content: bytes,
    doc_id: str,
    root: str | Path,
    policy: ArchivePolicy = DEFAULT_ARCHIVE_POLICY,
) -> tuple[Path, str, int]:
    """Validate and atomically persist a ZIP under a document-specific path."""
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", str(doc_id)):
        raise UnsafeArchiveError("Document ID contains unsafe characters")
    if not content or len(content) > policy.max_total_bytes:
        raise UnsafeArchiveError("ZIP payload exceeds the size limit")
    raw_root = Path(root).expanduser()
    if raw_root.exists() and raw_root.is_symlink():
        raise UnsafeArchiveError("Archive root must not be a symlink")
    root_path = raw_root.resolve()
    document_root = root_path / doc_id
    if document_root.exists() and document_root.is_symlink():
        raise UnsafeArchiveError("Document archive directory must not be a symlink")
    if document_root.resolve(strict=False).parent != root_path:
        raise UnsafeArchiveError("Document archive directory escapes the archive root")
    document_root.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(content).hexdigest()
    target = document_root / f"type-1-{digest}.zip"
    fd, temporary = tempfile.mkstemp(prefix=f"{doc_id}.", suffix=".partial", dir=document_root)
    os.close(fd)
    temporary_path = Path(temporary)
    try:
        temporary_path.write_bytes(content)
        validate_zip(temporary_path, policy)
        os.replace(temporary_path, target)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()
    return target, digest, len(content)
