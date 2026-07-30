"""Bounded EDINET type-1 acquisition; the provider token is read only here."""

from __future__ import annotations

import json
import logging
import os
import threading
from typing import Any

import requests  # type: ignore[import-untyped]

from .archive import DEFAULT_ARCHIVE_POLICY, ArchivePolicy
from .catalog import FilingCatalog

logger = logging.getLogger(__name__)
EDINET_DOCUMENTS_URL = "https://api.edinet-fsa.go.jp/api/v2/documents"


class EdinetAcquisitionError(RuntimeError):
    """Raised when EDINET cannot provide a valid document package."""


class EdinetDownloadClient:
    """Download EDINET data with explicit timeouts and a provider-only key."""

    def __init__(
        self,
        provider_token: str,
        *,
        base_url: str = EDINET_DOCUMENTS_URL,
        timeout: tuple[float, float] = (10.0, 60.0),
        max_bytes: int = 512 * 1024 * 1024,
    ) -> None:
        if not provider_token.strip():
            raise ValueError("EDINET provider token is required for acquisition")
        self.provider_token = provider_token
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_bytes = max_bytes
        self._thread_local = threading.local()
        self._sessions: dict[int, requests.Session] = {}
        self._sessions_lock = threading.Lock()

    @classmethod
    def from_environment(cls) -> "EdinetDownloadClient":
        """Build the acquisition client from the provider-only environment key."""
        token = os.getenv("EDINET_API_TOKEN", "")
        return cls(token)

    def download_type1(self, doc_id: str) -> bytes:
        """Download one submitted document ZIP without logging credentials or URLs."""
        response = self._session().get(
            f"{self.base_url}/{doc_id}",
            params={"type": "1", "Subscription-Key": self.provider_token},
            timeout=self.timeout,
            stream=True,
        )
        try:
            if response.status_code != 200:
                raise EdinetAcquisitionError(f"EDINET returned HTTP {response.status_code}")
            declared = response.headers.get("Content-Length")
            if declared:
                try:
                    if int(declared) > self.max_bytes:
                        raise EdinetAcquisitionError("EDINET document exceeds the configured size limit")
                except ValueError:
                    logger.debug("Ignoring invalid EDINET Content-Length for %s", doc_id)
            content = self._read_bounded(response)
            if self._looks_like_json(content):
                try:
                    payload = json.loads(content.decode("utf-8", errors="replace"))
                except json.JSONDecodeError:
                    payload = {}
                message = payload.get("message") if isinstance(payload, dict) else None
                raise EdinetAcquisitionError(str(message or "EDINET returned an error payload"))
            if not content.startswith(b"PK"):
                raise EdinetAcquisitionError("EDINET response is not a ZIP archive")
            return content
        finally:
            close = getattr(response, "close", None)
            if callable(close):
                close()

    def _session(self) -> requests.Session:
        """Return one persistent HTTP session for the current worker thread."""
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = requests.Session()
            self._thread_local.session = session
            with self._sessions_lock:
                self._sessions[threading.get_ident()] = session
        return session

    def close(self) -> None:
        """Close all sessions created by this client."""
        with self._sessions_lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()
        for session in sessions:
            session.close()
        self._thread_local.session = None

    def _read_bounded(self, response: requests.Response) -> bytes:
        iterator_factory = getattr(response, "iter_content", None)
        if not callable(iterator_factory):
            content = bytes(getattr(response, "content", b""))
            if len(content) > self.max_bytes:
                raise EdinetAcquisitionError("EDINET document exceeds the configured size limit")
            return content
        chunks: list[bytes] = []
        size = 0
        iterator = iterator_factory(chunk_size=1024 * 1024)
        for chunk in iterator:
            if not chunk:
                continue
            size += len(chunk)
            if size > self.max_bytes:
                raise EdinetAcquisitionError("EDINET document exceeds the configured size limit")
            chunks.append(chunk)
        return b"".join(chunks)

    @staticmethod
    def _looks_like_json(content: bytes) -> bool:
        return content.lstrip().startswith((b"{", b"["))

    def acquire_type1(
        self,
        doc_id: str,
        catalog: FilingCatalog,
        metadata: dict[str, Any] | None = None,
        *,
        policy: ArchivePolicy = DEFAULT_ARCHIVE_POLICY,
    ) -> int:
        """Download, validate, and index one document directly into the catalog."""
        content = self.download_type1(doc_id)
        return self.ingest_type1(doc_id, content, catalog, metadata, policy=policy)

    def ingest_type1(
        self,
        doc_id: str,
        content: bytes,
        catalog: FilingCatalog,
        metadata: dict[str, Any] | None = None,
        *,
        policy: ArchivePolicy = DEFAULT_ARCHIVE_POLICY,
    ) -> int:
        """Index a previously downloaded type-1 ZIP into the catalog."""
        from .ingest import ingest_content

        return ingest_content(content, doc_id, catalog, metadata, policy=policy)
