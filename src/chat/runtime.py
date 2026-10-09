"""Process-wide chat store, cipher, and notifier."""

from __future__ import annotations

from pathlib import Path

from src.orchestrator.common.db_config import get_chat_db

from .crypto import ChannelCipher
from .storage import ChatStore

CHAT_DB_PATH = Path(get_chat_db())
store = ChatStore(CHAT_DB_PATH)
cipher = ChannelCipher()
