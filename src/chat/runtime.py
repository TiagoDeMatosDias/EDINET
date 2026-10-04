"""Process-wide chat store, cipher, and notifier."""

from __future__ import annotations

import os
from pathlib import Path

from src.orchestrator.common.db_config import get_chat_db

from .crypto import ChannelCipher
from .storage import ChatStore

CHAT_DB_PATH = Path(os.getenv("EDINET_CHAT_DB") or get_chat_db()).expanduser()
store = ChatStore(CHAT_DB_PATH)
cipher = ChannelCipher()
