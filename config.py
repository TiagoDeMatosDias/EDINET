import os

from dotenv import load_dotenv

from src.paths import app_dir


class Config:
    def get(self, key, default=None):
        """Get a config value from settings or environment variables."""
        return self.settings.get(key, os.getenv(key, default))

    def resolve_db_path(self, db_value: str | None) -> str | None:
        """Resolve a user-provided database identifier into a filesystem path.

        Delegates to the unified resolver in ``src.orchestrator.common.db_config``.
        """
        from src.orchestrator.common.db_config import resolve_db_path as _resolve_db_path
        return _resolve_db_path(db_value)

    @classmethod
    def from_dict(cls, settings: dict) -> "Config":
        """Create a Config instance from a dict.

        All configuration must be supplied explicitly; no file is read.
        """
        load_dotenv(app_dir() / ".env")
        instance = object.__new__(cls)
        instance.settings = dict(settings)
        return instance

    @classmethod
    def reset(cls) -> None:
        """No-op. Retained for test compatibility."""
