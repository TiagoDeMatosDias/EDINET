import os
import sys
from dotenv import load_dotenv


def _base_dir() -> str:
    """Return the root directory used to resolve config-relative paths.

    - PyInstaller frozen exe: the folder that contains the exe, so the user
      can place the ``config/`` folder and ``.env`` file next to it.
    - Plain Python script: the project root (the folder containing this file).
    """
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


class Config:
    def get(self, key, default=None):
        """Get a config value from settings or environment variables."""
        return self.settings.get(key, os.getenv(key, default))

    def resolve_db_path(self, db_value: str | None) -> str | None:
        """Resolve a user-provided database identifier into a filesystem path.

        Behaviour:
        - If ``db_value`` is falsy, return it unchanged.
        - If ``db_value`` is an absolute path, return it unchanged.
        - If ``db_value`` contains a path separator, return its absolute path.
        - Otherwise treat ``db_value`` as a filename and attempt to locate it
          relative to the configured ``DB_PATH`` (from settings or environment).
          If ``DB_PATH`` points to a file, the filename's dirname is used; if
          it points to a directory that exists, that directory is used. When
          no ``DB_PATH`` is available, the filename is resolved relative to
          the current working directory.
        """
        if not db_value:
            return db_value
        raw = str(db_value).strip().strip("\"'")
        # Absolute path => return as-is
        if os.path.isabs(raw):
            return raw
        # If it contains a path separator, resolve relative to cwd
        if ("/" in raw) or ("\\" in raw):
            return os.path.abspath(raw)

        # Bare filename: try to derive a directory from DB_PATH
        db_path_setting = self.settings.get("DB_PATH") or os.getenv("DB_PATH")
        if db_path_setting:
            db_path_setting = str(db_path_setting).strip().strip("\"'")
            # If DB_PATH is an existing directory, join against it
            if os.path.isdir(db_path_setting):
                return os.path.abspath(os.path.join(db_path_setting, raw))
            # If DB_PATH looks like a file path, use its dirname
            base = os.path.dirname(db_path_setting) or os.getcwd()
            return os.path.abspath(os.path.join(base, raw))

        # Fallback: resolve relative to current working directory
        return os.path.abspath(raw)

    @classmethod
    def from_dict(cls, settings: dict) -> "Config":
        """Create a Config instance from a dict.

        All configuration must be supplied explicitly; no file is read.
        """
        load_dotenv(os.path.join(_base_dir(), ".env"))
        instance = object.__new__(cls)
        instance.settings = dict(settings)
        return instance

    @classmethod
    def reset(cls) -> None:
        """No-op. Retained for test compatibility."""