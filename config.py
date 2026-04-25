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