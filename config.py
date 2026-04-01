import json
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
    _instance = None  # Singleton instance

    def __new__(cls, run_config_path=None):
        if cls._instance is None:
            if run_config_path is None:
                run_config_path = os.path.join(_base_dir(), "config", "state", "run_config.json")
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance.run_config_path = run_config_path
            cls._instance._load_config()
        return cls._instance

    def _load_config(self):
        """Loads configuration from .env and JSON files."""
        base = _base_dir()
        load_dotenv(os.path.join(base, ".env"))
        self.settings = {}

        try:
            with open(self.run_config_path, "r") as file:
                self.settings.update(json.load(file))
        except FileNotFoundError:
            print(f"Warning: {self.run_config_path} not found.")


    def get(self, key, default=None):
        """Get a config value from settings or environment variables."""
        return self.settings.get(key, os.getenv(key, default))

    @classmethod
    def from_dict(cls, settings: dict) -> "Config":
        """Create a Config instance from a dict without touching disk.

        This bypasses the singleton pattern and does not write or read any file.
        Useful for UI-driven runs where the config lives in memory.
        """
        instance = object.__new__(cls)
        instance.settings = dict(settings)
        instance.run_config_path = None
        return instance

    @classmethod
    def reset(cls):
        """Clear the singleton so the next ``Config()`` call reloads from disk."""
        cls._instance = None