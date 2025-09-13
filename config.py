import json
import os
from dotenv import load_dotenv

class Config:
    _instance = None  # Singleton instance

    def __new__(cls, run_config_path="config/run_config.json"):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance.run_config_path = run_config_path
            cls._instance._load_config()
        return cls._instance

    def _load_config(self):
        """Loads configuration from .env and JSON files."""
        load_dotenv()
        self.settings = {}
        try:
            with open("config/app_config.json", "r") as file:
                self.settings.update(json.load(file))
        except FileNotFoundError:
            print("Warning: app_config.json not found.")

        try:
            with open(self.run_config_path, "r") as file:
                self.settings.update(json.load(file))
        except FileNotFoundError:
            print(f"Warning: {self.run_config_path} not found.")

    def get(self, key, default=None):
        """Get a config value from settings or environment variables."""
        return self.settings.get(key, os.getenv(key, default))