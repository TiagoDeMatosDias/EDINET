import json

class Config:
    _instance = None  # Singleton instance

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance._load_config()
        return cls._instance

    def _load_config(self):
        """Loads configuration from a JSON file."""
        try:
            with open("config.json", "r") as file:
                self.settings = json.load(file)
        except FileNotFoundError:
            self.settings = {}  # Default to empty if file not found

    def get(self, key, default=None):
        """Get a config value with an optional default."""
        return self.settings.get(key, default)
