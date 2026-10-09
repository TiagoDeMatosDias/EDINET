class Config:
    """The settings of one pipeline run, as submitted by the client.

    Operator settings such as the EDINET API key are not part of a pipeline's
    configuration; steps read them from ``src.settings``.
    """

    def get(self, key, default=None):
        """Get a pipeline setting."""
        return self.settings.get(key, default)

    @classmethod
    def from_dict(cls, settings: dict) -> "Config":
        """Create a Config instance from a dict."""
        instance = object.__new__(cls)
        instance.settings = dict(settings)
        return instance

    @classmethod
    def reset(cls) -> None:
        """No-op. Retained for test compatibility."""
