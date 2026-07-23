"""Web-based EDINET workstation frontend."""

__all__ = ["app"]


def __getattr__(name: str):
    """Load the assembled application only when callers request it."""
    if name == "app":
        from .server import app

        return app
    raise AttributeError(name)
