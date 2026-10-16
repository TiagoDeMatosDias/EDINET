"""Job worker: the separate process (``ShadeWorker``) that runs jobs for the server.

See ``docs/Feature Development/Job Workers Plan.md``. The package is kept free of
heavy imports; handlers and their dependencies load only inside job children.
"""
