"""Jobs placed by the server in ``jobs.db`` and run by separate worker processes.

See ``docs/Feature Development/Job Workers Plan.md`` for the design and the
contracts. ``protocol`` and ``kinds`` are importable by both the server and
the worker; everything else in this package is server-side.
"""
