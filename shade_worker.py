"""Entry point of the ShadeWorker program (``python shade_worker.py ...``).

``freeze_support`` must run first: in a PyInstaller build every spawned job child
starts by re-running this executable.
"""

import multiprocessing

if __name__ == "__main__":
    multiprocessing.freeze_support()
    from src.worker.cli import main

    raise SystemExit(main())
