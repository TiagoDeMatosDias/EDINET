import sys


def _run_cli():
    """Original headless / CLI execution path."""
    import logging
    import src.orchestrator as o
    from src.logger import setup_logging

    logger, log_path = setup_logging()
    logger.info("=" * 80)
    logger.info("APPLICATION START")
    logger.info("=" * 80)

    try:
        o.run()
        logger.info("=" * 80)
        logger.info("APPLICATION COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
    except Exception as e:
        logger.error(f"Application failed with error: {e}", exc_info=True)
        raise


def _run_gui():
    """Launch the Tkinter terminal-style GUI."""
    from src.logger import setup_logging
    setup_logging()
    from ui_tk import run_tk_app
    run_tk_app()


def _run_flet():
    """Launch the legacy Flet GUI (deprecated)."""
    try:
        from ui.app import launch
        launch()
    except ImportError as exc:
        print(f"Could not start Flet GUI: {exc}")
        print("Install the dependency with:  pip install flet")
        print("Or run the default Tk UI:     python main.py")
        sys.exit(1)


if __name__ == '__main__':
    if '--cli' in sys.argv:
        _run_cli()
    elif '--flet' in sys.argv:
        _run_flet()
    else:
        _run_gui()