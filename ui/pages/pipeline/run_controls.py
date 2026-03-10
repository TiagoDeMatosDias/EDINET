import logging
import sys

import flet as ft


def create_run_controls(
    page: ft.Page,
    *,
    is_running: list[bool],
    base_dir,
    current_config,
    save_run_config,
):
    """Create output controls + Run button and wire execution behavior."""
    log_output = ft.TextField(
        value="",
        multiline=True,
        read_only=True,
        min_lines=4,
        max_lines=8,
        expand=True,
        text_size=11,
        dense=True,
        border_color=ft.Colors.TRANSPARENT,
        filled=True,
    )

    progress = ft.ProgressBar(visible=False, value=0)

    class _UILogHandler(logging.Handler):
        def emit(self, record):
            try:
                msg = self.format(record)
                log_output.value = (log_output.value or "") + msg + "\n"
                page.update()
            except Exception:
                pass

    def on_run(_):
        if is_running[0]:
            return
        is_running[0] = True
        run_btn.disabled = True
        progress.visible = True
        progress.value = None  # Indeterminate progress
        log_output.value = ""
        page.update()

        save_run_config(current_config())

        def _do_run():
            handler = _UILogHandler()
            handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s  %(levelname)s  %(message)s",
                    datefmt="%H:%M:%S",
                )
            )
            try:
                proj = str(base_dir)
                if proj not in sys.path:
                    sys.path.insert(0, proj)

                from config import Config
                Config._instance = None

                from src.logger import setup_logging
                setup_logging()

                # Add UI handler AFTER setup_logging, because
                # setup_logging removes all existing handlers.
                root_logger = logging.getLogger()
                root_logger.addHandler(handler)
                handler.setLevel(logging.INFO)

                import src.orchestrator as orchestrator
                orchestrator.run()

                log_output.value = (log_output.value or "") + "\n✅ Run completed successfully!\n"
            except Exception as ex:
                log_output.value = (log_output.value or "") + f"\n❌ Error: {ex}\n"
            finally:
                try:
                    logging.getLogger().removeHandler(handler)
                except Exception:
                    pass
                is_running[0] = False
                run_btn.disabled = False
                progress.visible = False
                progress.value = 0
                page.update()

        page.run_thread(_do_run)

    run_btn = ft.Button(
        "Run",
        icon=ft.Icons.PLAY_ARROW,
        color=ft.Colors.WHITE,
        bgcolor=ft.Colors.GREEN_700,
        style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
        height=44,
        on_click=on_run,
    )

    return log_output, progress, run_btn
