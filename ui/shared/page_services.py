from dataclasses import dataclass
from typing import Callable

import flet as ft


@dataclass
class PageServices:
    snack: Callable[[str], None]
    show: Callable[[ft.AlertDialog], None]
    pop: Callable[[], None]


def create_page_services(page: ft.Page) -> PageServices:
    snack_bar = ft.SnackBar(content=ft.Text(""), open=False)
    page.overlay.append(snack_bar)

    def snack(message: str):
        snack_bar.content = ft.Text(message)
        snack_bar.open = True
        page.update()

    def show(dialog: ft.AlertDialog):
        page.show_dialog(dialog)

    def pop():
        page.pop_dialog()

    return PageServices(snack=snack, show=show, pop=pop)
