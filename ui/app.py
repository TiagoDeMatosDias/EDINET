"""UI application bootstrap and page registry."""

from dataclasses import dataclass
from typing import Callable

import flet as ft

from ui.pages.pipeline.page import build_pipeline_page
from ui.pages.pipeline.persistence import ASSETS_DIR


@dataclass(frozen=True)
class PageDefinition:
    key: str
    title: str
    build: Callable[[ft.Page], None]


PAGE_REGISTRY: dict[str, PageDefinition] = {
    "pipeline": PageDefinition(
        key="pipeline",
        title="Financial Data Pipeline",
        build=build_pipeline_page,
    ),
}

DEFAULT_PAGE_KEY = "pipeline"


def open_page(page: ft.Page, page_key: str = DEFAULT_PAGE_KEY):
    page.appbar = None
    page.overlay.clear()
    page.services.clear()
    page.clean()
    PAGE_REGISTRY[page_key].build(page)


def main(page: ft.Page):
    open_page(page, DEFAULT_PAGE_KEY)


def launch():
    """Start the EDINET GUI."""
    ft.run(main, assets_dir=str(ASSETS_DIR))


if __name__ == "__main__":
    launch()
