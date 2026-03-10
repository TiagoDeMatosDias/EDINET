import copy
from typing import Callable

import flet as ft

from ui.pages.pipeline.persistence import DEFAULT_STEP_CONFIGS, STEP_DISPLAY
from ui.shared.dialog_fields import build_fields, read_fields


def open_generic_step_config(
    page: ft.Page,
    step_name: str,
    step_configs: dict[str, dict],
    snack: Callable[[str], None],
    show: Callable[[ft.AlertDialog], None],
    pop: Callable[[], None],
):
    current = step_configs.get(step_name, {})
    if not current:
        current = copy.deepcopy(DEFAULT_STEP_CONFIGS.get(step_name, {}))
    if not current:
        snack(f"No configuration for {STEP_DISPLAY.get(step_name, step_name)}")
        return
    fields = build_fields(current)

    def save(_):
        step_configs[step_name] = read_fields(fields, current)
        pop()
        snack(f"Config for '{STEP_DISPLAY.get(step_name, step_name)}' updated")

    show(ft.AlertDialog(
        modal=True,
        title=ft.Text(f"Configure: {STEP_DISPLAY.get(step_name, step_name)}"),
        content=ft.Column(
            [ctrl for _, ctrl in fields],
            scroll=ft.ScrollMode.AUTO,
            width=500,
            height=400,
        ),
        actions=[
            ft.TextButton("Cancel", on_click=lambda _: pop()),
            ft.Button("Save", on_click=save),
        ],
    ))


def open_import_csv_config(
    page: ft.Page,
    fp: ft.FilePicker,
    step_configs: dict[str, dict],
    snack: Callable[[str], None],
    show: Callable[[ft.AlertDialog], None],
    pop: Callable[[], None],
):
    current = step_configs.get("import_stock_prices_csv", {})
    if not current:
        current = copy.deepcopy(DEFAULT_STEP_CONFIGS.get("import_stock_prices_csv", {}))

    csv_path_tf = ft.TextField(
        label="CSV File Path",
        value=current.get("csv_file", ""),
        dense=True,
        width=380,
        read_only=True,
    )

    async def _pick_csv(_):
        files = await fp.pick_files(
            dialog_title="Select stock-price CSV file",
            file_type=ft.FilePickerFileType.CUSTOM,
            allowed_extensions=["csv"],
            allow_multiple=False,
        )
        if files:
            csv_path_tf.value = files[0].path
            page.update()

    browse_btn = ft.IconButton(
        icon=ft.Icons.FOLDER_OPEN,
        tooltip="Browse for CSV file",
        on_click=_pick_csv,
    )

    ticker_tf = ft.TextField(label="Ticker", value=current.get("ticker", ""), dense=True, width=200, hint_text="e.g. 7203")
    currency_tf = ft.TextField(label="Currency", value=current.get("currency", "JPY"), dense=True, width=200, hint_text="e.g. JPY, USD")
    date_col_tf = ft.TextField(label="Date Column", value=current.get("date_column", "Date"), dense=True, width=200, hint_text="CSV column for date")
    price_col_tf = ft.TextField(label="Price Column", value=current.get("price_column", "Close"), dense=True, width=200, hint_text="CSV column for price")

    def save(_):
        if not csv_path_tf.value.strip():
            snack("Please select a CSV file")
            return
        if not ticker_tf.value.strip():
            snack("Please enter a ticker symbol")
            return
        step_configs["import_stock_prices_csv"] = {
            "csv_file": csv_path_tf.value.strip(),
            "ticker": ticker_tf.value.strip(),
            "currency": currency_tf.value.strip() or "JPY",
            "date_column": date_col_tf.value.strip() or "Date",
            "price_column": price_col_tf.value.strip() or "Close",
        }
        pop()
        snack("Import CSV config updated")

    show(ft.AlertDialog(
        modal=True,
        title=ft.Text("Configure: Import Stock Prices (CSV)"),
        content=ft.Column(
            [
                ft.Text("Select a CSV file and map its columns to the database fields.", size=12, color=ft.Colors.GREY_500),
                ft.Row([csv_path_tf, browse_btn], spacing=4),
                ft.Divider(height=1),
                ft.Row([ticker_tf, currency_tf], spacing=16),
                ft.Divider(height=1),
                ft.Text("Column Mapping", weight=ft.FontWeight.BOLD, size=13),
                ft.Text("Specify which columns in the CSV correspond to Date and Price.", size=11, color=ft.Colors.GREY_500),
                ft.Row([date_col_tf, price_col_tf], spacing=16),
            ],
            scroll=ft.ScrollMode.AUTO,
            width=500,
            height=320,
            spacing=8,
        ),
        actions=[ft.TextButton("Cancel", on_click=lambda _: pop()), ft.Button("Save", on_click=save)],
    ))


def open_backtest_config(
    page: ft.Page,
    step_configs: dict[str, dict],
    snack: Callable[[str], None],
    show: Callable[[ft.AlertDialog], None],
    pop: Callable[[], None],
):
    current = step_configs.get("backtest", {})
    if not current:
        current = copy.deepcopy(DEFAULT_STEP_CONFIGS.get("backtest", {}))
    raw_portfolio = current.get("portfolio", {})

    portfolio: dict[str, dict] = {}
    for tk, spec in raw_portfolio.items():
        if isinstance(spec, (int, float)):
            portfolio[tk] = {"mode": "weight", "value": spec * 100}
        elif isinstance(spec, dict):
            mode = spec.get("mode", "weight")
            val = spec.get("value", 0)
            portfolio[tk] = {"mode": mode, "value": val * 100 if mode == "weight" else val}
        else:
            portfolio[tk] = {"mode": "weight", "value": 0}

    start_tf = ft.TextField(label="Start Date (YYYY-MM-DD)", value=current.get("start_date", ""), dense=True, width=220)
    end_tf = ft.TextField(label="End Date (YYYY-MM-DD)", value=current.get("end_date", ""), dense=True, width=220)
    bench_tf = ft.TextField(label="Benchmark Ticker (optional)", value=current.get("benchmark_ticker", ""), dense=True, width=220)
    output_tf = ft.TextField(label="Output File", value=current.get("output_file", "data/backtest_results/backtest_report.txt"), dense=True, width=460)
    risk_free_tf = ft.TextField(label="Risk-Free Rate (%)", value=str(current.get("risk_free_rate", 0.0) * 100), dense=True, width=220, hint_text="e.g. 2.5 for 2.5%")
    capital_tf = ft.TextField(label="Initial Capital (0 = omit)", value=str(int(current.get("initial_capital", 0))), dense=True, width=220, hint_text="e.g. 1000000")

    min_empty_rows = 3
    col_order = ["ticker", "type", "amount"]
    grid_rows = [{"ticker": tk, "type": entry["mode"], "amount": str(entry["value"])} for tk, entry in sorted(portfolio.items())]
    focused_cell = {"row": 0, "col": "ticker"}
    selected_rows: set[int] = set()

    def ensure_empty_rows():
        empty = sum(1 for r in grid_rows if not r["ticker"].strip())
        while empty < min_empty_rows:
            grid_rows.append({"ticker": "", "type": "weight", "amount": ""})
            empty += 1

    ensure_empty_rows()
    weight_total_text = ft.Text("", size=12)
    grid_column = ft.Column(spacing=0, scroll=ft.ScrollMode.AUTO, height=280)

    def update_weight_total():
        filled = [r for r in grid_rows if r["ticker"].strip()]
        if not filled:
            weight_total_text.value = ""
            weight_total_text.color = None
            return
        weight_sum = 0.0
        has_fixed = False
        n_shares = n_value = 0
        for r in filled:
            mode = r["type"]
            try:
                val = float(r["amount"])
            except (ValueError, TypeError):
                val = 0
            if mode == "weight":
                weight_sum += val
            elif mode == "shares":
                has_fixed = True; n_shares += 1
            elif mode == "value":
                has_fixed = True; n_value += 1
        parts: list[str] = []
        if weight_sum > 0:
            if abs(weight_sum - 100.0) < 0.01 and not has_fixed:
                parts.append(f"Weight total: {weight_sum:.1f}% ✓")
                weight_total_text.color = ft.Colors.GREEN_700
            elif has_fixed:
                parts.append(f"Weight total: {weight_sum:.1f}%")
                weight_total_text.color = ft.Colors.BLUE_400
            else:
                parts.append(f"Weight total: {weight_sum:.1f}% (will be normalised)")
                weight_total_text.color = ft.Colors.ORANGE_400
        if has_fixed:
            fp = []
            if n_shares:
                fp.append(f"{n_shares} by shares")
            if n_value:
                fp.append(f"{n_value} by value")
            parts.append("Fixed: " + ", ".join(fp))
            if not weight_sum:
                weight_total_text.color = ft.Colors.GREEN_700
        weight_total_text.value = "  |  ".join(parts)

    cell_border = ft.border.only(right=ft.BorderSide(1, ft.Colors.GREY_300), bottom=ft.BorderSide(1, ft.Colors.GREY_200))
    header_border = ft.border.only(right=ft.BorderSide(1, ft.Colors.GREY_400), bottom=ft.BorderSide(2, ft.Colors.GREY_500))

    def row_bg(idx: int) -> str:
        return ft.Colors.LIGHT_BLUE_50 if idx in selected_rows else (ft.Colors.WHITE if idx % 2 == 0 else ft.Colors.GREY_50)

    def on_cell_focus(row_idx: int, col: str):
        focused_cell["row"] = row_idx
        focused_cell["col"] = col

    def on_cell_blur(idx: int, col: str, value: str):
        if idx >= len(grid_rows):
            return
        if col == "ticker":
            grid_rows[idx]["ticker"] = value.strip().upper()
        elif col == "type":
            v = value.strip().lower()
            grid_rows[idx]["type"] = v if v in ("weight", "shares", "value") else "weight"
        elif col == "amount":
            grid_rows[idx]["amount"] = value.strip()
        ensure_empty_rows()
        update_weight_total()
        page.update()

    def cell_tf(value: str, width: int, *, row_idx: int, col: str) -> ft.Container:
        tf = ft.TextField(
            value=value,
            border=ft.InputBorder.NONE,
            text_size=12,
            dense=True,
            content_padding=ft.padding.symmetric(horizontal=8, vertical=6),
            on_focus=lambda _, i=row_idx, c=col: on_cell_focus(i, c),
            on_blur=lambda e, i=row_idx, c=col: on_cell_blur(i, c, e.control.value),
            hint_text="weight" if (col == "type" and not value) else None,
        )
        return ft.Container(content=tf, width=width, border=cell_border)

    def rebuild_grid():
        grid_column.controls.clear()
        grid_column.controls.append(
            ft.Container(
                content=ft.Row([
                    ft.Container(ft.Text("#", weight=ft.FontWeight.BOLD, size=11), width=30, padding=ft.padding.symmetric(horizontal=4, vertical=6), border=header_border),
                    ft.Container(ft.Text("Ticker", weight=ft.FontWeight.BOLD, size=12), width=120, padding=ft.padding.symmetric(horizontal=8, vertical=6), border=header_border),
                    ft.Container(ft.Text("Type", weight=ft.FontWeight.BOLD, size=12), width=100, padding=ft.padding.symmetric(horizontal=8, vertical=6), border=header_border),
                    ft.Container(ft.Text("Amount", weight=ft.FontWeight.BOLD, size=12), width=110, padding=ft.padding.symmetric(horizontal=8, vertical=6), border=header_border),
                    ft.Container(width=36),
                ], spacing=0),
                bgcolor=ft.Colors.BLUE_GREY_50,
            )
        )
        for idx, row_data in enumerate(grid_rows):
            is_empty = not row_data["ticker"].strip()
            delete_btn = ft.IconButton(icon=ft.Icons.CLOSE, icon_size=14, icon_color=(ft.Colors.RED_400 if not is_empty else ft.Colors.TRANSPARENT), tooltip="Delete row" if not is_empty else None, on_click=((lambda _, i=idx: delete_row(i)) if not is_empty else None), disabled=is_empty)
            row_num_cell = ft.Container(
                content=ft.Text(str(idx + 1), size=10, color=(ft.Colors.BLUE_700 if idx in selected_rows else ft.Colors.GREY_500), weight=(ft.FontWeight.BOLD if idx in selected_rows else None)),
                width=30,
                padding=ft.padding.symmetric(horizontal=4, vertical=6),
                border=cell_border,
                on_click=lambda _, i=idx: on_row_click(i),
                bgcolor=(ft.Colors.LIGHT_BLUE_100 if idx in selected_rows else None),
            )
            grid_column.controls.append(
                ft.Container(
                    content=ft.Row([
                        row_num_cell,
                        cell_tf(row_data["ticker"], 120, row_idx=idx, col="ticker"),
                        cell_tf(row_data["type"], 100, row_idx=idx, col="type"),
                        cell_tf(row_data["amount"], 110, row_idx=idx, col="amount"),
                        ft.Container(content=delete_btn, width=36),
                    ], spacing=0),
                    bgcolor=row_bg(idx),
                )
            )
        update_weight_total()
        page.update()

    def on_row_click(idx: int):
        if idx in selected_rows:
            selected_rows.discard(idx)
        else:
            selected_rows.add(idx)
        rebuild_grid()

    def delete_row(idx: int):
        if idx < len(grid_rows):
            grid_rows.pop(idx)
            selected_rows.discard(idx)
            new_sel = {(s - 1 if s > idx else s) for s in selected_rows if s != idx}
            selected_rows.clear()
            selected_rows.update(new_sel)
            ensure_empty_rows()
            rebuild_grid()

    def distribute_paste(text: str, start_row: int, start_col: str):
        col_start = col_order.index(start_col) if start_col in col_order else 0
        lines = text.replace("\r", "").split("\n")
        for li, line in enumerate(lines):
            if not line.strip():
                continue
            parts = line.split("\t") if "\t" in line else line.split()
            target_row = start_row + li
            while target_row >= len(grid_rows):
                grid_rows.append({"ticker": "", "type": "weight", "amount": ""})
            for pi, part in enumerate(parts):
                ci = col_start + pi
                if ci >= len(col_order):
                    break
                col_name = col_order[ci]
                val = part.strip()
                if col_name == "ticker":
                    grid_rows[target_row]["ticker"] = val.upper()
                elif col_name == "type":
                    v = val.lower()
                    grid_rows[target_row]["type"] = v if v in ("weight", "shares", "value") else "weight"
                elif col_name == "amount":
                    grid_rows[target_row]["amount"] = val
        ensure_empty_rows()
        rebuild_grid()

    prev_kb_handler = page.on_keyboard_event

    async def grid_keyboard_handler(e: ft.KeyboardEvent):
        if e.ctrl and e.key.lower() == "v":
            try:
                text = await page.clipboard.get()
            except Exception:
                return
            if text and text.strip():
                distribute_paste(text, focused_cell["row"], focused_cell["col"])
        elif e.ctrl and e.key.lower() == "c":
            rows_to_copy = sorted(selected_rows) if selected_rows else [i for i, r in enumerate(grid_rows) if r["ticker"].strip()]
            if not rows_to_copy:
                return
            lines = [f"{grid_rows[i]['ticker']}\t{grid_rows[i]['type']}\t{grid_rows[i]['amount']}" for i in rows_to_copy]
            try:
                await page.clipboard.set("\n".join(lines))
                snack(f"Copied {len(lines)} rows")
            except Exception:
                pass
        elif e.ctrl and e.key.lower() == "a":
            selected_rows.clear()
            for i, r in enumerate(grid_rows):
                if r["ticker"].strip():
                    selected_rows.add(i)
            rebuild_grid()

    page.on_keyboard_event = grid_keyboard_handler

    def add_row(_):
        grid_rows.append({"ticker": "", "type": "weight", "amount": ""})
        rebuild_grid()

    def select_all(_):
        selected_rows.clear()
        for i, r in enumerate(grid_rows):
            if r["ticker"].strip():
                selected_rows.add(i)
        rebuild_grid()

    def deselect_all(_):
        selected_rows.clear()
        rebuild_grid()

    def clear_all(_):
        grid_rows.clear()
        selected_rows.clear()
        ensure_empty_rows()
        rebuild_grid()

    add_row_btn = ft.IconButton(icon=ft.Icons.ADD_CIRCLE, icon_color=ft.Colors.GREEN_700, tooltip="Add row", on_click=add_row)
    select_all_btn = ft.IconButton(icon=ft.Icons.SELECT_ALL, icon_color=ft.Colors.BLUE_700, tooltip="Select all rows (for copy)", on_click=select_all)
    deselect_btn = ft.IconButton(icon=ft.Icons.DESELECT, icon_color=ft.Colors.GREY_600, tooltip="Deselect all", on_click=deselect_all)
    clear_btn = ft.IconButton(icon=ft.Icons.DELETE_SWEEP, icon_color=ft.Colors.RED_400, tooltip="Clear all rows", on_click=clear_all)

    rebuild_grid()

    def restore_kb_handler():
        page.on_keyboard_event = prev_kb_handler

    def save(_):
        table_portfolio: dict[str, dict] = {}
        for r in grid_rows:
            tk = r["ticker"].strip().upper()
            if not tk:
                continue
            mode = r["type"].strip().lower()
            if mode not in ("weight", "shares", "value"):
                mode = "weight"
            raw = r["amount"].strip()
            try:
                val = float(raw)
            except (ValueError, TypeError):
                snack(f"Invalid amount for {tk}")
                return
            if val <= 0:
                snack(f"Amount for {tk} must be positive")
                return
            if mode == "weight" and val > 100:
                snack(f"Weight for {tk} cannot exceed 100%")
                return
            table_portfolio[tk] = {"mode": mode, "value": val}
        if not table_portfolio:
            snack("Portfolio is empty — add at least one ticker")
            return
        weight_sum = sum(e["value"] for e in table_portfolio.values() if e["mode"] == "weight")
        has_fixed = any(e["mode"] in ("shares", "value") for e in table_portfolio.values())
        if not has_fixed and abs(weight_sum - 100.0) > 0.01:
            snack(f"⚠ Weights sum to {weight_sum:.1f}% (not 100%). They will be normalised at run time.")
        try:
            rf = float(risk_free_tf.value.strip()) / 100.0
        except ValueError:
            rf = 0.0
        try:
            cap = float(capital_tf.value.strip())
        except ValueError:
            cap = 0.0
        saved_portfolio = {}
        for tk, entry in table_portfolio.items():
            mode = entry["mode"]
            val = entry["value"]
            saved_portfolio[tk] = {"mode": "weight", "value": val / 100.0} if mode == "weight" else {"mode": mode, "value": val}
        step_configs["backtest"] = {
            "start_date": start_tf.value.strip(),
            "end_date": end_tf.value.strip(),
            "portfolio": saved_portfolio,
            "benchmark_ticker": bench_tf.value.strip(),
            "output_file": output_tf.value.strip(),
            "risk_free_rate": rf,
            "initial_capital": cap,
        }
        restore_kb_handler()
        pop()
        snack("Backtest config updated")

    def cancel(_):
        restore_kb_handler()
        pop()

    show(ft.AlertDialog(
        modal=True,
        title=ft.Text("Configure: Backtest Portfolio"),
        content=ft.Column([
            ft.Row([start_tf, end_tf], spacing=16),
            ft.Row([bench_tf, risk_free_tf], spacing=16),
            capital_tf,
            output_tf,
            ft.Divider(height=1),
            ft.Text("Portfolio", weight=ft.FontWeight.BOLD, size=14),
            ft.Text("Click cells to edit  •  Ctrl+V to paste  •  Click row # to select  •  Ctrl+C to copy  •  Ctrl+A select all", size=10, color=ft.Colors.GREY_600),
            ft.Row([add_row_btn, select_all_btn, deselect_btn, clear_btn], spacing=0),
            ft.Container(content=grid_column, border=ft.border.all(1, ft.Colors.GREY_400), border_radius=4),
            weight_total_text,
        ], scroll=ft.ScrollMode.AUTO, width=540, height=620, spacing=8),
        actions=[ft.TextButton("Cancel", on_click=cancel), ft.Button("Save", on_click=save)],
    ))
