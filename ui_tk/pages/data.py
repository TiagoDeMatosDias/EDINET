"""Data page placeholder — to be developed later."""

from tkinter import ttk


class DataPage(ttk.Frame):
    def __init__(self, parent, **kw):
        super().__init__(parent, **kw)
        ttk.Label(self, text="Data — coming soon",
                  style="Dim.TLabel").pack(expand=True)
