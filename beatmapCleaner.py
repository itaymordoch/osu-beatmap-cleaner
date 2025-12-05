#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
osu_prune_maps_gui.py
Windows GUI to:
  TAB 1: delete osu! beatmap difficulties (and optionally entire mapsets)
         based on BPM, AR, and CS criteria.
  TAB 2: delete all mapsets that are NOT part of an osu! collection (collection.db)
         while keeping entire mapsets if ANY difficulty MD5 is referenced.

Features:
- Two independent modes (tabs):
    1) Delete by stats (BPM/AR/CS pruning)
    2) Delete rogue maps (keep only mapsets referenced in collection.db)
- Select Songs folder (auto-detects Windows default if possible, for both tabs)
- Single flow: scan first, show results, then let user confirm deletion
- Option to delete entire mapset if ALL difficulties fail (tab 1)
- Permanent deletions only (no Recycle Bin / send2trash)
- Background thread for long scan operations; live log output and progress
- UI styling loosely inspired by osu! skin mixer (color palette and layout)

Usage:
  python osu_prune_maps_gui.py

Author: ChatGPT for Shutaf
"""

import os
import re
import shutil
import threading
import queue
import struct
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Dict, Set

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

# -----------------------------
# Core parsing & filtering
# -----------------------------

TIMING_POINT_RE = re.compile(
    r'^\s*(?P<time>-?\d+)\s*,\s*(?P<ms_per_beat>-?\d+\.?\d*)\s*,\s*(?P<meter>\d+)\s*,\s*'
    r'(?P<sample_set>\d+)\s*,\s*(?P<sample_index>\d+)\s*,\s*(?P<volume>\d+)\s*,\s*'
    r'(?P<uninherited>[01])\s*,\s*(?P<effects>\d+)\s*$'
)


@dataclass
class BeatmapInfo:
    osu_path: Path
    mapset_dir: Path
    title: Optional[str] = None
    artist: Optional[str] = None
    creator: Optional[str] = None
    version: Optional[str] = None  # difficulty name
    ar: Optional[float] = None
    cs: Optional[float] = None
    bpm_main: Optional[float] = None


def default_songs_dir() -> Optional[Path]:
    if os.name == "nt":
        local = os.environ.get("LOCALAPPDATA")
        if local:
            p = Path(local) / "osu!" / "Songs"
            if p.exists():
                return p
    return None


def default_collection_db() -> Optional[Path]:
    if os.name == "nt":
        local = os.environ.get("LOCALAPPDATA")
        if local:
            p = Path(local) / "osu!" / "collection.db"
            if p.exists():
                return p
    return None


def read_osu_file(p: Path) -> BeatmapInfo:
    info = BeatmapInfo(osu_path=p, mapset_dir=p.parent)
    section = None
    timing_points = []

    try:
        with p.open("r", encoding="utf-8", errors="ignore") as f:
            for raw in f:
                line = raw.strip()
                if not line:
                    continue
                if line.startswith("[") and line.endswith("]"):
                    section = line[1:-1]
                    continue

                if section == "Metadata":
                    if line.startswith("Title:"):
                        info.title = line.split(":", 1)[1].strip()
                    elif line.startswith("Artist:"):
                        info.artist = line.split(":", 1)[1].strip()
                    elif line.startswith("Creator:"):
                        info.creator = line.split(":", 1)[1].strip()
                    elif line.startswith("Version:"):
                        info.version = line.split(":", 1)[1].strip()

                elif section == "Difficulty":
                    if line.startswith("ApproachRate:"):
                        try:
                            info.ar = float(line.split(":", 1)[1].strip())
                        except ValueError:
                            info.ar = None
                    elif line.startswith("CircleSize:"):
                        try:
                            info.cs = float(line.split(":", 1)[1].strip())
                        except ValueError:
                            info.cs = None

                elif section == "TimingPoints":
                    m = TIMING_POINT_RE.match(line)
                    if m:
                        uninherited = int(m.group("uninherited"))
                        ms_per_beat = float(m.group("ms_per_beat"))
                        start_time = int(m.group("time"))
                        timing_points.append((start_time, ms_per_beat, uninherited))
    except Exception:
        # Ignore unreadable files
        pass

    # Compute BPM from redline timing points
    redlines = [(t, ms) for (t, ms, u) in timing_points if u == 1 and ms > 0]
    if redlines:
        redlines.sort(key=lambda x: x[0])
        segments = []
        for i, (t, ms) in enumerate(redlines):
            if i + 1 < len(redlines):
                dur = redlines[i + 1][0] - t
            else:
                dur = 60000  # 60s placeholder
            bpm = 60000.0 / ms
            segments.append((bpm, max(1, dur)))
        info.bpm_main = round(max(segments, key=lambda x: x[1])[0], 2)

    return info


def fails_criteria(info: BeatmapInfo,
                   min_bpm: Optional[float],
                   max_bpm: Optional[float],
                   min_ar: Optional[float],
                   max_ar: Optional[float],
                   min_cs: Optional[float],
                   max_cs: Optional[float]) -> bool:
    # BPM checks
    if min_bpm is not None:
        if info.bpm_main is None or info.bpm_main < min_bpm:
            return True
    if max_bpm is not None:
        if info.bpm_main is None or info.bpm_main > max_bpm:
            return True
    # AR checks
    if min_ar is not None:
        if info.ar is None or info.ar < min_ar:
            return True
    if max_ar is not None:
        if info.ar is None or info.ar > max_ar:
            return True
    # CS checks
    if min_cs is not None:
        if info.cs is None or info.cs < min_cs:
            return True
    if max_cs is not None:
        if info.cs is None or info.cs > max_cs:
            return True
    return False


def scan_songs(songs_dir: Path) -> List[Path]:
    # Returns list of all .osu files in Songs subdirectories
    return list(songs_dir.glob("*/*.osu"))


def safe_delete_file(path: Path, log_cb) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass
    except Exception as e:
        log_cb(f"Failed to delete file {path}: {e}")


def safe_delete_dir(path: Path, log_cb) -> None:
    try:
        shutil.rmtree(path, ignore_errors=False)
    except Exception as e:
        log_cb(f"Failed to delete directory {path}: {e}")


# -----------------------------
# collection.db parsing
# -----------------------------

def _read_int32(f) -> int:
    data = f.read(4)
    if len(data) != 4:
        raise EOFError("Unexpected end of file while reading int32")
    return struct.unpack("<i", data)[0]


def _read_uleb128(f) -> int:
    result = 0
    shift = 0
    while True:
        b = f.read(1)
        if not b:
            raise EOFError("Unexpected end of file while reading uleb128")
        b = b[0]
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            break
        shift += 7
    return result


def _read_osu_string(f) -> str:
    prefix = f.read(1)
    if not prefix:
        raise EOFError("Unexpected end of file while reading osu string")
    if prefix == b"\x00":
        return ""
    if prefix != b"\x0b":
        # Unknown prefix, treat as empty
        return ""
    length = _read_uleb128(f)
    data = f.read(length)
    if len(data) != length:
        raise EOFError("Unexpected end of file while reading osu string payload")
    return data.decode("utf-8", errors="ignore")


def load_collection_hashes(path: Path, log_cb=None) -> Set[str]:
    hashes: Set[str] = set()
    try:
        with path.open("rb") as f:
            version = _read_int32(f)     # int32
            num_collections = _read_int32(f)
            if log_cb:
                log_cb(f"collection.db version: {version}, collections: {num_collections}")
            for _ in range(num_collections):
                _ = _read_osu_string(f)  # collection name (unused)
                num_beatmaps = _read_int32(f)
                for _ in range(num_beatmaps):
                    h = _read_osu_string(f).strip()
                    if h:
                        hashes.add(h)
    except Exception as e:
        if log_cb:
            log_cb(f"ERROR reading collection.db: {e}")
        raise
    return hashes


def compute_osu_md5(path: Path) -> Optional[str]:
    try:
        data = path.read_bytes()
        return hashlib.md5(data).hexdigest()
    except Exception:
        return None


# -----------------------------
# Worker (BPM/AR/CS) - TAB 1
# -----------------------------

class PruneWorker(threading.Thread):
    def __init__(self, params: dict, out_queue: queue.Queue):
        super().__init__(daemon=True)
        self.params = params
        self.out = out_queue

    def log(self, msg: str):
        self.out.put(("log", msg))

    def progress(self, done: int, total: int):
        self.out.put(("progress", (done, total)))

    def result(self, payload: dict):
        self.out.put(("result", payload))

    def run(self):
        try:
            self._run_impl()
        except Exception as e:
            self.log(f"ERROR: {e}")

    def _run_impl(self):
        songs_dir: Path = self.params["songs_dir"]
        min_bpm = self.params["min_bpm"]
        max_bpm = self.params["max_bpm"]
        min_ar = self.params["min_ar"]
        max_ar = self.params["max_ar"]
        min_cs = self.params["min_cs"]
        max_cs = self.params["max_cs"]
        delete_mapset_if_all_fail: bool = self.params["delete_mapset"]

        self.log(f"[BPM/AR/CS] Scanning .osu files under: {songs_dir}")
        osu_files = scan_songs(songs_dir)
        total = len(osu_files)
        if total == 0:
            self.log("No .osu files found.")
            self.result({"to_delete_mapsets": [], "to_delete_osu": []})
            return

        # Read infos with progress
        infos: List[BeatmapInfo] = []
        for idx, p in enumerate(osu_files, start=1):
            infos.append(read_osu_file(p))
            if idx % 50 == 0 or idx == total:
                self.progress(idx, total)

        # Group by mapset
        by_mapset: Dict[Path, List[BeatmapInfo]] = {}
        for info in infos:
            by_mapset.setdefault(info.mapset_dir, []).append(info)

        to_delete_osu: List[BeatmapInfo] = []
        to_delete_mapsets: List[Path] = []

        for mapset_dir, diffs in by_mapset.items():
            failing = [bm for bm in diffs if fails_criteria(
                bm, min_bpm, max_bpm, min_ar, max_ar, min_cs, max_cs
            )]
            if delete_mapset_if_all_fail:
                if failing and len(failing) == len(diffs):
                    to_delete_mapsets.append(mapset_dir)
                else:
                    to_delete_osu.extend(failing)
            else:
                to_delete_osu.extend(failing)

        # If mapset scheduled, skip its individual .osu deletions
        if to_delete_mapsets:
            mapset_set = set(to_delete_mapsets)
            to_delete_osu = [bm for bm in to_delete_osu if bm.mapset_dir not in mapset_set]

        # Return results (scan only; deletion handled after confirmation in GUI)
        self.result({
            "to_delete_mapsets": to_delete_mapsets,
            "to_delete_osu": to_delete_osu,
        })


# -----------------------------
# Worker (collection cleanup) - TAB 2
# -----------------------------

class CollectionPruneWorker(threading.Thread):
    """
    Scans mapsets that are NOT referenced by any MD5 hash in collection.db.
    Keeps full mapset if ANY difficulty MD5 matches a collection hash.
    Deletion is done later after user confirmation.
    """
    def __init__(self, params: dict, out_queue: queue.Queue):
        super().__init__(daemon=True)
        self.params = params
        self.out = out_queue

    def log(self, msg: str):
        self.out.put(("log", msg))

    def progress(self, done: int, total: int):
        self.out.put(("progress", (done, total)))

    def result(self, payload: dict):
        self.out.put(("result", payload))

    def run(self):
        try:
            self._run_impl()
        except Exception as e:
            self.log(f"ERROR (collection mode): {e}")

    def _run_impl(self):
        songs_dir: Path = self.params["songs_dir"]
        collection_db_path: Path = self.params["collection_db"]

        self.log(f"[COLLECTION] Loading hashes from: {collection_db_path}")
        hashes = load_collection_hashes(collection_db_path, log_cb=self.log)
        self.log(f"[COLLECTION] Unique beatmap hashes in collection: {len(hashes)}")

        if not hashes:
            self.log("[COLLECTION] No hashes parsed from collection.db; nothing to do.")
            self.result({"to_delete_mapsets": [], "to_delete_osu": []})
            return

        self.log(f"[COLLECTION] Scanning .osu files under: {songs_dir}")
        osu_files = scan_songs(songs_dir)
        total = len(osu_files)
        if total == 0:
            self.log("No .osu files found.")
            self.result({"to_delete_mapsets": [], "to_delete_osu": []})
            return

        all_mapsets: Set[Path] = set()
        keep_mapsets: Set[Path] = set()

        for idx, p in enumerate(osu_files, start=1):
            mapset_dir = p.parent
            all_mapsets.add(mapset_dir)

            md5 = compute_osu_md5(p)
            if md5 and md5 in hashes:
                keep_mapsets.add(mapset_dir)

            if idx % 50 == 0 or idx == total:
                self.progress(idx, total)

        to_delete_mapsets = sorted(all_mapsets - keep_mapsets, key=lambda x: str(x))
        to_delete_osu: List[BeatmapInfo] = []  # collection mode deletes mapsets only

        self.log(f"[COLLECTION] Mapsets total: {len(all_mapsets)}, kept: {len(keep_mapsets)}, "
                 f"to delete: {len(to_delete_mapsets)}")

        # Return results (scan only; deletion handled after confirmation in GUI)
        self.result({
            "to_delete_mapsets": to_delete_mapsets,
            "to_delete_osu": to_delete_osu,
        })


# -----------------------------
# GUI
# -----------------------------

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        # Color palette loosely matching osu! skin mixer
        self.APP_BG = "#15121d"
        self.CARD_BG = "#241826"
        self.CARD_BG_DARK = "#1e1623"
        self.ACCENT = "#ff90b8"
        self.ACCENT_DARK = "#d9729a"
        self.TEXT_PRIMARY = "#f6e8ff"
        self.TEXT_MUTED = "#c0a6d5"

        self.title("osu! Prune maps")
        self.geometry("980x640")
        self.minsize(900, 560)
        self.configure(bg=self.APP_BG)

        self.out_queue: queue.Queue = queue.Queue()
        self.worker: Optional[threading.Thread] = None

        self.last_to_delete_mapsets: List[Path] = []
        self.last_to_delete_osu: List[BeatmapInfo] = []

        self._init_style()

        # Vars for TAB 1 (BPM/AR/CS)
        self.songs_var = tk.StringVar()
        self.min_bpm_var = tk.StringVar()
        self.max_bpm_var = tk.StringVar()
        self.min_ar_var = tk.StringVar()
        self.max_ar_var = tk.StringVar()
        self.min_cs_var = tk.StringVar()
        self.max_cs_var = tk.StringVar()
        self.delete_mapset_var = tk.BooleanVar(value=False)

        # Vars for TAB 2 (collection)
        self.songs_collection_var = tk.StringVar()
        self.collection_db_var = tk.StringVar()

        self._build_widgets()
        self._init_defaults()
        self.after(100, self._poll_queue)

    def _init_style(self):
        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except Exception:
            pass

        style.configure("App.TFrame", background=self.APP_BG)
        style.configure("Header.TFrame", background=self.APP_BG)
        style.configure("Header.TLabel", background=self.APP_BG,
                        foreground=self.TEXT_PRIMARY, font=("Segoe UI", 16, "bold"))
        style.configure("HeaderSub.TLabel", background=self.APP_BG,
                        foreground=self.TEXT_MUTED, font=("Segoe UI", 9))

        style.configure("Card.TFrame", background=self.CARD_BG)
        style.configure("CardInner.TFrame", background=self.CARD_BG_DARK)
        style.configure("Card.TLabelframe", background=self.CARD_BG, borderwidth=0)
        style.configure("Card.TLabelframe.Label",
                        background=self.CARD_BG, foreground=self.TEXT_MUTED)

        style.configure("App.TLabel", background=self.CARD_BG, foreground=self.TEXT_PRIMARY)
        style.configure("Muted.TLabel", background=self.CARD_BG, foreground=self.TEXT_MUTED)

        style.configure("Dark.TButton",
                        background=self.CARD_BG_DARK,
                        foreground=self.TEXT_PRIMARY,
                        padding=(10, 4))
        style.map("Dark.TButton",
                  background=[("active", self.CARD_BG_DARK), ("pressed", self.CARD_BG_DARK)])

        style.configure("Accent.TButton",
                        background=self.ACCENT,
                        foreground="#261322",
                        padding=(16, 6),
                        relief="flat")
        style.map("Accent.TButton",
                  background=[("active", self.ACCENT_DARK), ("pressed", self.ACCENT_DARK)],
                  foreground=[("disabled", "#555555")])

        style.configure("App.TNotebook", background=self.APP_BG, borderwidth=0)
        style.configure("App.TNotebook.Tab", padding=(20, 8),
                        background=self.CARD_BG_DARK, foreground=self.TEXT_MUTED)
        style.map("App.TNotebook.Tab",
                  background=[("selected", self.CARD_BG)],
                  foreground=[("selected", self.TEXT_PRIMARY)])

        style.configure("App.Treeview",
                        background=self.CARD_BG_DARK,
                        foreground=self.TEXT_PRIMARY,
                        fieldbackground=self.CARD_BG_DARK,
                        bordercolor=self.CARD_BG,
                        rowheight=22)
        style.configure("App.Vertical.TScrollbar", background=self.CARD_BG_DARK)

    def _build_widgets(self):
        # Main container
        container = ttk.Frame(self, style="App.TFrame")
        container.pack(fill="both", expand=True)

        # Header bar (similar to skin mixer top bar)
        header = ttk.Frame(container, padding=(14, 10, 14, 6), style="Header.TFrame")
        header.pack(fill="x")

        # (Optional) back button / icon placeholder could go here

        title_lbl = ttk.Label(header, text="osu! prune maps", style="Header.TLabel")
        title_lbl.pack(side="left")

        subtitle_lbl = ttk.Label(
            header,
            text="Clean up your Songs folder safely",
            style="HeaderSub.TLabel"
        )
        subtitle_lbl.pack(side="left", padx=(10, 0))

        version_lbl = ttk.Label(
            header,
            text="v1.0",
            style="HeaderSub.TLabel"
        )
        version_lbl.pack(side="right")

        # Card container similar to central panel in skin mixer
        card_outer = ttk.Frame(container, padding=(16, 12, 16, 10), style="App.TFrame")
        card_outer.pack(fill="both", expand=True)

        card = ttk.Frame(card_outer, padding=18, style="Card.TFrame")
        card.pack(fill="both", expand=True)

        # Notebook with two tabs inside card
        self.notebook = ttk.Notebook(card, style="App.TNotebook")
        self.notebook.pack(fill="x", padx=4, pady=(0, 8))

        self.tab_bpm = ttk.Frame(self.notebook, style="CardInner.TFrame")
        self.tab_collection = ttk.Frame(self.notebook, style="CardInner.TFrame")

        self.notebook.add(self.tab_bpm, text="Delete by stats")
        self.notebook.add(self.tab_collection, text="Delete rogue maps")

        # ---------------- TAB 1: BPM/AR/CS ----------------
        top = ttk.Frame(self.tab_bpm, padding=(8, 10, 8, 4), style="CardInner.TFrame")
        top.pack(fill="x")

        ttk.Label(top, text="Songs folder:", style="App.TLabel").grid(row=0, column=0, sticky="w")
        self.songs_entry = ttk.Entry(top, textvariable=self.songs_var, width=80)
        self.songs_entry.grid(row=0, column=1, sticky="we", padx=6)
        ttk.Button(top, text="Browse…", command=self._browse_songs,
                   style="Dark.TButton").grid(row=0, column=2, sticky="w")
        top.columnconfigure(1, weight=1)

        crit = ttk.LabelFrame(
            self.tab_bpm,
            text="Criteria (BPM / AR / CS)",
            padding=(12, 10, 12, 10),
            style="Card.TLabelframe"
        )
        crit.pack(fill="x", padx=8, pady=(0, 4))

        ttk.Label(crit, text="BPM:", style="App.TLabel").grid(row=0, column=0, sticky="e")
        ttk.Entry(crit, width=10, textvariable=self.min_bpm_var).grid(row=0, column=1, sticky="w")
        ttk.Label(crit, text="to", style="Muted.TLabel").grid(row=0, column=2)
        ttk.Entry(crit, width=10, textvariable=self.max_bpm_var).grid(row=0, column=3, sticky="w")

        ttk.Label(crit, text="AR:", style="App.TLabel").grid(row=0, column=4, sticky="e", padx=(14, 0))
        ttk.Entry(crit, width=10, textvariable=self.min_ar_var).grid(row=0, column=5, sticky="w")
        ttk.Label(crit, text="to", style="Muted.TLabel").grid(row=0, column=6)
        ttk.Entry(crit, width=10, textvariable=self.max_ar_var).grid(row=0, column=7, sticky="w")

        ttk.Label(crit, text="CS:", style="App.TLabel").grid(row=0, column=8, sticky="e", padx=(14, 0))
        ttk.Entry(crit, width=10, textvariable=self.min_cs_var).grid(row=0, column=9, sticky="w")
        ttk.Label(crit, text="to", style="Muted.TLabel").grid(row=0, column=10)
        ttk.Entry(crit, width=10, textvariable=self.max_cs_var).grid(row=0, column=11, sticky="w")

        opts = ttk.Frame(self.tab_bpm, padding=(12, 4, 12, 6), style="CardInner.TFrame")
        opts.pack(fill="x")
        ttk.Checkbutton(
            opts,
            text="Delete whole mapset if ALL its diffs fail (permanent delete)",
            variable=self.delete_mapset_var
        ).grid(row=0, column=0, sticky="w")

        actions = ttk.Frame(self.tab_bpm, padding=(12, 4, 12, 10), style="CardInner.TFrame")
        actions.pack(fill="x")
        self.run_bpm_btn = ttk.Button(
            actions,
            text="Scan by stats",
            command=self._on_run_bpm,
            style="Dark.TButton"
        )
        self.run_bpm_btn.pack(side="left")

        # ---------------- TAB 2: COLLECTION ----------------
        top2 = ttk.Frame(self.tab_collection, padding=(8, 10, 8, 4), style="CardInner.TFrame")
        top2.pack(fill="x")

        ttk.Label(top2, text="Songs folder:", style="App.TLabel").grid(row=0, column=0, sticky="w")
        self.songs_collection_entry = ttk.Entry(top2, textvariable=self.songs_collection_var, width=80)
        self.songs_collection_entry.grid(row=0, column=1, sticky="we", padx=6)
        ttk.Button(top2, text="Browse…", command=self._browse_songs_collection,
                   style="Dark.TButton").grid(row=0, column=2, sticky="w")
        top2.columnconfigure(1, weight=1)

        ttk.Label(top2, text="collection.db:", style="App.TLabel").grid(
            row=1, column=0, sticky="w", pady=(4, 0)
        )
        self.collection_db_entry = ttk.Entry(top2, textvariable=self.collection_db_var, width=80)
        self.collection_db_entry.grid(row=1, column=1, sticky="we", padx=6, pady=(4, 0))
        ttk.Button(top2, text="Browse…", command=self._browse_collection_db,
                   style="Dark.TButton").grid(row=1, column=2, sticky="w", pady=(4, 0))

        actions2 = ttk.Frame(self.tab_collection, padding=(12, 4, 12, 10), style="CardInner.TFrame")
        actions2.pack(fill="x")
        self.run_collection_btn = ttk.Button(
            actions2,
            text="Scan rogue maps",
            command=self._on_run_collection,
            style="Dark.TButton"
        )
        self.run_collection_btn.pack(side="left")

        # ---------------- Common: Progress + Results + Log ----------------
        bottom = ttk.Frame(card, padding=(4, 4, 4, 0), style="Card.TFrame")
        bottom.pack(fill="both", expand=True)

        bottom_top = ttk.Frame(bottom, padding=(4, 0, 4, 4), style="Card.TFrame")
        bottom_top.pack(fill="x")

        # big confirm button centered (user can scroll list freely before pressing)
        self.confirm_btn = ttk.Button(
            bottom_top,
            text="Confirm permanent deletion",
            command=self._on_confirm_delete,
            state="disabled",
            style="Accent.TButton"
        )
        self.confirm_btn.pack(side="left", padx=(0, 10))

        self.progress = ttk.Progressbar(bottom_top, mode="determinate")
        self.progress.pack(side="right", fill="x", expand=True)
        self.progress_label = ttk.Label(bottom_top, text="Idle", style="Muted.TLabel")
        self.progress_label.pack(side="right", padx=(0, 8))

        paned = ttk.Panedwindow(bottom, orient="vertical")
        paned.pack(fill="both", expand=True, padx=0, pady=(0, 2))

        # Tree (preview of targets)
        self.tree_frame = ttk.Frame(paned, style="CardInner.TFrame")
        paned.add(self.tree_frame, weight=3)

        cols = ("type", "name")
        self.tree = ttk.Treeview(
            self.tree_frame,
            columns=cols,
            show="headings",
            height=10,
            style="App.Treeview"
        )
        self.tree.heading("type", text="TYPE")
        self.tree.heading("name", text="MAP / DIFF")
        self.tree.column("type", width=90, anchor="w")
        self.tree.column("name", width=650, anchor="w")

        yscroll = ttk.Scrollbar(
            self.tree_frame,
            orient="vertical",
            command=self.tree.yview,
            style="App.Vertical.TScrollbar"
        )
        self.tree.configure(yscrollcommand=yscroll.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        self.tree_frame.rowconfigure(0, weight=1)
        self.tree_frame.columnconfigure(0, weight=1)

        # Log
        self.log_frame = ttk.Frame(paned, style="CardInner.TFrame")
        paned.add(self.log_frame, weight=2)
        self.log_text = tk.Text(
            self.log_frame,
            height=8,
            wrap="word",
            bg=self.CARD_BG_DARK,
            fg=self.TEXT_PRIMARY,
            insertbackground=self.TEXT_PRIMARY,
            borderwidth=0,
            highlightthickness=0
        )
        self.log_text.pack(fill="both", expand=True)
        self.log_text.insert("end", "Ready.\n")

    def _init_defaults(self):
        default = default_songs_dir()
        if default:
            self.songs_var.set(str(default))
            self.songs_collection_var.set(str(default))
        coll = default_collection_db()
        if coll:
            self.collection_db_var.set(str(coll))

    # ---------------- TAB 1 Handlers ----------------

    def _browse_songs(self):
        path = filedialog.askdirectory(title="Select osu! Songs folder")
        if path:
            self.songs_var.set(path)

    def _parse_float(self, s: str) -> Optional[float]:
        s = s.strip()
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None

    def _collect_params(self) -> Optional[dict]:
        songs = self.songs_var.get().strip()
        if not songs:
            messagebox.showerror("Missing Songs Folder", "Please select your osu! Songs folder.")
            return None
        songs_path = Path(songs)
        if not songs_path.exists():
            messagebox.showerror("Invalid Path", f"Songs folder does not exist:\n{songs_path}")
            return None

        params = {
            "songs_dir": songs_path,
            "min_bpm": self._parse_float(self.min_bpm_var.get()),
            "max_bpm": self._parse_float(self.max_bpm_var.get()),
            "min_ar": self._parse_float(self.min_ar_var.get()),
            "max_ar": self._parse_float(self.max_ar_var.get()),
            "min_cs": self._parse_float(self.min_cs_var.get()),
            "max_cs": self._parse_float(self.max_cs_var.get()),
            "delete_mapset": bool(self.delete_mapset_var.get()),
        }
        return params

    def _on_run_bpm(self):
        params = self._collect_params()
        if not params:
            return
        self._start_worker_bpm(params)

    def _start_worker_bpm(self, params: dict):
        if self.worker and self.worker.is_alive():
            messagebox.showinfo("Busy", "An operation is already running. Please wait until it finishes.")
            return
        self._clear_tree()
        self.last_to_delete_mapsets = []
        self.last_to_delete_osu = []
        self.confirm_btn["state"] = "disabled"
        self._log("\n--- Started Delete by stats (scan only) ---\n")
        self.progress_label.config(text="Working…")
        self.progress["value"] = 0
        self.progress["maximum"] = 100

        self.worker = PruneWorker(params, self.out_queue)
        self.worker.start()

    # ---------------- TAB 2 Handlers ----------------

    def _browse_songs_collection(self):
        path = filedialog.askdirectory(title="Select osu! Songs folder")
        if path:
            self.songs_collection_var.set(path)

    def _browse_collection_db(self):
        path = filedialog.askopenfilename(
            title="Select collection.db",
            filetypes=[("collection.db", "collection.db"), ("All files", "*.*")]
        )
        if path:
            self.collection_db_var.set(path)

    def _collect_collection_params(self) -> Optional[dict]:
        songs = self.songs_collection_var.get().strip()
        if not songs:
            messagebox.showerror("Missing Songs Folder", "Please select your osu! Songs folder.")
            return None
        songs_path = Path(songs)
        if not songs_path.exists():
            messagebox.showerror("Invalid Path", f"Songs folder does not exist:\n{songs_path}")
            return None

        coll = self.collection_db_var.get().strip()
        if not coll:
            messagebox.showerror("Missing collection.db", "Please select your collection.db file.")
            return None
        coll_path = Path(coll)
        if not coll_path.exists():
            messagebox.showerror("Invalid Path", f"collection.db does not exist:\n{coll_path}")
            return None

        params = {
            "songs_dir": songs_path,
            "collection_db": coll_path,
        }
        return params

    def _on_run_collection(self):
        params = self._collect_collection_params()
        if not params:
            return
        self._start_worker_collection(params)

    def _start_worker_collection(self, params: dict):
        if self.worker and self.worker.is_alive():
            messagebox.showinfo("Busy", "An operation is already running. Please wait until it finishes.")
            return
        self._clear_tree()
        self.last_to_delete_mapsets = []
        self.last_to_delete_osu = []
        self.confirm_btn["state"] = "disabled"
        self._log("\n--- Started Delete rogue maps (scan only) ---\n")
        self.progress_label.config(text="Working…")
        self.progress["value"] = 0
        self.progress["maximum"] = 100

        self.worker = CollectionPruneWorker(params, self.out_queue)
        self.worker.start()

    # ---------------- Common helpers ----------------

    def _clear_tree(self):
        for i in self.tree.get_children():
            self.tree.delete(i)

    def _log(self, msg: str):
        self.log_text.insert("end", msg + "\n")
        self.log_text.see("end")

    def _poll_queue(self):
        try:
            while True:
                kind, payload = self.out_queue.get_nowait()
                if kind == "log":
                    self._log(payload)
                elif kind == "progress":
                    done, total = payload
                    pct = 0 if total == 0 else int(done * 100 / total)
                    self.progress["value"] = pct
                    self.progress_label.config(text=f"Parsing… {done}/{total} ({pct}%)")
                elif kind == "result":
                    self._on_result(payload)
        except queue.Empty:
            pass
        finally:
            self.after(80, self._poll_queue)

    def _perform_deletion(self, to_delete_mapsets: List[Path], to_delete_osu: List[BeatmapInfo]):
        total = len(to_delete_mapsets) + len(to_delete_osu)
        if total == 0:
            self._log("No targets to delete.")
            self.progress_label.config(text="Done (no targets)")
            return

        self.confirm_btn["state"] = "disabled"

        self._log(f"Starting permanent deletion of {total} target(s)...")
        self.progress["value"] = 0
        self.progress["maximum"] = total
        done = 0

        # Delete mapsets first
        for m in to_delete_mapsets:
            self._log(f"DELETE MAPSET: {m}")
            safe_delete_dir(m, log_cb=self._log)
            done += 1
            self.progress["value"] = done
            self.progress_label.config(text=f"Deleting… {done}/{total}")
            self.update_idletasks()

        # Then individual difficulties
        for bm in to_delete_osu:
            self._log(f"DELETE DIFF: {bm.osu_path}")
            safe_delete_file(bm.osu_path, log_cb=self._log)
            done += 1
            self.progress["value"] = done
            self.progress_label.config(text=f"Deleting… {done}/{total}")
            self.update_idletasks()

        self._log("Deletion complete.")
        self.progress_label.config(text="Done")
        self.last_to_delete_mapsets = []
        self.last_to_delete_osu = []

    def _on_confirm_delete(self):
        if not self.last_to_delete_mapsets and not self.last_to_delete_osu:
            self._log("No scanned targets to delete.")
            return
        # No modal popup; user reviews list and clicks this when ready
        self._perform_deletion(
            list(self.last_to_delete_mapsets),
            list(self.last_to_delete_osu)
        )

    def _on_result(self, payload: dict):
        to_delete_mapsets: List[Path] = payload.get("to_delete_mapsets", [])
        to_delete_osu: List[BeatmapInfo] = payload.get("to_delete_osu", [])

        self.last_to_delete_mapsets = to_delete_mapsets
        self.last_to_delete_osu = to_delete_osu

        # Fill tree with mapsets first (folder name only)
        for m in to_delete_mapsets:
            display_name = m.name  # e.g. "46502 goreshit - Satori De Pon!"
            self.tree.insert("", "end", values=("mapset", display_name))

        # Then difficulties, grouped as mapset name + diff name
        for bm in to_delete_osu:
            mapset_name = bm.mapset_dir.name
            diff_label = bm.version or bm.osu_path.name
            display_name = f"{mapset_name} [{diff_label}]"
            self.tree.insert("", "end", values=("difficulty", display_name))

        count = len(to_delete_mapsets) + len(to_delete_osu)
        self._log(f"--- Scan complete: {count} target(s) found ---")
        if count == 0:
            self.progress_label.config(text="Done (no targets)")
            self.confirm_btn["state"] = "disabled"
            return

        self.progress_label.config(text="Scan done")
        # enable confirmation button; user can scroll list before deciding
        self.confirm_btn["state"] = "normal"


# -----------------------------
# Main entry
# -----------------------------

def main():
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()
