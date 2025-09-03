#!/usr/bin/env python3
r"""
Walk D:\NevadoDelRuiz\suds_unzipped\ and convert *.WVR → MiniSEED using WinSUDS tools.
Skips any WVR whose target .mseed already exists.

Usage examples:
  python convert_wvr_to_mseed.py                                  # default input root
  python convert_wvr_to_mseed.py -o D:\NevadoDelRuiz\mseed_out    # mirror tree under output root
"""

import os
import sys
import glob
import subprocess
from pathlib import Path
import numpy as np
from obspy import read, Stream

# ---- CONFIG ----
DEFAULT_INPUT_ROOT = Path(r"D:\NevadoDelRuiz\suds_unzipped")
# WinSUDS assumed to be in a 'winsuds\bin' folder under the current working dir;
# change this if yours lives somewhere else:
WINSUDS_PATH = Path.cwd() / "winsuds" / "bin"
DEMUX = WINSUDS_PATH / "demux.exe"
IRIG = WINSUDS_PATH / "irig.exe"
SUD2SAC = WINSUDS_PATH / "sud2sac.exe"

def check_tools():
    missing = [p for p in (DEMUX, IRIG, SUD2SAC) if not p.exists()]
    if missing:
        print("[ERR] Missing WinSUDS tools:", ", ".join(str(m) for m in missing))
        sys.exit(1)

def fix_masked_arrays(st: Stream) -> None:
    """Convert masked arrays to normal arrays (fill with NaN) to avoid write errors."""
    for tr in st:
        if np.ma.isMaskedArray(tr.data):
            filled = tr.data.filled(np.nan)
            # keep dtype if possible
            tr.data = np.asarray(filled, dtype=tr.data.dtype)

def convert_one_event(wvr_path: Path, input_root: Path, output_root: Path | None):
    """
    Convert one WVR file:
      - run demux.exe → *.DMX
      - run irig.exe on DMX
      - run sud2sac.exe → SAC files
      - read SACs with ObsPy, write <base>.mseed
    Skips if <base>.mseed already exists at target.
    """
    base = wvr_path.stem            # e.g. 04-004
    cwd = wvr_path.parent           # folder where tools run
    # Where should output mseed go?
    if output_root:
        rel = cwd.relative_to(input_root) if input_root in cwd.parents or cwd == input_root else Path()
        outdir = output_root / rel
    else:
        outdir = cwd
    outdir.mkdir(parents=True, exist_ok=True)
    out_mseed = outdir / f"{base}.mseed"

    if out_mseed.exists():
        print(f"[SKIP] {wvr_path} → {out_mseed} already exists")
        return

    dmx = cwd / f"{base}.DMX"

    # Step 1: demux
    try:
        subprocess.run([str(DEMUX), str(wvr_path.name)], cwd=str(cwd), check=True)
    except subprocess.CalledProcessError as e:
        print(f"[ERR] demux failed for {wvr_path}: {e}")
        return

    if not dmx.exists():
        print(f"[WARN] Expected DMX not found: {dmx}")
        return

    # Step 2: IRIG timing
    try:
        subprocess.run([str(IRIG), str(dmx.name)], cwd=str(cwd), check=True)
    except subprocess.CalledProcessError as e:
        print(f"[ERR] irig failed for {dmx}: {e}")
        return

    # Step 3: SUD2SAC
    try:
        subprocess.run([str(SUD2SAC), str(dmx.name)], cwd=str(cwd), check=True)
    except subprocess.CalledProcessError as e:
        print(f"[ERR] sud2sac failed for {dmx}: {e}")
        return

    # Step 4: Merge SAC → MiniSEED
    # WinSUDS often writes files like "<base>.sac-STA-CHA" (lowercase 'sac'),
    # but let's accept multiple patterns just in case.
    sac_patterns = [f"{base}.sac-*", f"{base}.SAC-*"]
    sac_files = []
    for pat in sac_patterns:
        sac_files.extend(glob.glob(str(cwd / pat)))

    if not sac_files:
        print(f"[WARN] No SAC files found for base {base} in {cwd}")
        return

    st = Stream()
    for sac in sorted(sac_files):
        try:
            st += read(sac)
        except Exception as e:
            print(f"[ERR] Could not read {sac}: {e}")

    if len(st) == 0:
        print(f"[FAIL] No valid traces for {base}")
        return

    # Sort/clean, then write
    st.sort(keys=["network", "station", "location", "channel", "starttime"])
    fix_masked_arrays(st)
    try:
        # FLOAT32 preserves NaNs if any masked data was filled.
        st.write(str(out_mseed), format="MSEED", encoding="FLOAT32")
        print(f"[OK] Wrote {out_mseed} ({len(st)} trace(s))")
    except Exception as e:
        print(f"[ERR] Failed writing {out_mseed}: {e}")

def walk_and_convert(input_root: Path, output_root: Path | None):
    check_tools()
    count = skipped = 0
    # Walk all subfolders; match *.WVR (case-insensitive on Windows) by checking suffix
    for dirpath, _, filenames in os.walk(input_root):
        dir_p = Path(dirpath)
        for fname in filenames:
            if fname.lower().endswith(".wvr"):
                wvr = dir_p / fname
                # Determine target and skip quickly if mseed exists
                base = wvr.stem
                if output_root:
                    rel = dir_p.relative_to(input_root)
                    outdir = output_root / rel
                else:
                    outdir = dir_p
                out_mseed = outdir / f"{base}.mseed"
                if out_mseed.exists():
                    print(f"[SKIP] {wvr} → {out_mseed} already exists")
                    skipped += 1
                    continue

                print(f"[INFO] Processing: {wvr}")
                convert_one_event(wvr, input_root, output_root)
                count += 1
    print(f"[DONE] Submitted {count} WVR file(s). Skipped {skipped} existing.")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Convert WVR → MiniSEED using WinSUDS (skip if .mseed exists).")
    ap.add_argument("input_dir", nargs="?", default=str(DEFAULT_INPUT_ROOT),
                    help="Root directory to scan for WVR files (default: D:\\NevadoDelRuiz\\suds_unzipped)")
    ap.add_argument("-o", "--output_dir", help="Optional output root (mirrors substructure)")
    args = ap.parse_args()

    input_root = Path(args.input_dir)
    output_root = Path(args.output_dir) if args.output_dir else None

    if not input_root.exists():
        print(f"[ERR] Input root not found: {input_root}")
        sys.exit(1)

    walk_and_convert(input_root, output_root)
