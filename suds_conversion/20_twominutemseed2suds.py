#!/usr/bin/env python3
r"""
Convert 2-minute MiniSEED segments to SDS dayfiles.

- Walks INPUT_ROOT recursively for *.mseed
- Applies dataset-specific header fixes (IRIG removal, network/channel tweaks)
- Splits traces at UTC midnight so each piece fits in a single day
- Merges per (NET.STA.LOC.CHA, YYYY, JJJ) and writes:
    SDS_ROOT/YYYY/NET/STA/CHA.D/NET.STA.LOC.CHA.D.YYYY.JJJ

Example:
  python mseed_2min_to_sds.py ^
      -i "D:\NevadoDelRuiz\suds_unzipped" ^
      -o "D:\NevadoDelRuiz\SDS" ^
      -n NR --encoding FLOAT32 -v
"""

from pathlib import Path
import argparse
import sys
from collections import defaultdict
import numpy as np
from obspy import read, Stream, UTCDateTime

# ------------------------- dataset-specific fixes -------------------------
def fix_seedid(st: Stream, network: str = "NR") -> Stream:
    """
    Adapted from your legacy function:
    - Drop IRIG traces
    - Set network
    - If station names end with Z/N/E or L/H variants, coerce to EH?/EL? channels
      and trim station to 3 chars when there are multiple per base-code.
    """
    out = Stream()
    # Work on a copy of stats to avoid side-effects
    # We'll need to query multiple stations within the same stream,
    # so keep a reference to the original stream for selection logic.
    stations = [tr.stats.station for tr in st]
    for tr in st:
        if tr.stats.station == "IRIG":
            continue
        tr = tr.copy()
        tr.stats.network = network

        # Base 3-char station root
        root3 = tr.stats.station[:3]
        # Are there multiple stations sharing the same 3-char root present?
        multi_for_root = sum(s.startswith(root3) for s in stations) > 1

        # If station ends with Z/N/E and there are multiple per root, use EH?
        if tr.stats.station[-1:] in "ZNE" and multi_for_root:
            tr.stats.channel = "EH" + tr.stats.station[-1]
            tr.stats.station = root3
        else:
            # default to vertical if unknown
            if len(tr.stats.channel) != 3 or tr.stats.channel[2] not in "ZNE":
                tr.stats.channel = "EHZ"

        # If station is 4 chars and ends with L/H, normalize to 3-char base
        if len(tr.stats.station) == 4:
            if tr.stats.station[-1] == "L" and multi_for_root:
                # Low-gain: ELZ/ELE/ELN to parallel EH?
                tr.stats.channel = "EL" + tr.stats.channel[2]
                tr.stats.station = root3
            elif tr.stats.station[-1] == "H" and multi_for_root:
                tr.stats.station = root3

        # Ensure location is always a string (empty for none -> SDS uses '..')
        tr.stats.location = (tr.stats.location or "").strip()

        out.append(tr)
    return out

def split_trace_at_midnights(tr):
    """
    Yield copies of `tr` split so each piece lies entirely within a single UTC day.
    """
    parts = []
    cur = tr
    while True:
        st = cur.stats.starttime
        et = cur.stats.endtime
        day_start = UTCDateTime(st.year, st.month, st.day)
        day_end = day_start + 86400
        if et <= day_end:
            parts.append(cur)
            break
        # piece for this day
        left = cur.copy().trim(starttime=st, endtime=day_end, pad=False)
        parts.append(left)
        # remainder for next day
        cur = cur.copy().trim(starttime=day_end, endtime=et, pad=False)
    return parts

def round_sampling_rates(st: Stream):
    """
    Some files carry slightly non-integer sr (e.g. 99.9997). Round near-integers.
    """
    for tr in st:
        sr = tr.stats.sampling_rate
        if abs(sr - round(sr)) < 1e-6:
            tr.stats.sampling_rate = float(round(sr))

def fix_masked_arrays(st: Stream):
    """Fill masked arrays with NaN to avoid MiniSEED writer errors."""
    for tr in st:
        if np.ma.isMaskedArray(tr.data):
            tr.data = np.asarray(tr.data.filled(np.nan), dtype="float32")

# ------------------------------ SDS helpers ------------------------------
def sds_dir_for(tr, sds_root: Path, year: int):
    return sds_root / f"{year:04d}" / tr.stats.network / tr.stats.station / f"{tr.stats.channel}.D"

def sds_path_for(tr, sds_root: Path, year: int, jday: int):
    # tr.id => NET.STA.LOC.CHA ; empty LOC will appear as double dot in filename
    return sds_dir_for(tr, sds_root, year) / f"{tr.id}.D.{year:04d}.{jday:03d}"

# ------------------------------ main logic ------------------------------
def process(input_root: Path, sds_root: Path, network: str, encoding: str = "FLOAT32", verbose: bool = False):
    # Bins: (net, sta, loc, cha, year, jday) -> Stream
    bins: dict[tuple, Stream] = defaultdict(Stream)
    total_files = 0
    read_ok = 0

    # Find all *.mseed (case-insensitive; we check suffix lower)
    files = sorted(p for p in input_root.rglob("*") if p.is_file() and p.suffix.lower() == ".mseed")
    if verbose:
        print(f"[INFO] Found {len(files)} MiniSEED files under {input_root}")

    for f in files:
        total_files += 1
        try:
            st = read(str(f))
        except Exception as e:
            if verbose:
                print(f"[WARN] Could not read {f}: {e}")
            continue
        read_ok += 1

        st = fix_seedid(st, network=network)
        round_sampling_rates(st)
        fix_masked_arrays(st)
        st.sort(keys=["network", "station", "location", "channel", "starttime"])

        for tr in st:
            # Ensure location string not None
            tr.stats.location = (tr.stats.location or "").strip()

            # Split at midnights; bin by (id, day)
            for seg in split_trace_at_midnights(tr):
                y = seg.stats.starttime.year
                j = seg.stats.starttime.julday
                key = (seg.stats.network, seg.stats.station, seg.stats.location, seg.stats.channel, y, j)
                bins[key].append(seg)

    if verbose:
        print(f"[INFO] Read OK: {read_ok}/{total_files}. Writing SDS to {sds_root}")

    # Write each bin as a dayfile
    wrote = 0
    for (net, sta, loc, cha, y, j), st in sorted(bins.items()):
        # Merge segments (keep gaps as is; don't fill with zeros by default)
        try:
            st.merge(method=1)  # 1 = don't fill gaps, combine contiguous
        except Exception:
            # some sr jitter: attempt round & merge again
            round_sampling_rates(st)
            st.merge(method=1)

        # Use the first trace's stats for path components
        tr0 = st[0]
        tr0.stats.network = net
        tr0.stats.station = sta
        tr0.stats.location = loc
        tr0.stats.channel = cha

        out_dir = sds_dir_for(tr0, sds_root, y)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = sds_path_for(tr0, sds_root, y, j)

        if verbose:
            print(f"[WRITE] {net}.{sta}.{loc}.{cha}  {y}.{j:03d}  ->  {out_path}")

        # Write one Stream containing all segments for the day
        try:
            st.write(str(out_path), format="MSEED", encoding=encoding)
            wrote += 1
        except Exception as e:
            print(f"[ERR] Failed writing {out_path}: {e}")

    print(f"[DONE] Wrote {wrote} SDS dayfile(s) to {sds_root}. Read {read_ok}/{total_files} files.")

# ------------------------------ CLI ------------------------------
def main():
    ap = argparse.ArgumentParser(description="Convert 2-minute MiniSEED segments to SDS dayfiles.")
    ap.add_argument("-i", "--input_root", default=r"D:\NevadoDelRuiz\suds_unzipped",
                    help="Root directory containing 2-min MiniSEED files (default: D:\\NevadoDelRuiz\\suds_unzipped)")
    ap.add_argument("-o", "--sds_root", default=r"D:\NevadoDelRuiz\SDS",
                    help="Top-level SDS output directory (default: D:\\NevadoDelRuiz\\SDS)")
    ap.add_argument("-n", "--network", default="NR", help="Network code to set (default: NR)")
    ap.add_argument("--encoding", default="FLOAT32", help="MiniSEED encoding for output (FLOAT32 or STEIM2; default: FLOAT32)")
    ap.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    args = ap.parse_args()

    input_root = Path(args.input_root)
    sds_root = Path(args.sds_root)
    if not input_root.exists():
        print(f"[ERR] Input root not found: {input_root}")
        sys.exit(1)

    process(input_root, sds_root, args.network, args.encoding, args.verbose)

if __name__ == "__main__":
    main()
