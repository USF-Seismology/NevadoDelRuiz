#!/usr/bin/env python3
"""
Convert Yuly's sac_files folder, which has one folder per event with a SAC in each, into a
set of MiniSEED files with the same naming convention.
"""
import argparse
import logging
import re
from pathlib import Path
import numpy as np
from obspy import read, Stream

EVENT_DIR_RE = re.compile(r"^\d{4}\d{4}\.R([A-Z]{2})$")  # MMDDhhmm.R??

def fix_masked_arrays(st: Stream) -> None:
    for tr in st:
        if np.ma.isMaskedArray(tr.data):
            tr.data = tr.data.filled(np.nan).astype(tr.data.dtype, copy=False)

def pack_event_folder(folder: Path, outdir: Path, encoding: str = "FLOAT32") -> tuple[Path, Path]:
    sac_files = sorted(folder.glob("*.sac"))
    if not sac_files:
        raise FileNotFoundError(f"No SAC files found in {folder}")

    st = Stream()
    for f in sac_files:
        try:
            st += read(str(f))
        except Exception as e:
            logging.error("Failed reading %s: %s", f, e)

    if len(st) == 0:
        raise IOError(f"Could not read any SAC files in {folder}")

    # Sort for determinism.
    st.sort(keys=["network", "station", "location", "channel", "starttime"])

    # Ensure no masked arrays (prevents MiniSEED write errors).
    fix_masked_arrays(st)

    # Derive year from the first trace starttime.
    year = int(st[0].stats.starttime.year)

    outdir.mkdir(parents=True, exist_ok=True) 
    mseed_path = outdir / f"{year}_{folder.name}.mseed"
    png_path   = mseed_path.with_suffix(".png")
    
    for tr in st:
        print(tr)

    # Write MiniSEED
    try:
        st.write(str(mseed_path), format="MSEED", encoding=encoding)
        logging.info("Wrote %s", mseed_path)
    except:
        print('Failed to write - probably because of sampling rates')
        input('ENTER to continue')
        

    # Write plot PNG (equal_scale=False)
    try:
        st.plot(equal_scale=False, outfile=str(png_path))
        logging.info("Wrote %s", png_path)
    except Exception as e:
        logging.error("Failed to write plot for %s: %s", folder.name, e)

    return mseed_path, png_path

def main():
    p = argparse.ArgumentParser(description="Bundle SAC files per event folder into MiniSEED + PNG.")
    p.add_argument("root", type=Path, help="Root directory containing event folders (MMDDhhmm.R??).")
    p.add_argument("-o", "--outdir", type=Path, default=None,
                   help="Directory to write outputs (default: alongside each event folder).")
    p.add_argument("--encoding", default="FLOAT32",
                   help="MiniSEED encoding (default: FLOAT32; e.g., STEIM2).")
    p.add_argument("-v", "--verbose", action="store_true", help="Verbose logging.")
    args = p.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(levelname)s: %(message)s")

    root = args.root
    if not root.is_dir():
        raise NotADirectoryError(f"{root} is not a directory")

    out_base = args.outdir if args.outdir else root

    seen_suffixes = set()
    count = 0
    skipped = 0

    for child in sorted(root.iterdir()):
        if not child.is_dir():
            continue
        m = EVENT_DIR_RE.match(child.name)
        if not m:
            logging.debug("Skipping non-event folder: %s", child.name)
            continue

        suffix = m.group(1)
        seen_suffixes.add(suffix)

        try:
            pack_event_folder(child, out_base, encoding=args.encoding)
            count += 1
        except Exception as e:
            logging.error("Failed processing %s: %s", child.name, e)
            skipped += 1

    expected = {"VT", "HB", "LP"}
    extras = sorted(s for s in seen_suffixes if s not in expected)
    if extras:
        logging.warning("Found additional R?? suffixes besides VT/HB/LP: %s", ", ".join(extras))
    else:
        logging.info("Event suffixes seen: %s", ", ".join(sorted(seen_suffixes)) or "(none)")

    logging.info("Done. Wrote %d file(s), skipped %d.", count, skipped)

if __name__ == "__main__":
    main()
