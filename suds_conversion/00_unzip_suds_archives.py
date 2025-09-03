#!/usr/bin/env python
r"""
Unzip all .zip files in D:\NevadoDelRuiz\suds_zipped\
into D:\NevadoDelRuiz\suds_unzipped\<zipname>\

- Each .zip gets its own folder named after the zip file (without extension).
- If that folder already exists, the archive is skipped.
- Only top-level *.zip files are processed (no recursion).
"""

import zipfile
from pathlib import Path

SRC = Path(r"D:\NevadoDelRuiz\suds_zipped")    # input folder with .zip files
DEST = Path(r"D:\NevadoDelRuiz\suds_unzipped") # output folder for extracted files
DEST.mkdir(parents=True, exist_ok=True)

def main():
    zips = sorted(SRC.glob("*.zip"))  # only matches *.zip
    if not zips:
        print(f"No .zip files found in {SRC}")
        return

    for zip_path in zips:
        subdir = DEST / zip_path.stem
        if subdir.exists():
            print(f"Skipping {zip_path.name}: output folder {subdir} already exists")
            continue

        subdir.mkdir(parents=True, exist_ok=False)
        print(f"Extracting {zip_path.name} → {subdir}")

        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(subdir)
                print(f"  Extracted {len(zf.infolist())} files")
        except zipfile.BadZipFile:
            print(f"[WARN] Skipping corrupt archive: {zip_path.name}")

    print("All zip files processed.")

if __name__ == "__main__":
    main()
