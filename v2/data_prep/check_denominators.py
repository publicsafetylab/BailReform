"""Denominator counts: bookings grouped by state × cycle for each sample."""
import json
import pandas as pd
from pathlib import Path
from get_sample import iter_samples

OUTPUT = Path(__file__).resolve().parent.parent / "output"

for state_dir in sorted(OUTPUT.iterdir()):
    if not state_dir.is_dir():
        continue
    for thresh_dir in sorted(state_dir.iterdir()):
        if not thresh_dir.is_dir():
            continue
        manifest = json.load(open(thresh_dir / "manifest.json"))
        print(f"\n{manifest['state'].upper()}  threshold={manifest['threshold']}  "
              f"window={manifest['first']} → {manifest['last']}")

        for sample, sub, sub_dir, df in iter_samples(thresh_dir):
            out_dir = sub_dir / "denominators" / sub
            out_dir.mkdir(parents=True, exist_ok=True)
            pivot = df.groupby(["state", "cycle"]).size().unstack("state", fill_value=0)
            pivot.to_csv(out_dir / "denominators.csv")
            print(f"  {sample}/{sub}: {len(df)} → {out_dir / 'denominators.csv'}")
