"""Proportion of admissions with LOS > 3 days, by state × cycle."""
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
            out_dir = sub_dir / "incarceration" / sub
            out_dir.mkdir(parents=True, exist_ok=True)
            grouped = df.groupby(["state", "cycle"])
            counts = grouped.size()
            detained = grouped["length_of_stay"].apply(lambda x: (x > 3).sum())
            prop = (detained / counts).unstack("state", fill_value=0)
            prop.to_csv(out_dir / "incarceration.csv")
            print(f"  {sample}/{sub}: {out_dir / 'incarceration.csv'}")
