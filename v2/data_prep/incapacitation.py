"""Piecewise incapacitation: mean proportion of days in each future window
that bookings from a given cycle are still present, by state × cycle."""
import json
import numpy as np
import pandas as pd
from pathlib import Path
from get_sample import iter_samples, CYCLE

OUTPUT = Path(__file__).resolve().parent.parent / "output"


for state_dir in sorted(OUTPUT.iterdir()):
    if not state_dir.is_dir():
        continue
    for thresh_dir in sorted(state_dir.iterdir()):
        if not thresh_dir.is_dir():
            continue
        manifest = json.load(open(thresh_dir / "manifest.json"))
        max_offset = manifest.get("incapacitation_windows")
        if max_offset is None:
            continue
        cycles = {c[0]: c for c in manifest["cycles"]}
        max_cycle = max(cycles.keys())
        print(f"\n{manifest['state'].upper()}  threshold={manifest['threshold']}  "
              f"window={manifest['first']} → {manifest['last']}  "
              f"incapacitation_windows={max_offset}")

        # precompute cycle start ordinals
        cycle_starts = {}
        for ci, (_, start_str, _) in cycles.items():
            cycle_starts[ci] = pd.Timestamp(start_str).toordinal()

        for sample, sub, sub_dir, df in iter_samples(thresh_dir):
            out_dir = sub_dir / "incapacitation" / sub
            out_dir.mkdir(parents=True, exist_ok=True)

            df = df.copy()
            first_seen_ord = pd.to_datetime(df["first_seen"]).apply(lambda d: d.toordinal()).values
            los = df["length_of_stay"].values
            booking_end = first_seen_ord + los - 1  # last day present
            cycle_col = df["cycle"].values
            state_col = df["state"].values

            for w in range(1, max_offset + 1):
                valid_cycles = [c for c in cycles if c + w <= max_cycle]
                if not valid_cycles:
                    break

                mask = np.isin(cycle_col, valid_cycles)
                if not mask.any():
                    continue

                # vectorized: compute future window start/end for each booking
                future_starts = np.array([cycle_starts[c + w] for c in cycle_col[mask]])
                future_ends = future_starts + CYCLE - 1

                overlap_start = np.maximum(first_seen_ord[mask], future_starts)
                overlap_end = np.minimum(booking_end[mask], future_ends)
                overlap = np.maximum(overlap_end - overlap_start + 1, 0) / CYCLE

                tmp = pd.DataFrame({
                    "state": state_col[mask],
                    "cycle": cycle_col[mask],
                    "overlap": overlap,
                })
                pivot = tmp.groupby(["state", "cycle"])["overlap"].mean().unstack("state", fill_value=0)
                pivot.to_csv(out_dir / f"window_{w}.csv")

            n_files = len(list(out_dir.glob("window_*.csv")))
            print(f"  {sample}/{sub}: {n_files} window files → {out_dir}")
