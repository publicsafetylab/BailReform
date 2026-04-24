"""Rebooking rate: proportion of admissions with a rebooking within
28, 56, ..., 28*N days, by state × cycle."""
import json
import numpy as np
import pandas as pd
from pathlib import Path
from get_sample import iter_samples, CYCLE, CHARGE_HIERARCHY

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
        last_date = pd.Timestamp(manifest["last"])
        print(f"\n{manifest['state'].upper()}  threshold={manifest['threshold']}  "
              f"window={manifest['first']} → {manifest['last']}  "
              f"rebooking_windows={max_offset}")

        for sample, sub, sub_dir, df in iter_samples(thresh_dir):
            df = df.copy()
            df["first_seen_dt"] = pd.to_datetime(df["first_seen"])

            # for each booking, find days until next booking by same
            # person at same roster
            df = df.sort_values(["id_roster", "id_person", "first_seen_dt"])
            same = (df["id_roster"] == df["id_roster"].shift(-1)) & \
                   (df["id_person"] == df["id_person"].shift(-1))
            df["days_to_next"] = np.where(
                same,
                (df["first_seen_dt"].shift(-1) - df["first_seen_dt"]).dt.days,
                np.nan,
            )
            df["next_top_charge"] = np.where(same, df["top_charge"].shift(-1), None)

            if sample == "all":
                reb_cats = [("", None)]
            else:
                reb_cats = [("all_rebookings", None)] + [
                    (c.lower().replace(" ", "_"), c) for c in CHARGE_HIERARCHY
                ]

            for reb_slug, reb_charge in reb_cats:
                out_dir = sub_dir / "rebooking" / sub
                if reb_slug:
                    out_dir = out_dir / reb_slug
                out_dir.mkdir(parents=True, exist_ok=True)

                for w in range(1, max_offset + 1):
                    threshold_days = w * CYCLE
                    # only include bookings where the full rebooking window
                    # fits within the data (first_seen + threshold <= last_date)
                    eligible = df[
                        df["first_seen_dt"] + pd.Timedelta(days=threshold_days) <= last_date
                    ]
                    if eligible.empty:
                        continue

                    rebooked = (eligible["days_to_next"] <= threshold_days) & \
                               (eligible["days_to_next"] > 0)
                    if reb_charge is not None:
                        rebooked = rebooked & (eligible["next_top_charge"] == reb_charge)
                    tmp = pd.DataFrame({
                        "state": eligible["state"].values,
                        "cycle": eligible["cycle"].values,
                        "rebooked": rebooked.astype(int).values,
                    })
                    pivot = tmp.groupby(["state", "cycle"])["rebooked"].mean().unstack("state", fill_value=0)
                    pivot.to_csv(out_dir / f"within_{threshold_days}d.csv")

            print(f"  {sample}/{sub}: {max_offset} windows × {len(reb_cats)} rebooking splits → {sub_dir / 'rebooking' / sub}")
