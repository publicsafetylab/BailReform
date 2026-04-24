import argparse
import os

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime as dt
from datetime import timedelta as td
from dotenv import load_dotenv
from pymongo import MongoClient


POLICY_DATES = {
    "fl": dt(2024, 1, 1),
    "ga": dt(2024, 7, 1),
}

INCAPACITATION_WINDOWS = {
    "fl": 24,
    "ga": 18,
}

BOUND_FIRST = dt(2023, 1, 1)
BOUND_LAST = dt.now().replace(day=1) - td(days=1)

CYCLE = 28

CHARGE_HIERARCHY = [
    "Violent",
    "Property",
    "Drug",
    "Public Order",
    "DUI Offense",
    "Criminal traffic",
]


def get_cycles(policy_date, first, last):
    cycles = []

    i = 0
    while True:
        start = policy_date + td(days=i * CYCLE)
        end = start + td(days=CYCLE - 1)
        if end > last:
            break
        cycles.append((i, start, end))
        i += 1

    i = 1
    while True:
        end = policy_date - td(days=(i - 1) * CYCLE + 1)
        start = end - td(days=CYCLE - 1)
        if start < first:
            break
        cycles.append((-i, start, end))
        i += 1

    return sorted(cycles, key=lambda c: c[0])


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--state",
        type=str,
        required=True,
        choices=["fl", "ga"],
    )
    parser.add_argument("-f", "--first", type=str, default=None)
    parser.add_argument("-l", "--last", type=str, default=None)
    parser.add_argument("--pre_cycles", type=int, default=None)
    parser.add_argument("--post_cycles", type=int, default=None)
    parser.add_argument("-t", "--threshold", type=float, default=0.75)
    return parser


def get_mongo_client():
    load_dotenv()
    return MongoClient(os.getenv("MONGO_READ_URI"))


def get_viable_rosters(first, last, threshold):
    client = get_mongo_client()
    scrape_dates = client.get_database("jdi-stats").get_collection("scrape-dates")

    first_d = first.date()
    last_d = last.date()
    total_days = (last_d - first_d).days + 1
    viable = []
    for doc in scrape_dates.find({}):
        if doc["first_scrape"].date() > first_d or doc["last_scrape"].date() < last_d:
            continue
        missing_in_window = {
            d.date()
            for d in doc.get("missing_scrapes", [])
            if first_d <= d.date() <= last_d
        }
        coverage = (total_days - len(missing_in_window)) / total_days
        if coverage >= threshold:
            viable.append(doc["_id"])
    return sorted(viable)


def get_bookings(rosters, first, last, workers=8):
    client = get_mongo_client()
    bookings = client.get_database("jdi").get_collection("jdi")

    project = {
        "_id": 0,
        "id_booking": "$_id",
        "id_person": "$meta.jdi_inmate_id",
        "id_roster": "$meta.Jail_ID",
        "state": "$meta.State",
        "county": "$meta.County",
        "first_seen": "$meta.first_seen",
        "last_seen": "$meta.last_seen",
        "age": "$Age_Standardized",
        "race": "$Race_Ethnicity_Standardized",
        "gender": "$Sex_Gender_Standardized",
        "top_charge": "$Top_Charge",
        "num_charges": {
            "$size": {"$cond": [{"$isArray": "$Charges"}, "$Charges", []]}
        },
        "length_of_stay": {
            "$add": [
                {"$divide": [
                    {"$subtract": ["$meta.last_seen", "$meta.first_seen"]},
                    86400000,  # ms per day
                ]},
                1,
            ]
        },
    }

    def fetch(roster):
        state, county = roster.split("-", 1)
        return list(
            bookings.aggregate(
                [
                    {
                        "$match": {
                            "meta.State": state,
                            "meta.County": county,
                            "meta.first_seen": {"$gte": first, "$lte": last},
                            "meta.flags": {
                                "$nin": [
                                    "non_distinct_jdi_inmate_id",
                                    "intersects_first",
                                    "left_intersects_gap",
                                    "right_intersects_gap",
                                ]
                            },
                            "meta.jdi_inmate_id": {
                                "$exists": True,
                                "$ne": None,
                                "$not": {"$type": "array"},
                            },
                        }
                    },
                    {"$project": project},
                ]
            )
        )

    out = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for batch in pool.map(fetch, rosters):
            out.extend(batch)
    return out


def validate_charges(bookings):
    valid = set(CHARGE_HIERARCHY)
    unexpected = set()
    for b in bookings:
        tc = b.get("top_charge")
        if tc is not None and tc != "TBD":
            if tc not in valid:
                unexpected.add(tc)
    if unexpected:
        raise ValueError(f"Unexpected top_charge values: {unexpected}")


def standardize_fields(bookings):
    race_map = {"Other POC": "Other", "Indigenous": "Other", "AAPI": "Other"}
    gender_map = {"Trans": "Unknown Gender", "Nonbinary": "Unknown Gender"}
    for b in bookings:
        if b.get("race") in race_map:
            b["race"] = race_map[b["race"]]
        if b.get("gender") in gender_map:
            b["gender"] = gender_map[b["gender"]]
    return bookings


def assign_cycles(bookings, cycles):
    lookup = {}
    for i, start, end in cycles:
        for d in range((end - start).days + 1):
            lookup[(start + td(days=d)).date()] = i
    for b in bookings:
        b["cycle"] = lookup[b["first_seen"].date()]
    return bookings


def filter_charges(bookings):
    return [
        b for b in bookings
        if b.get("top_charge") is not None and b["top_charge"] != "TBD"
    ]


def filter_demographics(bookings):
    return [
        b for b in bookings
        if b.get("race") is not None and b["race"] != "Unknown Race"
        and b.get("gender") is not None and b["gender"] != "Unknown Gender"
        and b.get("age") is not None and b["age"] != "Unknown Age"
        and b.get("num_charges", 0) > 0
        and b.get("top_charge") is not None and b["top_charge"] != "TBD"
    ]


def iter_samples(thresh_dir):
    """Yield (sample_name, sub_name, sub_dir, df) for each sample/charge combo.

    For 'all': just the full sample.
    For 'charges' and 'demographics': full sample as 'all_charges',
    plus one per charge category.
    """
    import pandas as pd

    for sample in ["all", "charges", "demographics"]:
        path = thresh_dir / "bookings" / f"{sample}.csv"
        if not path.exists():
            continue
        df = pd.read_csv(path, low_memory=False)

        sub_dir = thresh_dir / "matrices" / sample
        yield sample, "all_charges", sub_dir, df
        if sample != "all":
            for charge in CHARGE_HIERARCHY:
                slug = charge.lower().replace(" ", "_")
                yield sample, slug, sub_dir, df[df["top_charge"] == charge]


def save_samples(bookings, state, threshold, cycles, rosters, output_dir=None):
    import json
    import pandas as pd
    from pathlib import Path

    if output_dir is None:
        t_str = str(threshold).replace(".", "_")
        output_dir = Path(__file__).resolve().parent.parent / "output" / state / f"t{t_str}"
    else:
        output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    samples = {
        "all": bookings,
        "charges": filter_charges(bookings),
        "demographics": filter_demographics(bookings),
    }

    bookings_dir = output_dir / "bookings"
    bookings_dir.mkdir(exist_ok=True)

    for name, data in samples.items():
        df = pd.DataFrame(data)
        df.to_csv(bookings_dir / f"{name}.csv", index=False)

    manifest = {
        "state": state,
        "threshold": threshold,
        "incapacitation_windows": INCAPACITATION_WINDOWS.get(state),
        "first": cycles[0][1].strftime("%Y-%m-%d"),
        "last": cycles[-1][2].strftime("%Y-%m-%d"),
        "n_cycles": len(cycles),
        "cycles": [[i, s.strftime("%Y-%m-%d"), e.strftime("%Y-%m-%d")] for i, s, e in cycles],
        "rosters": rosters,
        "counts": {name: len(data) for name, data in samples.items()},
        "counts_by_charge": {
            name: {
                charge: sum(1 for b in data if b.get("top_charge") == charge)
                for charge in CHARGE_HIERARCHY
            }
            for name in ("charges", "demographics")
            if name in samples
            for data in [samples[name]]
        },
        "created": dt.now().isoformat(),
    }
    with open(output_dir / "manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)

    return output_dir, samples


def resolve_bounds(args, policy_date):
    if args.first is not None and args.pre_cycles is not None:
        raise ValueError("--first and --pre_cycles are mutually exclusive")
    if args.last is not None and args.post_cycles is not None:
        raise ValueError("--last and --post_cycles are mutually exclusive")

    if args.first is not None:
        first = dt.strptime(args.first, "%Y-%m-%d")
    elif args.pre_cycles is not None:
        first = policy_date - td(days=args.pre_cycles * CYCLE)
    else:
        max_pre = (policy_date - BOUND_FIRST).days // CYCLE
        first = policy_date - td(days=max_pre * CYCLE)

    if args.last is not None:
        last = dt.strptime(args.last, "%Y-%m-%d")
    elif args.post_cycles is not None:
        last = policy_date + td(days=args.post_cycles * CYCLE - 1)
    else:
        max_post = (BOUND_LAST - policy_date + td(days=1)).days // CYCLE
        last = policy_date + td(days=max_post * CYCLE - 1)

    if first < BOUND_FIRST:
        raise ValueError(f"first {first:%Y-%m-%d} is before {BOUND_FIRST:%Y-%m-%d}")
    if last > BOUND_LAST:
        raise ValueError(f"last {last:%Y-%m-%d} is after {BOUND_LAST:%Y-%m-%d}")
    if first > policy_date - td(days=CYCLE):
        raise ValueError(
            f"first {first:%Y-%m-%d} must be at least one cycle before "
            f"policy date ({policy_date:%Y-%m-%d})"
        )
    if last < policy_date + td(days=CYCLE - 1):
        raise ValueError(
            f"last {last:%Y-%m-%d} must be at least one cycle after "
            f"policy date ({policy_date:%Y-%m-%d})"
        )
    inc_windows = INCAPACITATION_WINDOWS.get(args.state)
    if inc_windows is not None:
        min_last = policy_date + td(days=(inc_windows + 1) * CYCLE - 1)
        if last < min_last:
            n_post = (last - policy_date + td(days=1)).days // CYCLE
            raise ValueError(
                f"need at least {inc_windows + 1} post-treatment cycles for "
                f"{inc_windows} incapacitation windows, but only have {n_post} "
                f"(last={last:%Y-%m-%d}, need {min_last:%Y-%m-%d})"
            )
    return first, last


if __name__ == "__main__":
    args = get_parser().parse_args()
    policy_date = POLICY_DATES[args.state]
    first, last = resolve_bounds(args, policy_date)
    cycles = get_cycles(policy_date, first, last)
    coverage_first, coverage_last = cycles[0][1], cycles[-1][2]

    print(f"\n── {args.state.upper()} (policy: {policy_date:%Y-%m-%d}) ──")
    print(f"  window: {coverage_first:%Y-%m-%d} → {coverage_last:%Y-%m-%d}")
    print(f"  cycles: {len(cycles)} ({cycles[0][0]} to {cycles[-1][0]})")

    rosters = get_viable_rosters(coverage_first, coverage_last, args.threshold)
    print(f"  rosters: {len(rosters)} (threshold {args.threshold})")

    bookings = get_bookings(rosters, coverage_first, coverage_last)
    print(f"  bookings: {len(bookings)}")

    standardize_fields(bookings)
    validate_charges(bookings)
    assign_cycles(bookings, cycles)

    output_dir, samples = save_samples(
        bookings, args.state, args.threshold, cycles, rosters,
    )

    for name, data in samples.items():
        print(f"  {name}: {len(data)}")
    print(f"\n  saved to {output_dir}")
