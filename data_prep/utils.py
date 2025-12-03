import argparse
import os
import pandas as pd

from datetime import datetime as dt
from datetime import timedelta as td
from dotenv import load_dotenv
from multiprocessing.pool import ThreadPool
from pymongo import MongoClient
from tqdm import tqdm


START_FL = dt(2024, 1, 1, 0, 0)
START_GA = dt(2024, 7, 1, 0, 0)
CYCLE = 28


L1 = [
    "Violent",
    "Property",
    "Drug",
    "Public Order",
    "DUI Offense",
    "Criminal traffic",
]


class MongoCollections:
    def __init__(self):
        load_dotenv()
        self.client = MongoClient(os.getenv("MONGO_READ_URI"))
        self.jdi = self.client.get_database("jdi")
        self.jdi_stats = self.client.get_database("jdi-stats")
        self.bookings = self.jdi.get_collection("jdi")
        self.scrape_dates = self.jdi_stats.get_collection("scrape-dates")
        self.pops = self.jdi_stats.get_collection("pops-by-date-jail")


def get_cycles(self):
    if self.args.state == "fl":
        start = START_FL
    elif self.args.state == "ga":
        start = START_GA
    else:
        raise ValueError("unidentified state")

    # get cycles of n days after policy date
    cycles_fwd = list()
    for i in range(0, round(len(list(pd.date_range(start, self.last))) / CYCLE)):
        cycle_start = start + td(days=i * CYCLE)
        cycle_end = cycle_start + td(days=CYCLE - 1)
        if cycle_end > self.last:
            break
        cycles_fwd.append((i, cycle_start, cycle_end))

    # get cycles of n days before policy date
    cycles_bck = list()
    for i in range(
        0, round(len(list(pd.date_range(self.first, start - td(days=1)))) / CYCLE)
    ):
        cycle_end = start - td(days=1) - td(days=i * CYCLE)
        cycle_start = cycle_end - td(days=CYCLE - 1)
        if cycle_start < self.first:
            break
        cycles_bck.append((-i - 1, cycle_start, cycle_end))

    # combine and return cycles
    cycles = cycles_bck + cycles_fwd
    cycles = sorted(cycles, key=lambda t: t[1])
    return cycles


def get_parser():
    parser = argparse.ArgumentParser()

    # date range and geographic sample restriction arguments
    parser.add_argument(
        "-s",
        "--state",
        type=str,
        default="fl",
        choices=["fl", "ga"],
        help="""
            State for which to produce data
            (defaults to 'fl').
            """,
    )
    parser.add_argument(
        "-m",
        "--threshold",
        type=float,
        default=0.75,
        help="""
            Threshold below which to exclude rosters defined by the proportion of 
            dates in specified range with missing scrape data.
            """,
    )
    parser.add_argument(
        "-x",
        "--exclude",
        type=str,
        default=list(),
        nargs="*",
        help="""
                Exclude any admissions from specified list of states
                (specify as, e.g., `AL AR ...`).
                """,
    )
    parser.add_argument(
        "-f",
        "--first",
        type=str,
        default="2023-01-01",
        help="""
                Earliest date from which to collect admissions 
                (defaults to 2023-01-01).
                """,
    )
    parser.add_argument(
        "-l",
        "--last",
        type=str,
        default=dt.strftime(dt.now().replace(day=1) - td(days=1), "%Y-%m-%d"),
        help="""
                Latest date from which to collect admissions 
                (defaults to last day of previous month).
                """,
    )

    # covariate completeness sample restriction arguments
    parser.add_argument(
        "-r",
        "--sample",
        type=str,
        default="all",
        choices=["all", "charges", "demographics"],
        help="""
                Specifies the roster sample for which to produce matrices
                (all rosters, those including charge data, 
                or those including demographic and charge data).
        """,
    )
    parser.add_argument(
        "-btc",
        "--by_top_charge",
        action="store_true",
        help="""
            If specified, break out results matrices by top L1 charge
            (note: this can only be done ofr the charges and demographic samples).
        """,
    )

    # specific flag for saving roster samples
    parser.add_argument(
        "--save",
        action="store_true",
        help="""
            If specified, save roster list to local file
            (used in `get_roster_list.py`).
            """,
    )
    return parser


def get_viable_rosters(self):
    """
    collect list of rosters by id
    with format `"_id": "{state abbreviation}-{county}"`,
    e.g., "AL-Autauga", that meet inclusion criteria
    """
    rosters = list(self.dbs.scrape_dates.find({}))

    # exclude states
    for state in self.args.exclude:
        rosters = [r for r in rosters if not r["_id"].startswith(f"{state}-")]

    # span date range
    rosters = [
        r
        for r in rosters
        if r["first_scrape"] <= self.first and r["last_scrape"] >= self.last
    ]

    # above missing scrape date threshold for specified date range
    rosters = [
        r
        for r in rosters
        if len(
            pd.date_range(self.first, self.last).difference(
                [d for d in r["missing_scrapes"] if self.first <= d <= self.last]
            )
        )
        / len(pd.date_range(self.first, self.last))
        >= self.args.threshold
    ]

    return list(r["_id"] for r in rosters)


def thread(worker, jobs, threads=5):
    pool = ThreadPool(threads)
    results = list()
    for result in tqdm(pool.imap_unordered(worker, jobs), total=len(jobs)):
        if result and isinstance(result, list):
            results.extend([r for r in result if r])
        elif result:
            results.append(result)
    pool.close()
    pool.join()
    if results:
        return results


def get_roster_sample(self):
    if self.args.sample == "demographics":
        return sorted(
            list(
                pd.read_csv(
                    f"../tmp/threshold_{str(self.args.threshold).replace('.', '_')}/rosters_demographics.csv"
                )["rosters"].unique()
            )
        )
    elif self.args.sample == "charges":
        return sorted(
            list(
                pd.read_csv(
                    f"../tmp/threshold_{str(self.args.threshold).replace('.', '_')}/rosters_charges.csv"
                )["rosters"].unique()
            )
        )
    elif self.args.sample == "all":
        return sorted(
            list(
                pd.read_csv(
                    f"../tmp/threshold_{str(self.args.threshold).replace('.', '_')}/rosters.csv"
                )["rosters"].unique()
            )
        )


def get_path_prefix(self):
    path = f"../matrices/{self.args.state}/threshold_{str(self.args.threshold).replace('.', '_')}/"
    if self.args.sample == "demographics":
        path += "demographics/"
    elif self.args.sample == "charges":
        path += "charges/"
    elif self.args.sample == "all":
        path += "all/"
    if self.args.sample in ["demographics", "charges"]:
        if self.args.by_top_charge:
            path += "by_top_charge/"
        else:
            path += "all_charges/"
    elif self.args.by_top_charge:
        raise ValueError(
            "if using [-btc, --by_top_charge] argument, "
            "please select a roster sample [-r, --sample] from {charges, demographics}"
        )
    else:
        path += "all_charges/"
    return path


def find_date_range(self, date):
    for i, start_date, end_date in self.cycles:
        if start_date <= date <= end_date:
            return i
    return None


def apply_exclusions(self, df):
    """
    reduce set of bookings based on exclusion criteria
    (specific flags, type issues with `id_person`, etc.)
    """
    df["flags"] = df["flags"].astype(str)
    for flag in self.flags:
        df = df[~df["flags"].str.contains(flag)]
    del df["flags"]
    df = df[df["id_person"].notna()]
    return df[~df["id_person"].apply(lambda o: isinstance(o, list))]
