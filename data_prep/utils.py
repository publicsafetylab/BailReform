import argparse
import os
import pandas as pd

from datetime import datetime as dt
from datetime import timedelta as td
from dotenv import load_dotenv
from multiprocessing.pool import ThreadPool
from pymongo import MongoClient
from tqdm import tqdm


class MongoCollections:
    def __init__(self):
        load_dotenv()
        self.client = MongoClient(os.getenv("MONGO_READ_URI"))
        self.jdi = self.client.get_database("jdi")
        self.jdi_stats = self.client.get_database("jdi-stats")
        self.bookings = self.jdi.get_collection("jdi")
        self.scrape_dates = self.jdi_stats.get_collection("scrape-dates")
        self.pops = self.jdi_stats.get_collection("pops-by-date-jail")


def get_parser():
    parser = argparse.ArgumentParser()
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
    parser.add_argument(
        "-t",
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
    return parser


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

    # above missing scrape date threshold
    rosters = [
        r
        for r in rosters
        if len(pd.date_range(self.first, self.last).difference(r["missing_scrapes"]))
        / len(pd.date_range(self.first, self.last))
        >= self.args.threshold
    ]

    return list(r["_id"] for r in rosters)
