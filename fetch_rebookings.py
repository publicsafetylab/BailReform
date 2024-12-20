import argparse
import numpy as np
import pandas as pd

from datetime import datetime as dt
from datetime import timedelta as td

from utils import MongoCollections, thread


# TODO: - optimize rebooking flagger (takes ~20 min currently)
#       - standardize admission date based on explicit field hierarchy


class FetchRebookings:
    def __init__(self, arguments):
        self.args = arguments
        self.first = dt.strptime(self.args.first, "%Y-%m-%d")
        self.last = dt.strptime(self.args.last, "%Y-%m-%d")
        self.dbs = MongoCollections()
        self.flags = ["non_distinct_jdi_inmate_id"]
        pd.options.mode.chained_assignment = None

    def run(self):
        rosters = self.get_viable_rosters()
        df = pd.DataFrame(thread(self.get_bookings, rosters))
        df["month"] = df["first_seen"].dt.month
        df["year"] = df["first_seen"].dt.year
        df = self.apply_exclusions(df)
        df = df.sort_values(by=["id_roster", "id_person", "first_seen"])
        df = self.flag_rebookings(df)

        # run through windows, abridge data as necessary
        # and output state-year-month matrix csvs
        for window in self.args.windows:
            mx = self.prep_matrix(df, window)
            mx.to_csv(f"rebookings/within_{window}_days.csv", index=False)

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
            if len(
                pd.date_range(self.first, self.last).difference(r["missing_scrapes"])
            )
            / len(pd.date_range(self.first, self.last))
            >= self.args.threshold
        ]

        return list(r["_id"] for r in rosters)

    def get_bookings(self, roster):
        """
        select relevant fields from bookings from specified roster
        that meet date range criteria for admissions
        """
        state, county = roster.split("-", 1)
        match = {
            "meta.State": state,
            "meta.County": county,
            "meta.first_seen": {"$gte": self.first, "$lte": self.last}
        }
        query = {
            "_id": 0,
            "id_booking": "$_id",
            "id_person": "$meta.jdi_inmate_id",
            "flags": "$meta.flags",
            "id_roster": "$meta.Jail_ID",
            "state": "$meta.State",
            "county": "$meta.County",
            "first_seen": "$meta.first_seen",
        }
        return list(self.dbs.bookings.find(match, query))

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

    def flag_rebookings(self, df):
        """
        for each person, indicate rebookings within windows of
        n days as specified in `args`
        """
        grouped = df.groupby(["id_roster", "id_person"])

        def get_person_rebookings(group):
            name, group = group
            bookings = group.to_dict("records")
            rs = list()

            for i in range(len(bookings) - 1):
                r = {"id_booking": bookings[i]["id_booking"]}
                for window in self.args.windows:
                    if (bookings[i + 1]["first_seen"] - bookings[i]["first_seen"]).days <= window:
                        r.update({f"rb_{window}": 1})
                rs.append(r)

            return rs

        rebookings = thread(get_person_rebookings, grouped)
        rebookings = pd.DataFrame(rebookings)

        # merge rebooking flags into original df
        df = pd.merge(df, rebookings, how="left", on="id_booking")

        for col in df.columns:
            if col.startswith("rb_"):
                df[col] = df[col].fillna(0)
                df[col] = df[col].astype(int)

        return df

    def prep_matrix(self, df, window):
        """
        reduce span of admissions to account for
        look-forward window for rebookings,
        then aggregate to proportions by state-year-month
        """
        df = df[df["first_seen"] < self.last - td(days=window)]
        admissions = df.groupby(["state", "year", "month"]).size().reset_index().rename(columns={0: "admissions"})
        rebookings = df.groupby(["state", "year", "month"])[f"rb_{window}"].sum().reset_index()
        aggregated = pd.merge(admissions, rebookings, on=["state", "year", "month"])
        aggregated["proportion"] = aggregated[f"rb_{window}"] / aggregated["admissions"]
        matrix = aggregated.pivot(columns=["year", "month"], index=["state"], values="proportion").T
        return matrix


if __name__ == "__main__":
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
        default=0,
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
        """
    )
    parser.add_argument(
        "-w",
        "--windows",
        type=int,
        default=[90],
        nargs="*",
        help="""
        Forward windows within which to check for rebookings
        (defaults to 90 days, specify as, e.g., `30 60 90 ...`).
        """
    )
    args = parser.parse_args()

    FetchRebookings(args).run()
