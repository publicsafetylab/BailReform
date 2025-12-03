import numpy as np

from utils import *


# TODO: - standardize admission date based on explicit field hierarchy


class Incapacitation:
    def __init__(self, arguments):
        self.args = arguments
        self.first = dt.strptime(self.args.first, "%Y-%m-%d")
        self.last = dt.strptime(self.args.last, "%Y-%m-%d")
        self.dbs = MongoCollections()
        self.flags = [
            "non_distinct_jdi_inmate_id",
            "left_intersects_gap",
            "right_intersects_gap",
            "intersects_first",
        ]
        pd.options.mode.chained_assignment = None
        self.cycles = get_cycles(self)
        self.first = min([t[1] for t in self.cycles])
        self.last = max([t[2] for t in self.cycles])

        self.path = get_path_prefix(self)
        if not self.args.by_top_charge:
            self.path += "inc/"
            self.path_piece = self.path + "piecewise/"
            self.path_cum = self.path + "cumulative/"

            for path in [self.path_piece, self.path_cum]:
                for i, piece in enumerate(path.split("/")[1:-1]):
                    if not os.path.exists(
                        "../" + "/".join(path.split("/")[1:-1][: i + 1])
                    ):
                        os.mkdir("../" + "/".join(path.split("/")[1:-1][: i + 1]))

        if self.args.state == "fl":
            start = START_FL
        elif self.args.state == "ga":
            start = START_GA
        else:
            raise ValueError("unidentified state")

        self.windows = [n * 28 for n in range(1, round((self.last - start).days / 28))]
        if self.args.windows:
            self.windows = self.args.windows

    def run(self):
        rosters = get_roster_sample(self)
        df = pd.DataFrame(thread(self.get_bookings, rosters))
        df = apply_exclusions(self, df)
        df["cycle"] = df["first_seen"].apply(lambda date: find_date_range(self, date))
        df["los"] = (df["last_seen"] - df["first_seen"]).dt.days + 1

        for w in self.windows:
            # handle piecewise window proportions
            window_range = (w - 27, w)
            df[f"prop_{window_range[0]}-{window_range[1]}"] = np.where(
                df["los"] < window_range[0], 0, -1
            )
            df[f"prop_{window_range[0]}-{window_range[1]}"] = np.where(
                df["los"] > window_range[1],
                1,
                df[f"prop_{window_range[0]}-{window_range[1]}"],
            )
            df[f"prop_{window_range[0]}-{window_range[1]}"] = np.where(
                (window_range[0] <= df["los"]) & (df["los"] <= window_range[1]),
                (df["los"] - window_range[0] + 1) / 28,
                df[f"prop_{window_range[0]}-{window_range[1]}"],
            )

            # handle cumulative window proportions
            df[f"prop_{w}"] = df["los"] / w
            df[f"prop_{w}"] = np.where(df[f"prop_{w}"] > 1, 1, df[f"prop_{w}"])
            assert df[f"prop_{w}"].min() > 0

        # run through windows, abridge data as necessary
        # and output state-year-month matrix csvs
        if not self.args.by_top_charge:
            for window in self.windows:
                mx_cumulative = self.prep_matrix_cumulative(df, window)
                mx_cumulative.to_csv(
                    self.path_cum + f"proportion_of_days_1_to_{window}.csv",
                    index=False,
                )
                mx_piecewise = self.prep_matrix_piecewise(df, window)
                mx_piecewise.to_csv(
                    self.path_piece
                    + f"proportion_of_days_{window - 27}_to_{window}.csv",
                    index=False,
                )

        else:
            for charge in L1:
                tmp = df[df["charge"] == charge]
                for window in self.windows:
                    mx_cumulative = self.prep_matrix_cumulative(tmp, window)
                    if not os.path.exists(
                        self.path
                        + f"{charge.lower().replace(' ', '_')}/inc/cumulative/"
                    ):
                        os.makedirs(
                            self.path
                            + f"{charge.lower().replace(' ', '_')}/inc/cumulative/"
                        )
                    mx_cumulative.to_csv(
                        self.path
                        + f"{charge.lower().replace(' ', '_')}/inc/cumulative/proportion_of_days_1_to_{window}.csv",
                        index=False,
                    )
                    mx_piecewise = self.prep_matrix_piecewise(tmp, window)
                    if not os.path.exists(
                        self.path + f"{charge.lower().replace(' ', '_')}/inc/piecewise/"
                    ):
                        os.makedirs(
                            self.path
                            + f"{charge.lower().replace(' ', '_')}/inc/piecewise/"
                        )
                    mx_piecewise.to_csv(
                        self.path
                        + f"{charge.lower().replace(' ', '_')}/inc/piecewise/proportion_of_days_{window - 27}_to_{window}.csv",
                        index=False,
                    )

    def get_bookings(self, roster):
        """
        select relevant fields from bookings from specified roster
        that meet date range criteria for admissions
        """
        state, county = roster.split("-", 1)
        match = {
            "meta.State": state,
            "meta.County": county,
            "meta.first_seen": {"$gte": self.first, "$lte": self.last},
        }
        if self.args.by_top_charge:
            match.update({"Top_Charge": {"$exists": True}})
        query = {
            "_id": 0,
            "id_booking": "$_id",
            "id_person": "$meta.jdi_inmate_id",
            "flags": "$meta.flags",
            "id_roster": "$meta.Jail_ID",
            "state": "$meta.State",
            "county": "$meta.County",
            "first_seen": "$meta.first_seen",
            "last_seen": "$meta.last_seen",
            "charge": "$Top_Charge",
        }
        return list(self.dbs.bookings.find(match, query))

    def prep_matrix_cumulative(self, df, window):
        """
        reformat to matrix of populations by state-year-month
        """
        df = df[df["first_seen"] <= self.last - td(days=window)]
        aggregated = (
            df.groupby(["state", "cycle"])[f"prop_{window}"].mean().reset_index()
        )
        matrix = aggregated.pivot(
            columns=["cycle"], index=["state"], values=f"prop_{window}"
        ).T.reset_index()
        return matrix

    def prep_matrix_piecewise(self, df, window):
        """
        reformat to matrix of populations by state-year-month
        """
        df = df[df["first_seen"] <= self.last - td(days=window)]
        aggregated = (
            df.groupby(["state", "cycle"])[f"prop_{window - 27}-{window}"]
            .mean()
            .reset_index()
        )
        matrix = aggregated.pivot(
            columns=["cycle"], index=["state"], values=f"prop_{window - 27}-{window}"
        ).T.reset_index()
        return matrix


if __name__ == "__main__":
    parser = get_parser()
    parser.add_argument(
        "-w",
        "--windows",
        type=int,
        nargs="*",
        help="""
        Forward windows within which to check for rebookings.
        """,
    )
    args = parser.parse_args()

    Incapacitation(args).run()
