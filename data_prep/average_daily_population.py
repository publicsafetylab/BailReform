from utils import *


class ADP:
    def __init__(self, arguments):
        self.args = arguments
        self.first = dt.strptime(self.args.first, "%Y-%m-%d")
        self.last = dt.strptime(self.args.last, "%Y-%m-%d")
        self.dbs = MongoCollections()
        self.cycles = get_cycles(self)
        self.first = min([t[1] for t in self.cycles])
        self.last = max([t[2] for t in self.cycles])
        self.path = get_path_prefix(self)
        if not self.args.by_top_charge:
            self.path += "adp/"
        for i, piece in enumerate(self.path.split("/")[1:-1]):
            if not os.path.exists(
                "../" + "/".join(self.path.split("/")[1:-1][: i + 1])
            ):
                os.mkdir("../" + "/".join(self.path.split("/")[1:-1][: i + 1]))

    def run(self):
        rosters = get_roster_sample(self)

        # if no top charge specified, get overall adp, ada and adr
        if not self.args.by_top_charge:
            mxs = self.get_pops(rosters)
            for field, mx in mxs.items():
                mx = self.prep_matrix(mx, field)
                mx.to_csv(
                    self.path + f"ad{field[0]}.csv",
                    index=False,
                )

        # otherwise, handle per-top-charge splits
        # (note: these cannot be constructed from the pre-aggregated db)
        if self.args.by_top_charge:
            res = thread(self.get_one_roster_pops_by_top_charge, rosters, threads=10)
            df = pd.DataFrame(res)
            df = df.sort_values(by=["state", "roster", "date", "charge"])
            df["cycle"] = df["date"].apply(lambda date: find_date_range(self, date))

            # for each charge, get averages by year-month
            for charge in df["charge"].unique():
                tmp = df[df["charge"] == charge]
                pops = (
                    tmp.groupby(["state", "cycle"])["population"].mean().reset_index()
                )
                admissions = (
                    tmp.groupby(["state", "cycle"])["admissions"].mean().reset_index()
                )
                releases = (
                    tmp.groupby(["state", "cycle"])["releases"].mean().reset_index()
                )
                mxs = {
                    "population": pops,
                    "admissions": admissions,
                    "releases": releases,
                }

                # save output matrix to charge-specific path
                for field, tmp in mxs.items():
                    mx = tmp.pivot(
                        columns=["cycle"], index=["state"], values=field
                    ).T.reset_index()

                    if not os.path.exists(
                        self.path + f"{charge.lower().replace(' ', '_')}"
                    ):
                        os.makedirs(self.path + f"{charge.lower().replace(' ', '_')}")
                    if not os.path.exists(
                        self.path + f"{charge.lower().replace(' ', '_')}/adp"
                    ):
                        os.makedirs(
                            self.path + f"{charge.lower().replace(' ', '_')}/adp"
                        )
                    mx.to_csv(
                        self.path
                        + f"{charge.lower().replace(' ', '_')}/adp/ad{field[0]}.csv",
                        index=False,
                    )

    def get_pops(self, rosters):
        """
        produce monthly-averaged daily populations, admissions and releases
        (linearly interpolated) for each roster
        """
        df = pd.DataFrame(
            self.dbs.pops.aggregate(
                [
                    {
                        "$match": {
                            "Jail_ID": {"$in": rosters},
                            "Date": {"$gte": self.first, "$lte": self.last},
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "state": "$State",
                            "roster": "$Jail_ID",
                            "date": "$Date",
                            "features": "$features",
                        }
                    },
                    {"$unwind": "$features"},
                    {"$match": {"features.feature_id": "All-All-All-All"}},
                    {
                        "$project": {
                            "state": 1,
                            "roster": 1,
                            "date": 1,
                            "population": "$features.Population_Interpolated",
                            "admissions": "$features.Admissions_Interpolated",
                            "releases": "$features.Releases_Interpolated",
                        }
                    },
                ]
            )
        )

        # get average by year-month
        df = df.sort_values(by=["state", "roster", "date"])
        df["cycle"] = df["date"].apply(lambda date: find_date_range(self, date))
        pops = df.groupby(["state", "cycle"])["population"].mean().reset_index()
        admissions = df.groupby(["state", "cycle"])["admissions"].mean().reset_index()
        releases = df.groupby(["state", "cycle"])["releases"].mean().reset_index()
        return {"population": pops, "admissions": admissions, "releases": releases}

    def get_one_roster_pops_by_top_charge(self, roster):
        state, county = roster.split("-")

        def get_daily_traffic(date):
            daily_res = list()

            for charge in L1:
                pop = self.dbs.bookings.count_documents(
                    {
                        "meta.State": state,
                        "meta.County": county,
                        "Top_Charge": charge,
                        "meta.first_seen": {"$lte": date},
                        "meta.last_seen": {"$gte": date},
                    }
                )
                admissions = self.dbs.bookings.count_documents(
                    {
                        "meta.State": state,
                        "meta.County": county,
                        "Top_Charge": charge,
                        "meta.first_seen": date,
                    }
                )
                releases = self.dbs.bookings.count_documents(
                    {
                        "meta.State": state,
                        "meta.County": county,
                        "Top_Charge": charge,
                        "meta.last_seen": date,
                    }
                )
                daily_res.append(
                    {
                        "charge": charge,
                        "state": state,
                        "roster": roster,
                        "date": date,
                        "population": pop,
                        "admissions": admissions,
                        "releases": releases,
                    }
                )

            return daily_res

        results = thread(get_daily_traffic, pd.date_range(self.first, self.last))
        return results

    @staticmethod
    def prep_matrix(df, field):
        """
        reformat to matrix of populations, admissions or releases
        by state-year-month
        """
        matrix = df.pivot(
            columns=["cycle"], index=["state"], values=field
        ).T.reset_index()
        return matrix


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    ADP(args).run()
