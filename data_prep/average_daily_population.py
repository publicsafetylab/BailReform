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
        if self.args.demographics:
            self.path = f"../matrices/{self.args.state}/threshold_{str(self.args.threshold).replace('.', '_')}/cov/adp/"
        else:
            self.path = (
                f"../matrices/{self.args.state}/threshold_{str(self.args.threshold).replace('.', '_')}/no_cov"
                f"/adp/"
            )
        for i, piece in enumerate(self.path.split("/")[1:-1]):
            if not os.path.exists(
                "../" + "/".join(self.path.split("/")[1:-1][: i + 1])
            ):
                os.mkdir("../" + "/".join(self.path.split("/")[1:-1][: i + 1]))

    def run(self):
        if self.args.demographics:
            rosters = sorted(
                list(
                    pd.read_csv(
                        f"../tmp/threshold_{str(self.args.threshold).replace('.', '_')}/rosters_demographics.csv"
                    )["rosters"].unique()
                )
            )
        else:
            rosters = sorted(
                list(
                    pd.read_csv(
                        f"../tmp/threshold_{str(self.args.threshold).replace('.', '_')}/rosters.csv"
                    )["rosters"].unique()
                )
            )
        mxs = self.get_pops(rosters)
        for field, mx in mxs.items():
            mx = self.prep_matrix(mx, field)
            mx.to_csv(
                self.path + f"ad{field[0]}.csv",
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
        df["cycle"] = df["date"].apply(lambda date: self.find_date_range(date))
        pops = df.groupby(["state", "cycle"])["population"].mean().reset_index()
        admissions = df.groupby(["state", "cycle"])["admissions"].mean().reset_index()
        releases = df.groupby(["state", "cycle"])["releases"].mean().reset_index()
        return {"population": pops, "admissions": admissions, "releases": releases}

    def find_date_range(self, date):
        for i, start_date, end_date in self.cycles:
            if start_date <= date <= end_date:
                return i
        return None

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
