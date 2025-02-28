from utils import *


class ADP:
    def __init__(self, arguments):
        self.args = arguments
        self.first = dt.strptime(self.args.first, "%Y-%m-%d")
        self.last = dt.strptime(self.args.last, "%Y-%m-%d")
        self.dbs = MongoCollections()

    def run(self):
        rosters = get_viable_rosters(self)
        df = self.get_pops(rosters)
        mx = self.prep_matrix(df)
        mx.to_csv(
            f"../matrices/adp/threshold_{str(self.args.threshold).replace('.', '_')}/adp.csv",
            index=False,
        )

    def get_pops(self, rosters):
        """
        produce monthly-averaged daily populations
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
                        }
                    },
                ]
            )
        )

        # get average by year-month
        df = df.sort_values(by=["state", "roster", "date"])
        df["month"] = df["date"].dt.month
        df["year"] = df["date"].dt.year
        df = df.groupby(["state", "year", "month"])["population"].mean().reset_index()

        return df

    @staticmethod
    def prep_matrix(df):
        """
        reformat to matrix of populations by state-year-month
        """
        matrix = df.pivot(
            columns=["year", "month"], index=["state"], values="population"
        ).T
        return matrix


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    ADP(args).run()
