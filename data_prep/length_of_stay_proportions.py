from utils import *


# TODO: - standardize admission date based on explicit field hierarchy


class LOS:
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
        self.los_cutoff = 3
        self.cycles = get_cycles(self)
        self.first = min([t[1] for t in self.cycles])
        self.last = max([t[2] for t in self.cycles])
        self.path = f"../matrices/{self.args.state}/los/threshold_{str(self.args.threshold).replace('.', '_')}/"
        for i, piece in enumerate(self.path.split("/")[1:-1]):
            if not os.path.exists(
                "../" + "/".join(self.path.split("/")[1:-1][: i + 1])
            ):
                os.mkdir("../" + "/".join(self.path.split("/")[1:-1][: i + 1]))

    def run(self):
        rosters = get_viable_rosters(self)

        df = pd.DataFrame(thread(self.get_bookings, rosters))
        df = self.apply_exclusions(df)

        df["cycle"] = df["first_seen"].apply(lambda date: self.find_date_range(date))

        df["los"] = (df["last_seen"] - df["first_seen"]).dt.days + 1
        df["los_indicator"] = df["los"].apply(lambda i: self.indicate(i))

        mx = self.prep_matrix(df)
        mx.to_csv(
            self.path + f"cutoff_{self.los_cutoff}_days.csv",
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

    def find_date_range(self, date):
        for i, start_date, end_date in self.cycles:
            if start_date <= date <= end_date:
                return i
        return None

    def indicate(self, i):
        if i > self.los_cutoff:
            return 1
        return 0

    @staticmethod
    def prep_matrix(df):
        """
        reformat to matrix of populations by state-year-month
        """
        admissions = (
            df.groupby(["state", "cycle"])
            .size()
            .reset_index()
            .rename(columns={0: "admissions"})
        )
        los = df.groupby(["state", "cycle"])["los_indicator"].sum().reset_index()
        aggregated = pd.merge(admissions, los, on=["state", "cycle"])
        aggregated["proportion"] = (
            aggregated[f"los_indicator"] / aggregated["admissions"]
        )
        matrix = aggregated.pivot(
            columns=["cycle"], index=["state"], values="proportion"
        ).T.reset_index()
        return matrix


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    LOS(args).run()
