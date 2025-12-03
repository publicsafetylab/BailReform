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
        self.path = get_path_prefix(self)
        if not self.args.by_top_charge:
            self.path += "los/"
        for i, piece in enumerate(self.path.split("/")[1:-1]):
            if not os.path.exists(
                "../" + "/".join(self.path.split("/")[1:-1][: i + 1])
            ):
                os.mkdir("../" + "/".join(self.path.split("/")[1:-1][: i + 1]))

    def run(self):
        rosters = get_roster_sample(self)

        # get los for sample
        df = pd.DataFrame(thread(self.get_bookings, rosters))
        df = apply_exclusions(self, df)
        df["cycle"] = df["first_seen"].apply(lambda date: find_date_range(self, date))
        df["los"] = (df["last_seen"] - df["first_seen"]).dt.days + 1
        df["los_indicator"] = df["los"].apply(lambda i: self.indicate(i))

        # if no top charge specified, get overall los
        if not self.args.by_top_charge:
            mx = self.prep_matrix(df)
            mx.to_csv(
                self.path + f"cutoff_{self.los_cutoff}_days.csv",
                index=False,
            )

        # otherwise, handle per-top-charge splits
        else:
            for charge in L1:
                res = df[df["charge"] == charge]
                mx = self.prep_matrix(res)

                # save output matrix to charge-specific path
                if not os.path.exists(
                    self.path + f"{charge.lower().replace(' ', '_')}"
                ):
                    os.makedirs(self.path + f"{charge.lower().replace(' ', '_')}")
                if not os.path.exists(
                    self.path + f"{charge.lower().replace(' ', '_')}/los"
                ):
                    os.makedirs(self.path + f"{charge.lower().replace(' ', '_')}/los")
                mx.to_csv(
                    self.path
                    + f"{charge.lower().replace(' ', '_')}/los/cutoff_{self.los_cutoff}_days.csv",
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
