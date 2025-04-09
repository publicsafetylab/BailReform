from utils import *


# TODO: - optimize rebooking flagger (takes ~20 min currently)
#       - standardize admission date based on explicit field hierarchy


class RebookingProportions:
    def __init__(self, arguments):
        self.args = arguments
        self.first = dt.strptime(self.args.first, "%Y-%m-%d")
        self.last = dt.strptime(self.args.last, "%Y-%m-%d")
        self.dbs = MongoCollections()
        self.flags = ["non_distinct_jdi_inmate_id"]
        pd.options.mode.chained_assignment = None
        self.cycles = get_cycles(self)
        self.first = min([t[1] for t in self.cycles])
        self.last = max([t[2] for t in self.cycles])
        self.path = f"../matrices/{self.args.state}/rebookings/threshold_{str(self.args.threshold).replace('.', '_')}/"
        for i, piece in enumerate(self.path.split("/")[1:-1]):
            if not os.path.exists(
                "../" + "/".join(self.path.split("/")[1:-1][: i + 1])
            ):
                os.mkdir("../" + "/".join(self.path.split("/")[1:-1][: i + 1]))
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
        rosters = get_viable_rosters(self)
        df = pd.DataFrame(thread(self.get_bookings, rosters))
        df = self.apply_exclusions(df)
        df["cycle"] = df["first_seen"].apply(lambda date: self.find_date_range(date))
        df = df.sort_values(by=["id_roster", "id_person", "first_seen"])
        df = self.flag_rebookings(df)

        # run through windows, abridge data as necessary
        # and output state-year-month matrix csvs
        for window in self.windows:
            mx = self.prep_matrix(df, window)
            mx.to_csv(
                self.path + f"within_{window}_days.csv",
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
                for window in self.windows:
                    if (
                        bookings[i + 1]["first_seen"] - bookings[i]["first_seen"]
                    ).days <= window:
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
        df = df[df["first_seen"] <= self.last - td(days=window)]
        admissions = (
            df.groupby(["state", "cycle"])
            .size()
            .reset_index()
            .rename(columns={0: "admissions"})
        )
        rebookings = df.groupby(["state", "cycle"])[f"rb_{window}"].sum().reset_index()
        aggregated = pd.merge(admissions, rebookings, on=["state", "cycle"])
        aggregated["proportion"] = aggregated[f"rb_{window}"] / aggregated["admissions"]
        matrix = aggregated.pivot(
            columns=["cycle"], index=["state"], values="proportion"
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
        Forward windows within which to check for rebookings
        """,
    )
    args = parser.parse_args()

    RebookingProportions(args).run()
