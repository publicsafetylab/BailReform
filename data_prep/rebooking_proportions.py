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

        self.path = get_path_prefix(self)
        if not self.args.by_top_charge and not self.args.by_rebooking_top_charge:
            self.path += "reb/"

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
        rosters = get_roster_sample(self)
        df = pd.DataFrame(thread(self.get_bookings, rosters))
        df = apply_exclusions(self, df)
        df["cycle"] = df["first_seen"].apply(lambda date: find_date_range(self, date))
        df = df.sort_values(by=["id_roster", "id_person", "first_seen"])
        df = self.flag_rebookings(df)

        if not self.args.by_top_charge and not self.args.by_rebooking_top_charge:
            for window in self.windows:
                mx = self.prep_matrix(df, window)
                if not os.path.exists(self.path + f"all_rebookings/"):
                    os.mkdir(self.path + f"all_rebookings/")
                mx.to_csv(
                    self.path + f"all_rebookings/within_{window}_days.csv",
                    index=False,
                )

        # run through windows, abridge data as necessary
        # and output state-year-month matrix csvs
        elif self.args.by_top_charge and not self.args.by_rebooking_top_charge:
            for charge in L1:
                tmp = df[df["charge"] == charge]
                for window in self.windows:
                    mx = self.prep_matrix(tmp, window)
                    if not os.path.exists(
                        self.path
                        + f"{charge.lower().replace(' ', '_')}/reb/all_rebookings/"
                    ):
                        os.makedirs(
                            self.path
                            + f"{charge.lower().replace(' ', '_')}/reb/all_rebookings/"
                        )
                    mx.to_csv(
                        self.path
                        + f"{charge.lower().replace(' ', '_')}/reb/all_rebookings/within_{window}_days.csv",
                        index=False,
                    )

        elif self.args.by_rebooking_top_charge and not self.args.by_top_charge:
            for charge in L1:
                drop_cols = [
                    col
                    for col in df.columns
                    if col.startswith("rb_") and not col.endswith(f"_{charge}")
                ]
                tmp = df[[col for col in df.columns if col not in drop_cols]]
                tmp.columns = tmp.columns.str.removesuffix(f"_{charge}")
                for window in self.windows:
                    mx = self.prep_matrix(tmp, window)
                    if not os.path.exists(
                        self.path + f"reb/by_rebooking_top_charge/"
                        f"{charge.lower().replace(' ', '_')}"
                    ):
                        os.makedirs(
                            self.path
                            + f"reb/by_rebooking_top_charge/{charge.lower().replace(' ', '_')}"
                        )
                    mx.to_csv(
                        self.path
                        + f"reb/by_rebooking_top_charge/{charge.lower().replace(' ', '_')}/within_{window}_days.csv",
                        index=False,
                    )

        else:
            for charge in L1:
                tmp = df[df["charge"] == charge]
                for c in L1:
                    drop_cols = [
                        col
                        for col in tmp.columns
                        if col.startswith("rb_") and not col.endswith(f"_{c}")
                    ]
                    t = tmp[[col for col in tmp.columns if col not in drop_cols]]
                    t.columns = t.columns.str.removesuffix(f"_{c}")
                    for window in self.windows:
                        mx = self.prep_matrix(t, window)
                        if not os.path.exists(
                            self.path
                            + f"{charge.lower().replace(' ', '_')}/reb/by_rebooking_top_charge/"
                            f"{c.lower().replace(' ', '_')}/"
                        ):
                            os.makedirs(
                                self.path
                                + f"{charge.lower().replace(' ', '_')}/reb/by_rebooking_top_charge/"
                                f"{c.lower().replace(' ', '_')}/"
                            )
                        mx.to_csv(
                            self.path
                            + f"{charge.lower().replace(' ', '_')}/reb/by_rebooking_top_charge/"
                            f"{c.lower().replace(' ', '_')}/within_{window}_days.csv",
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
        if self.args.sample == "charges":
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
            "charge": "$Top_Charge",
        }

        return list(self.dbs.bookings.find(match, query))

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

                # if grouping by rebooking charge downstream,
                # indicate rebooking within window for each L1 charge type
                if self.args.by_rebooking_top_charge:
                    for window in self.windows:
                        for charge in L1:
                            if (
                                bookings[i + 1]["charge"] == charge
                                and (
                                    bookings[i + 1]["first_seen"]
                                    - bookings[i]["first_seen"]
                                ).days
                                <= window
                            ):
                                r.update({f"rb_{window}_{charge}": 1})
                            else:
                                r.update({f"rb_{window}_{charge}": 0})

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
    parser.add_argument(
        "-brtc",
        "--by_rebooking_top_charge",
        action="store_true",
        help="""
        If specified, split out results based on charge type of rebooking, not initial booking
        """,
    )
    args = parser.parse_args()

    RebookingProportions(args).run()
