from utils import *


class GetRosters:
    def __init__(self, arguments):
        self.args = arguments
        self.first = dt.strptime(self.args.first, "%Y-%m-%d")
        self.last = dt.strptime(self.args.last, "%Y-%m-%d")
        self.dbs = MongoCollections()
        self.cycles = get_cycles(self)
        self.first = min([t[1] for t in self.cycles])
        self.last = max([t[2] for t in self.cycles])
        self.path = f"../tmp/threshold_{str(self.args.threshold).replace('.', '_')}/"
        for i, piece in enumerate(self.path.split("/")[1:-1]):
            if not os.path.exists(
                "../" + "/".join(self.path.split("/")[1:-1][: i + 1])
            ):
                os.mkdir("../" + "/".join(self.path.split("/")[1:-1][: i + 1]))

    def run(self):
        if self.args.demographics:
            rosters = self.get_viable_rosters_demographics()
        else:
            rosters = self.get_viable_rosters()

        if args.save:
            df = pd.DataFrame(rosters, columns=["rosters"])
            if self.args.demographics:
                df.to_csv(self.path + "rosters_demographics.csv", index=False)
            else:
                df.to_csv(self.path + "rosters.csv", index=False)

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

        # above missing scrape date threshold for specified date range
        rosters = [
            r
            for r in rosters
            if len(
                pd.date_range(self.first, self.last).difference(
                    [d for d in r["missing_scrapes"] if self.first <= d <= self.last]
                )
            )
            / len(pd.date_range(self.first, self.last))
            >= self.args.threshold
        ]

        return sorted(list(r["_id"] for r in rosters))

    def get_viable_rosters_demographics(self):
        """
        collects list of rosters by id
        with format `"_id": "{state abbreviation}-{county}"`,
        e.g., "AL-Autauga", that meet inclusion criteria:
        the intersection of rosters for which bookings contain
        at least one non-missing/unknown value of
        each specified demographic field.
        """
        rosters = self.get_viable_rosters()
        bookings = thread(self.get_demographics, rosters)
        df = pd.DataFrame(bookings)
        df["roster"] = df["state"] + "-" + df["county"]
        df = df[df["race"] != "Unknown Race"]
        df = df[df["gender"] != "Unknown Gender"]
        df = df[df["top_charge"] != "TBD"]
        df = df[df["age"].notna()]
        df = df[df["age"] != "Unknown Age"]
        df = df[df["num_charges"] > 0]
        return sorted(list(df["roster"].unique()))

    def get_demographics(self, roster):
        """
        TODO: fill in
        """
        state, county = roster.split("-", 1)
        match = {
            "meta.State": state,
            "meta.County": county,
            "meta.first_seen": {"$gte": self.first, "$lte": self.last},
            "Age_Standardized": {"$exists": True},
            "Race_Ethnicity_Standardized": {"$exists": True},
            "Sex_Gender_Standardized": {"$exists": True},
            "Top_Charge": {"$exists": True},
            "Charges": {"$exists": True},
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
            "age": "$Age_Standardized",
            "race": "$Race_Ethnicity_Standardized",
            "gender": "$Sex_Gender_Standardized",
            "top_charge": "$Top_Charge",
            "num_charges": {
                "$size": {"$cond": [{"$isArray": "$Charges"}, "$Charges", []]}
            },
        }
        return list(self.dbs.bookings.find(match, query))


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    GetRosters(args).run()
