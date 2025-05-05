import numpy as np

from utils import *


class ADD:
    def __init__(self, arguments):
        self.args = arguments
        self.first = dt.strptime(self.args.first, "%Y-%m-%d")
        self.last = dt.strptime(self.args.last, "%Y-%m-%d")
        self.dbs = MongoCollections()
        self.cycles = get_cycles(self)
        self.first = min([t[1] for t in self.cycles])
        self.last = max([t[2] for t in self.cycles])
        self.flags = ["non_distinct_jdi_inmate_id"]
        self.path = f"../matrices/{self.args.state}/threshold_{str(self.args.threshold).replace('.', '_')}/cov/add/"
        for i, piece in enumerate(self.path.split("/")[1:-1]):
            if not os.path.exists(
                "../" + "/".join(self.path.split("/")[1:-1][: i + 1])
            ):
                os.mkdir("../" + "/".join(self.path.split("/")[1:-1][: i + 1]))

    def run(self):
        rosters = sorted(
            list(
                pd.read_csv(
                    f"../tmp/threshold_{str(self.args.threshold).replace('.', '_')}/rosters_demographics.csv"
                )["rosters"].unique()
            )
        )

        df = pd.DataFrame(thread(self.get_demographics, rosters))
        df = self.apply_exclusions(df)

        df["cycle"] = df["first_seen"].apply(lambda date: self.find_date_range(date))

        df = self.reduce_demographics(df)

        for field in ["race", "gender", "top_charge"]:
            self.prep_categorical_matrix(df, field)

        for field in ["age", "num_charges"]:
            self.prep_numerical_matrix(df, field)

    def prep_categorical_matrix(self, df, field):
        df_all = (
            df.groupby(["state", "cycle"])
            .size()
            .reset_index()
            .rename(columns={0: "admissions"})
        )

        df_known_value = (
            df[~df[field].str.contains("Unknown")]
            .groupby(["state", "cycle"])
            .size()
            .reset_index()
            .rename(columns={0: f"admissions_known_{field}"})
        )

        mx = pd.merge(df_all, df_known_value, on=["state", "cycle"])
        mx[f"proportion_known_{field}"] = (
            mx[f"admissions_known_{field}"] / mx["admissions"]
        )

        for value in sorted(list(df[field].unique())):
            if "Unknown" not in value:
                df_field = (
                    df[df[field] == value]
                    .groupby(["state", "cycle"])
                    .size()
                    .reset_index()
                    .rename(
                        columns={
                            0: f"admissions_known_{value.lower().replace(' ', '_')}"
                        }
                    )
                )

                mx = pd.merge(mx, df_field, on=["state", "cycle"])
                mx[f"proportion_{value.lower().replace(' ', '_')}"] = (
                    mx[f"admissions_known_{value.lower().replace(' ', '_')}"]
                    / mx[f"admissions_known_{field}"]
                )

        for col in mx.columns:
            if "proportion_" in col:
                mx_out = mx.pivot(
                    columns=["cycle"], index=["state"], values=col
                ).T.reset_index()

                if not os.path.exists(f"{self.path}{field}/"):
                    os.mkdir(f"{self.path}{field}/")

                mx_out.to_csv(
                    f"{self.path}{field}/{col}.csv",
                    index=False,
                )

    def prep_numerical_matrix(self, df, field):
        mx_mean = (
            df.groupby(["state", "cycle"])[field]
            .mean()
            .reset_index()
            .rename(columns={field: f"{field}_mean"})
        )
        mx_median = (
            df.groupby(["state", "cycle"])[field]
            .median()
            .reset_index()
            .rename(columns={field: f"{field}_median"})
        )
        mx = pd.merge(mx_mean, mx_median, on=["state", "cycle"])

        for col in [f"{field}_mean", f"{field}_median"]:
            mx_out = mx.pivot(
                columns=["cycle"], index=["state"], values=col
            ).T.reset_index()

            if not os.path.exists(f"{self.path}{field}/"):
                os.mkdir(f"{self.path}{field}/")

            mx_out.to_csv(
                f"{self.path}{field}/{col}.csv",
                index=False,
            )

    @staticmethod
    def reduce_demographics(df):
        df["race"] = np.where(
            df["race"].isin(["Other POC", "Indigenous", "AAPI"]), "Other", df["race"]
        )
        df["gender"] = np.where(
            df["gender"].isin(["Trans", "Nonbinary"]), "Unknown Gender", df["gender"]
        )
        df["top_charge"] = np.where(
            df["top_charge"].isin(
                [
                    "Violent",
                    "Property",
                    "Drug",
                    "Public Order",
                    "DUI Offense",
                    "Criminal traffic",
                ]
            ),
            df["top_charge"],
            "Unknown Top Charge",
        )

        return df

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

    ADD(args).run()
