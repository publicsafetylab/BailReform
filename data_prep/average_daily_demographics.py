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
        self.path = get_path_prefix(self)
        if not self.args.by_top_charge:
            self.path += "add/"
        for i, piece in enumerate(self.path.split("/")[1:-1]):
            if not os.path.exists(
                "../" + "/".join(self.path.split("/")[1:-1][: i + 1])
            ):
                os.mkdir("../" + "/".join(self.path.split("/")[1:-1][: i + 1]))

    def run(self):
        rosters = get_roster_sample(self)
        df = pd.DataFrame(thread(self.get_demographics, rosters))
        df = apply_exclusions(self, df)
        df["cycle"] = df["first_seen"].apply(lambda date: find_date_range(self, date))
        df = self.reduce_demographics(df)

        if not self.args.by_top_charge:
            for field in ["race", "gender", "top_charge"]:
                self.prep_categorical_matrix(df, field)
            for field in ["age", "num_charges"]:
                self.prep_numerical_matrix(df, field)

        else:
            for charge in L1:
                tmp = df[df["top_charge"] == charge]
                for field in ["race", "gender", "top_charge"]:
                    self.prep_categorical_matrix(tmp, field, charge)
                for field in ["age", "num_charges"]:
                    self.prep_numerical_matrix(tmp, field, charge)

    def prep_categorical_matrix(self, df, field, top_charge=None):
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

                if not top_charge:
                    if not os.path.exists(f"{self.path}{field}/"):
                        os.mkdir(f"{self.path}{field}/")
                    mx_out.to_csv(
                        f"{self.path}{field}/{col}.csv",
                        index=False,
                    )
                else:
                    if not os.path.exists(
                        f"{self.path}{top_charge.lower().replace(' ', '_')}/add/"
                    ):
                        os.mkdir(
                            f"{self.path}{top_charge.lower().replace(' ', '_')}/add/"
                        )
                    if not os.path.exists(
                        f"{self.path}{top_charge.lower().replace(' ', '_')}/add/{field}/"
                    ):
                        os.mkdir(
                            f"{self.path}{top_charge.lower().replace(' ', '_')}/add/{field}/"
                        )
                    mx_out.to_csv(
                        f"{self.path}/{top_charge.lower().replace(' ', '_')}/add/{field}/{col}.csv",
                        index=False,
                    )

    def prep_numerical_matrix(self, df, field, top_charge=None):
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

            if not top_charge:
                if not os.path.exists(f"{self.path}{field}/"):
                    os.mkdir(f"{self.path}{field}/")
                mx_out.to_csv(
                    f"{self.path}{field}/{col}.csv",
                    index=False,
                )
            else:
                if not os.path.exists(
                    f"{self.path}{top_charge.lower().replace(' ', '_')}/add/"
                ):
                    os.mkdir(f"{self.path}{top_charge.lower().replace(' ', '_')}/add/")
                if not os.path.exists(
                    f"{self.path}{top_charge.lower().replace(' ', '_')}/add/{field}/"
                ):
                    os.mkdir(
                        f"{self.path}{top_charge.lower().replace(' ', '_')}/add/{field}/"
                    )
                mx_out.to_csv(
                    f"{self.path}/{top_charge.lower().replace(' ', '_')}/add/{field}/{col}.csv",
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
            df["top_charge"].isin(L1),
            df["top_charge"],
            "Unknown Top Charge",
        )
        return df

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
