"""
get_roster_list.py
==================

Builds the list of jail rosters (state-county pairs) that pass data-quality
inclusion criteria for the study window, and writes it to
`../tmp/threshold_{t}/rosters[_demographics|_charges].csv`. Optionally also
freezes the underlying booking sample to a pickle + JSON manifest so that
downstream scripts can run against a stable dataset.

Downstream scripts (`average_daily_population.py`, `length_of_stay_proportions.py`,
etc.) read the roster file to decide which jurisdictions to analyze, so the
roster list defines the denominator sample for every matrix produced in the
paper. When `--lock` is used, they can additionally call
`utils.load_snapshot(args)` to read the frozen booking DataFrame and manifest
instead of re-querying MongoDB.

CLI arguments (shared parser in `utils.get_parser`):

    -s, --state {fl,ga}       State whose policy start anchors the cycle grid.
    -m, --threshold FLOAT     Min. fraction of non-missing scrape days in the
                              window for a roster to be kept (default 0.75).
    -x, --exclude [STATE ...] States to drop entirely (e.g. when building a
                              pure control pool).
    -r, --sample {all,charges,demographics}
                              Sample mode — see below. Determines which
                              secondary filter (if any) is applied on top of
                              the scrape-coverage base filter, and which CSV
                              / snapshot file is written.

    Window (mutually exclusive — specify EITHER dates OR cycles):

        -f, --first YYYY-MM-DD   Earliest admission date.
        -l, --last  YYYY-MM-DD   Latest admission date.
            (Legacy defaults "2023-01-01" through last day of previous month
             apply only when neither dates nor cycles are specified.)

        --pre_cycles  N          N 28-day cycles BEFORE the state policy date
                                 (computes --first automatically).
        --post_cycles N          N 28-day cycles AFTER the state policy date
                                 (computes --last automatically, inclusive).

    Persistence:

        --save   Write the roster list to CSV.
        --lock   In addition to --save, pull the underlying booking sample
                 for the viable rosters across the resolved window and
                 persist it as:
                     ../tmp/threshold_{t}/snapshot_{sample}.pkl   (DataFrame)
                     ../tmp/threshold_{t}/snapshot_{sample}.json  (manifest)
                 The manifest records state, threshold, sample, exclusions,
                 resolved window, cycle grid, roster list, booking count, and
                 creation timestamp — enough to fully describe the frozen
                 sample for reproducibility.

Three sample modes, selected by `--sample`:
    - `all`          (default): base scrape-coverage filter only.
    - `demographics`: base filter PLUS requirement that the roster has at
                      least one booking with non-missing values for EVERY
                      demographic field used in the paper
                      (race, gender, age, top_charge, num_charges > 0).
    - `charges`     : base filter PLUS requirement that the roster has at
                      least one booking with a known `Top_Charge`.

Inclusion criteria applied by `get_viable_rosters` (the base filter):
    1. Roster is not in the user-supplied `--exclude` state list.
    2. Roster's scraping history spans the full study window
       (`first_scrape <= study_first` and `last_scrape >= study_last`).
    3. Fraction of non-missing scrape days within the study window is at
       least `--threshold` (e.g. 0.75 keeps rosters with >=75% coverage).

Study window resolution:
    `resolve_window(args)` runs first in `__init__`. It enforces mutual
    exclusion between the date-mode and cycle-mode argument pairs and
    writes the resolved "%Y-%m-%d" strings back onto `args.first` /
    `args.last`. The window is then further snapped to whole 28-day cycles
    by `get_cycles`, which is the grid every downstream metric uses.

Example invocations:

    # Date mode (explicit window), sample = all, write CSV only:
    python get_roster_list.py -s fl -f 2023-01-01 -l 2025-06-30 --save

    # Cycle mode: 12 pre / 24 post, sample = charges, freeze snapshot:
    python get_roster_list.py -s fl -r charges \\
        --pre_cycles 12 --post_cycles 24 --save --lock

Output:
    A single-column CSV of roster IDs in the form `"{STATE}-{County}"`
    (e.g. `"FL-Broward"`), sorted alphabetically. Optionally also a
    booking snapshot `.pkl` and manifest `.json` under the same
    `tmp/threshold_{t}/` directory.
"""

from pathlib import Path

from utils import *


class GetRosters:
    def __init__(self, arguments):
        self.args = arguments
        # Resolve the window first so either --first/--last OR
        # --pre_cycles/--post_cycles produces a usable date pair
        # (and their mutual exclusion is enforced).
        resolve_window(self.args)
        self.first = dt.strptime(self.args.first, "%Y-%m-%d")
        self.last = dt.strptime(self.args.last, "%Y-%m-%d")
        self.dbs = MongoCollections()

        # Snap the user-supplied window to whole 28-day cycles so that
        # `self.first` / `self.last` match the grid every downstream
        # script uses. Without this, a roster could pass scrape-coverage
        # checks on the raw window but fail them on the cycle-aligned
        # window actually analyzed later.
        self.cycles = get_cycles(self)
        self.first = min([t[1] for t in self.cycles])
        self.last = max([t[2] for t in self.cycles])
        n_pre = sum(1 for i, _, _ in self.cycles if i < 0)
        n_post = sum(1 for i, _, _ in self.cycles if i >= 0)
        print(
            f"[get_roster_list] resolved: {self.first:%Y-%m-%d} → {self.last:%Y-%m-%d} "
            f"({len(self.cycles)} cycles: {n_pre} pre, {n_post} post)\n"
            f"[get_roster_list] args:     {self.args.first} → {self.args.last}\n"
            f"[get_roster_list] caps:     {LEGACY_FIRST} → {LEGACY_LAST}"
        )

        # Output lives under `../tmp/threshold_{t}/` so that multiple
        # coverage thresholds (0.75, 0.90, etc.) can coexist without
        # overwriting one another — the threshold value is baked into
        # the directory name. `pathlib.mkdir(parents=True, exist_ok=True)`
        # is robust to the script being invoked from a non-default CWD.
        self.path = f"../tmp/threshold_{str(self.args.threshold).replace('.', '_')}/"
        Path(self.path).mkdir(parents=True, exist_ok=True)

        # --lock is only meaningful alongside --save (it piggybacks on
        # the same write step). Flag the misuse loudly rather than
        # silently doing nothing.
        if self.args.lock and not self.args.save:
            raise ValueError(
                "--lock requires --save; the booking snapshot is written "
                "alongside the roster CSV."
            )

    def run(self):
        """
        Dispatch to the correct viable-roster routine based on `--sample`
        and optionally write the result to a CSV. The three sample modes
        produce three separate files so that downstream scripts can load
        exactly the denominator set they need.
        """
        if self.args.sample == "demographics":
            rosters = self.get_viable_rosters_demographics()
        elif self.args.sample == "charges":
            rosters = self.get_viable_rosters_charges()
        else:
            rosters = self.get_viable_rosters()

        if args.save:
            df = pd.DataFrame(rosters, columns=["rosters"])
            if self.args.sample == "demographics":
                df.to_csv(self.path + "rosters_demographics.csv", index=False)
            elif self.args.sample == "charges":
                df.to_csv(self.path + "rosters_charges.csv", index=False)
            else:
                df.to_csv(self.path + "rosters.csv", index=False)

            # Optionally freeze the underlying booking sample so that
            # downstream scripts can run against a stable dataset via
            # `load_snapshot(args)` instead of re-querying MongoDB.
            if self.args.lock:
                self.lock_snapshot(rosters)

    def get_viable_rosters(self):
        """
        Base inclusion filter — returns the sorted list of roster IDs
        (`"{STATE}-{County}"`) that meet the scrape-coverage criteria
        for the cycle-aligned study window.

        Source collection:
            `scrape_dates` — one document per roster summarizing the
            span of observed scrapes and any missing-scrape dates.

        Criteria applied (in order):
            1. Drop any roster whose state appears in `--exclude`.
            2. Keep only rosters whose scrape history fully brackets the
               study window (`first_scrape <= study_first` AND
               `last_scrape >= study_last`). This prevents partial-span
               rosters from contributing biased cycle counts.
            3. Keep only rosters where the fraction of non-missing
               scrape days inside the study window is >= `--threshold`.
               Missing dates outside the window are ignored.

        Returns:
            sorted list[str] of roster IDs.
        """
        rosters = list(self.dbs.scrape_dates.find({}))

        # Criterion 1: drop excluded states (e.g. the treated states when
        # building a pure control pool).
        for state in self.args.exclude:
            rosters = [r for r in rosters if not r["_id"].startswith(f"{state}-")]

        # Criterion 2: roster's scrape history must fully span the window.
        rosters = [
            r
            for r in rosters
            if r["first_scrape"] <= self.first and r["last_scrape"] >= self.last
        ]

        # Criterion 3: within-window scrape coverage must clear the
        # `--threshold` floor. `missing_scrapes` is normalized to
        # midnight and clipped to the study window before computing
        # the ratio: normalization guards against time-of-day
        # components in stored timestamps failing to match the
        # midnight-aligned `pd.date_range`, which would otherwise
        # silently inflate the coverage ratio; clipping ensures that
        # gaps outside the window are not held against the roster.
        window = pd.date_range(self.first, self.last)
        window_len = len(window)
        kept = list()
        for r in rosters:
            missing_in_window = {
                pd.Timestamp(d).normalize()
                for d in r["missing_scrapes"]
                if self.first <= d <= self.last
            }
            coverage = len(window.difference(list(missing_in_window))) / window_len
            if coverage >= self.args.threshold:
                kept.append(r)

        return sorted(list(r["_id"] for r in kept))

    def get_viable_rosters_demographics(self):
        """
        Demographics-sample filter — returns the subset of base-viable
        rosters that have at least one booking with usable values for
        EVERY demographic field used in the paper.

        A roster is kept only if, after dropping rows with
        missing/unknown values in any of the following fields, at least
        one booking remains:
            - race (not "Unknown Race")
            - gender (not "Unknown Gender")
            - top_charge (not NaN and not "TBD")
            - age (not NaN and not "Unknown Age")
            - num_charges > 0

        Why all-fields-at-once: the demographic matrices are built from
        the same underlying booking rows, so a roster that has race but
        no age would produce inconsistent denominators across
        demographic splits. Requiring joint coverage guarantees every
        demographic proportion is computed over the same sample.

        Returns:
            sorted list[str] of roster IDs passing the joint filter.
        """
        rosters = self.get_viable_rosters()
        bookings = thread(self.get_demographics, rosters)
        df = pd.DataFrame(bookings)
        df["roster"] = df["state"] + "-" + df["county"]
        df = df[df["race"] != "Unknown Race"]
        df = df[df["gender"] != "Unknown Gender"]
        df = df[df["top_charge"].notna()]
        df = df[df["top_charge"] != "TBD"]
        df = df[df["age"].notna()]
        df = df[df["age"] != "Unknown Age"]
        df = df[df["num_charges"] > 0]
        return sorted(list(df["roster"].unique()))

    def get_demographics(self, roster):
        """
        Fetch every booking in `roster` whose `first_seen` falls in the
        study window AND that has all demographic fields present (the
        Mongo query requires `$exists: True` on each). Called in
        parallel by `get_viable_rosters_demographics` via `thread(...)`.

        Note the `$exists` check only enforces that the field is set —
        the Python-side filter in the caller is what rejects explicit
        "Unknown" sentinels. Both layers are needed.

        The projected fields mirror what downstream demographic scripts
        consume, so the same query shape can be reused if this routine
        is ever refactored into a shared helper.
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

    def get_viable_rosters_charges(self):
        """
        Charges-sample filter — returns the subset of base-viable
        rosters that have at least one booking with a usable `Top_Charge`
        (i.e. not NaN and not the "TBD" sentinel).

        This is the roster set consumed by analyses that split by top
        charge (e.g. `--by_top_charge` in the proportion scripts).
        Rosters with no known top charges would contribute zero to
        every charge-split numerator while still inflating the
        denominator, so they are excluded here rather than downstream.

        Returns:
            sorted list[str] of roster IDs passing the charge filter.
        """
        rosters = self.get_viable_rosters()
        bookings = thread(self.get_charges, rosters)
        df = pd.DataFrame(bookings)
        df["roster"] = df["state"] + "-" + df["county"]
        df = df[df["top_charge"].notna()]
        df = df[df["top_charge"] != "TBD"]
        return sorted(list(df["roster"].unique()))

    def get_charges(self, roster):
        """
        Fetch every booking in `roster` whose `first_seen` falls in the
        study window AND that has a `Top_Charge` set. Called in parallel
        by `get_viable_rosters_charges` via `thread(...)`.

        The Mongo `$exists` check filters out documents missing the
        field entirely; the caller additionally drops the "TBD"
        sentinel value, which represents a placeholder rather than a
        real charge classification.
        """
        state, county = roster.split("-", 1)
        match = {
            "meta.State": state,
            "meta.County": county,
            "meta.first_seen": {"$gte": self.first, "$lte": self.last},
            "Top_Charge": {"$exists": True},
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
            "top_charge": "$Top_Charge",
        }
        return list(self.dbs.bookings.find(match, query))

    def lock_snapshot(self, rosters):
        """
        Pull every booking in the resolved study window for `rosters`
        using a broad projection that covers every downstream script's
        needs, and persist it as:

            ../tmp/threshold_{t}/snapshot_{sample}.pkl   — booking DataFrame
            ../tmp/threshold_{t}/snapshot_{sample}.json  — manifest

        The manifest records exactly how the snapshot was built (state,
        threshold, sample, exclusions, resolved window, cycle grid,
        roster list, booking count, creation timestamp) so that
        downstream scripts — and future reviewers — can verify the
        sample used to produce each matrix.

        Note on `last_seen`: the projection pulls `last_seen` verbatim,
        so bookings admitted inside the window but released after it
        will have `last_seen > self.last`. This is intentional — LOS,
        incapacitation, and daily-population analyses all need the
        true release date — but downstream code should NOT assume
        `last_seen ∈ [self.first, self.last]`.

        Why lock: without this, every downstream script re-queries
        MongoDB on its own schedule, so a late scrape or re-processing
        of a booking can quietly shift the sample under a matrix that
        was already computed. A frozen pickle + manifest makes the
        analysis reproducible end-to-end.
        """
        bookings = thread(self._get_snapshot_bookings, rosters)
        df = pd.DataFrame(bookings or [])

        base = self.path + f"snapshot_{self.args.sample}"
        df.to_pickle(f"{base}.pkl")

        manifest = {
            "state": self.args.state,
            "sample": self.args.sample,
            "threshold": self.args.threshold,
            "exclude": list(self.args.exclude),
            "first": dt.strftime(self.first, "%Y-%m-%d"),
            "last": dt.strftime(self.last, "%Y-%m-%d"),
            "pre_cycles": self.args.pre_cycles,
            "post_cycles": self.args.post_cycles,
            "cycles": [
                {
                    "i": i,
                    "start": dt.strftime(s, "%Y-%m-%d"),
                    "end": dt.strftime(e, "%Y-%m-%d"),
                }
                for (i, s, e) in self.cycles
            ],
            "rosters": rosters,
            "num_bookings": int(len(df)),
            "created_at": dt.now().isoformat(),
        }
        with open(f"{base}.json", "w") as f:
            json.dump(manifest, f, indent=2, default=str)

    def _get_snapshot_bookings(self, roster):
        """
        Worker for `lock_snapshot` — pulls all in-window bookings for
        a single roster with the union of fields that every downstream
        script needs (admission / release timestamps, person ID,
        flags, standardized demographics, top charge, full charge
        array). Called in parallel via `thread(...)`.
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
            "age": "$Age_Standardized",
            "race": "$Race_Ethnicity_Standardized",
            "gender": "$Sex_Gender_Standardized",
            "top_charge": "$Top_Charge",
            "charges": "$Charges",
        }
        return list(self.dbs.bookings.find(match, query))


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    GetRosters(args).run()
