"""
utils.py
========

Shared infrastructure for every script in `data_prep/`. This module is
imported via `from utils import *`, so anything defined here becomes
available to `get_roster_list.py` and the five metric scripts
(`average_daily_population.py`, `average_daily_demographics.py`,
`length_of_stay_proportions.py`, `incapacitation_proportions.py`,
`rebooking_proportions.py`).

Contents, in order:

    Constants
        START_FL, START_GA : policy-change dates anchoring each state's
                             cycle grid.
        CYCLE              : cycle length in days (28).
        L1                 : ordered list of top-charge L1 categories
                             used by the `--by_top_charge` splits.

    MongoCollections       : thin wrapper around the three MongoDB
                             collections every script reads.

    get_cycles(self)       : build the cycle grid for a script instance
                             (pre- and post-policy 28-day blocks).

    get_parser()           : shared argparse parser for all scripts.
                             Every CLI flag the pipeline understands is
                             defined here in one place.

    resolve_window(args)   : enforce mutual exclusion between
                             --first/--last and --pre_cycles/--post_cycles
                             and write the resolved dates back onto args.

    load_snapshot(args)    : load the frozen booking DataFrame + manifest
                             produced by `get_roster_list.py --save --lock`.

    thread(worker, jobs)   : run `worker(job)` in parallel across `jobs`
                             with a progress bar.

    get_roster_sample(self): load the roster list CSV corresponding to
                             `args.sample` from the tmp dir.

    get_path_prefix(self)  : build the matrix output directory path for
                             the current `--state`, `--threshold`,
                             `--sample`, and `--by_top_charge` combo.

    find_date_range(self, date)
                           : return the cycle index that contains `date`,
                             or None.

    apply_exclusions(self, df)
                           : drop booking rows that hit any flag on
                             `self.flags` or have malformed `id_person`.

Design note — the "self" parameter on module-level helpers:
    Several helpers (`get_cycles`, `get_roster_sample`,
    `get_path_prefix`, `find_date_range`, `apply_exclusions`) take `self`
    as a positional argument even though they are not methods. Scripts
    call them as `get_cycles(self)` from inside their own methods to
    reuse the caller's `args` / `first` / `last` / `cycles` / `flags`
    attributes without re-plumbing them. It is unconventional but
    intentional; keep the pattern if you add more helpers.
"""

import argparse
import json
import os
import pandas as pd

from datetime import datetime as dt
from datetime import timedelta as td
from dotenv import load_dotenv
from multiprocessing.pool import ThreadPool
from pymongo import MongoClient
from tqdm import tqdm


# Policy-change anchor dates. Every cycle index in the study grid is
# measured relative to these — cycle 0 is the first 28-day block
# starting on the policy date, cycle -1 is the block immediately
# preceding it, and so on. Update these if the policy effective date
# ever changes.
START_FL = dt(2024, 1, 1, 0, 0)
START_GA = dt(2024, 7, 1, 0, 0)

# Cycle length in days. 28 (= 4 weeks) is used throughout so that every
# cycle contains the same number of each weekday, which keeps
# day-of-week composition constant across cycles and simplifies
# comparisons of daily-rate metrics.
CYCLE = 28


# Legacy study-window defaults, applied by `resolve_window(args)` only
# when the user supplied neither --first/--last nor
# --pre_cycles/--post_cycles. Hoisted here (rather than buried inside
# the function) so the defaults are visible alongside the other global
# constants. LEGACY_LAST is computed at import time; for a CLI
# invocation that runs once and exits, import time and
# resolve_window-call time are effectively identical.
LEGACY_FIRST = "2023-01-01"
LEGACY_LAST = dt.strftime(dt.now().replace(day=1) - td(days=1), "%Y-%m-%d")


# Top-charge L1 categories used by the `--by_top_charge` splits. The
# list is ordered so that downstream matrices and plots render the
# categories in a consistent sequence. Any value of `Top_Charge` in the
# underlying data that is not in this list is treated as unknown.
L1 = [
    "Violent",
    "Property",
    "Drug",
    "Public Order",
    "DUI Offense",
    "Criminal traffic",
]


class MongoCollections:
    """
    Thin wrapper that opens a MongoDB connection and exposes the three
    collections every data-prep script reads.

    Connection string comes from the `MONGO_READ_URI` environment
    variable (loaded from `.env` via `python-dotenv`). The credentials
    should point at a read-only user — none of the data-prep scripts
    ever write to the database.

    Attributes:
        bookings     — `jdi.jdi`: one document per booking. Source of
                       truth for admissions, releases, demographics,
                       charges, and per-booking flags. All metric
                       scripts hit this collection.
        scrape_dates — `jdi-stats.scrape-dates`: one document per
                       roster summarizing the span of observed scrapes
                       and a list of missing-scrape dates. Used by the
                       base-viability coverage filter in
                       `get_roster_list.GetRosters.get_viable_rosters`.
        pops         — `jdi-stats.pops-by-date-jail`: precomputed daily
                       in-custody population by roster. Used by
                       `average_daily_population.py`.
    """

    def __init__(self):
        load_dotenv()
        self.client = MongoClient(os.getenv("MONGO_READ_URI"))
        self.jdi = self.client.get_database("jdi")
        self.jdi_stats = self.client.get_database("jdi-stats")
        self.bookings = self.jdi.get_collection("jdi")
        self.scrape_dates = self.jdi_stats.get_collection("scrape-dates")
        self.pops = self.jdi_stats.get_collection("pops-by-date-jail")


def get_cycles(self):
    """
    Build the 28-day cycle grid for a script instance.

    The grid is anchored on the state's policy-change date
    (`START_FL` or `START_GA`): cycle 0 is the 28-day block starting
    on the policy date, cycle 1 is the next block, etc. Pre-policy
    cycles run backward and are indexed -1, -2, ... — cycle -1 is the
    28-day block ending the day before the policy date.

    The grid is truncated INWARD to `[self.first, self.last]`: any
    candidate cycle whose endpoint would fall outside that range is
    dropped entirely (no partial cycles). This guarantees that every
    cycle returned contains a full 28 days of observation, which is
    what the downstream metrics rely on for cross-cycle comparability.

    Args:
        self — the calling script instance. Must expose
               `self.args.state`, `self.first`, and `self.last`.

    Returns:
        list of (cycle_index, cycle_start, cycle_end) tuples, sorted
        by cycle_start ascending. Indices are negative for pre-policy
        cycles, zero or positive for post-policy cycles.

    Raises:
        ValueError if `self.args.state` is not a state with a known
        policy-change date.
    """
    if self.args.state == "fl":
        start = START_FL
    elif self.args.state == "ga":
        start = START_GA
    else:
        raise ValueError("unidentified state")

    # Post-policy cycles: walk forward from the policy date in 28-day
    # blocks, breaking as soon as a candidate block's end exceeds
    # `self.last` so that only complete cycles are kept.
    cycles_fwd = list()
    for i in range(0, round(len(list(pd.date_range(start, self.last))) / CYCLE)):
        cycle_start = start + td(days=i * CYCLE)
        cycle_end = cycle_start + td(days=CYCLE - 1)
        if cycle_end > self.last:
            break
        cycles_fwd.append((i, cycle_start, cycle_end))

    # Pre-policy cycles: walk backward from (policy_date - 1) in 28-day
    # blocks, breaking as soon as a candidate block's start drops below
    # `self.first`. Indices are negative so that cycle -1 is adjacent
    # to cycle 0 on the timeline.
    cycles_bck = list()
    for i in range(
        0, round(len(list(pd.date_range(self.first, start - td(days=1)))) / CYCLE)
    ):
        cycle_end = start - td(days=1) - td(days=i * CYCLE)
        cycle_start = cycle_end - td(days=CYCLE - 1)
        if cycle_start < self.first:
            break
        cycles_bck.append((-i - 1, cycle_start, cycle_end))

    cycles = cycles_bck + cycles_fwd
    cycles = sorted(cycles, key=lambda t: t[1])
    return cycles


def get_parser():
    """
    Build the shared argparse parser used by every data-prep script.

    Every CLI flag the pipeline understands is defined here — scripts
    import this parser verbatim and then call `parser.parse_args()` in
    their `__main__` block, so adding a flag here makes it available
    everywhere. The parser is intentionally permissive: scripts that
    don't need a given flag simply ignore it on `args`.

    Flag groups:
      - Geographic / sample restriction: -s/--state, -m/--threshold,
        -x/--exclude, -r/--sample, -btc/--by_top_charge.
      - Study window (mutually exclusive pairs enforced by
        `resolve_window(args)`): -f/--first + -l/--last (date mode),
        or --pre_cycles + --post_cycles (cycle mode).
      - Persistence (only meaningful in `get_roster_list.py`):
        --save, --lock.

    Returns:
        argparse.ArgumentParser, ready for `.parse_args()`.
    """
    parser = argparse.ArgumentParser()

    # date range and geographic sample restriction arguments
    parser.add_argument(
        "-s",
        "--state",
        type=str,
        default="fl",
        choices=["fl", "ga"],
        help="""
            State for which to produce data
            (defaults to 'fl').
            """,
    )
    parser.add_argument(
        "-m",
        "--threshold",
        type=float,
        default=0.75,
        help="""
            Threshold below which to exclude rosters defined by the proportion of 
            dates in specified range with missing scrape data.
            """,
    )
    parser.add_argument(
        "-x",
        "--exclude",
        type=str,
        default=list(),
        nargs="*",
        help="""
                Exclude any admissions from specified list of states
                (specify as, e.g., `AL AR ...`).
                """,
    )
    # Note on `default=None`: the argparse default is None so that
    # `resolve_window(args)` can distinguish "user explicitly typed
    # --first" from "user left it alone" and enforce mutual exclusion
    # with --pre_cycles/--post_cycles. When no window args are supplied
    # at all, `resolve_window` fills in LEGACY_FIRST / LEGACY_LAST
    # (defined as module constants above).
    parser.add_argument(
        "-f",
        "--first",
        type=str,
        default=None,
        help="""
                Earliest date from which to collect admissions.
                If neither --first/--last nor --pre_cycles/--post_cycles
                are supplied, resolve_window(args) fills this in with
                LEGACY_FIRST ("2023-01-01"). Mutually exclusive with
                --pre_cycles/--post_cycles.
                """,
    )
    parser.add_argument(
        "-l",
        "--last",
        type=str,
        default=None,
        help="""
                Latest date from which to collect admissions.
                If neither --first/--last nor --pre_cycles/--post_cycles
                are supplied, resolve_window(args) fills this in with
                LEGACY_LAST (the last day of the previous month at
                module-import time). Mutually exclusive with
                --pre_cycles/--post_cycles.
                """,
    )
    parser.add_argument(
        "--pre_cycles",
        type=int,
        default=None,
        help="""
                Number of 28-day cycles BEFORE the state policy start date
                to include in the study window. If specified, --first is
                computed automatically as (policy_start - pre_cycles * 28
                days). Mutually exclusive with --first/--last.
                """,
    )
    parser.add_argument(
        "--post_cycles",
        type=int,
        default=None,
        help="""
                Number of 28-day cycles AFTER the state policy start date
                to include in the study window. If specified, --last is
                computed automatically as (policy_start + post_cycles * 28
                days - 1). Mutually exclusive with --first/--last.
                """,
    )
    parser.add_argument(
        "--lock",
        action="store_true",
        help="""
            If specified (alongside --save in `get_roster_list.py`), also
            pull the underlying booking sample for the viable rosters in
            the resolved window and persist it as a frozen snapshot at
            `../tmp/threshold_{t}/snapshot_{sample}.pkl` plus a JSON
            manifest. Downstream scripts can then load the snapshot via
            `load_snapshot(args)` instead of re-querying MongoDB, so the
            analysis sample is reproducible even if the database changes.
            """,
    )

    # covariate completeness sample restriction arguments
    parser.add_argument(
        "-r",
        "--sample",
        type=str,
        default="all",
        choices=["all", "charges", "demographics"],
        help="""
                Specifies the roster sample for which to produce matrices
                (all rosters, those including charge data, 
                or those including demographic and charge data).
        """,
    )
    parser.add_argument(
        "-btc",
        "--by_top_charge",
        action="store_true",
        help="""
            If specified, break out results matrices by top L1 charge
            (note: this can only be done for the charges and demographic samples).
        """,
    )

    # specific flag for saving roster samples
    parser.add_argument(
        "--save",
        action="store_true",
        help="""
            If specified, save roster list to local file
            (used in `get_roster_list.py`).
            """,
    )
    return parser


def resolve_window(args):
    """
    Resolve the study window on `args` into (args.first, args.last) as
    "%Y-%m-%d" strings. Enforces mutual exclusion between --first/--last
    and --pre_cycles/--post_cycles, and clamps the result to
    [LEGACY_FIRST, LEGACY_LAST] regardless of mode.
    """
    cycles_set = (
        getattr(args, "pre_cycles", None) is not None
        or getattr(args, "post_cycles", None) is not None
    )
    dates_set = args.first is not None or args.last is not None
    if cycles_set and dates_set:
        raise ValueError(
            "--pre_cycles/--post_cycles and --first/--last are mutually exclusive"
        )

    if cycles_set:
        policy_start = {"fl": START_FL, "ga": START_GA}.get(args.state)
        if policy_start is None:
            raise ValueError(f"no policy start date for state '{args.state}'")
        first = policy_start - td(days=(args.pre_cycles or 0) * CYCLE)
        last = policy_start + td(days=(args.post_cycles or 0) * CYCLE - 1)
    else:
        first = dt.strptime(args.first or LEGACY_FIRST, "%Y-%m-%d")
        last = dt.strptime(args.last or LEGACY_LAST, "%Y-%m-%d")

    legacy_first = dt.strptime(LEGACY_FIRST, "%Y-%m-%d")
    legacy_last = dt.strptime(LEGACY_LAST, "%Y-%m-%d")
    if first < legacy_first:
        raise ValueError(
            f"resolved --first {dt.strftime(first, '%Y-%m-%d')} is before "
            f"LEGACY_FIRST ({LEGACY_FIRST})"
        )
    if last > legacy_last:
        raise ValueError(
            f"resolved --last {dt.strftime(last, '%Y-%m-%d')} is after "
            f"LEGACY_LAST ({LEGACY_LAST})"
        )

    args.first = dt.strftime(first, "%Y-%m-%d")
    args.last = dt.strftime(last, "%Y-%m-%d")


def load_snapshot(args):
    """
    Load a frozen booking snapshot produced by
    `get_roster_list.py --save --lock`.

    Returns a tuple (df, manifest) where `df` is the pandas DataFrame
    of bookings pulled for the viable rosters across the resolved
    window, and `manifest` is the dict describing how the snapshot
    was built (state, threshold, sample, window, cycle list, roster
    list, creation timestamp).

    Downstream scripts should prefer this over hitting MongoDB when a
    frozen sample is required for reproducibility.
    """
    base = (
        f"../tmp/threshold_{str(args.threshold).replace('.', '_')}/"
        f"snapshot_{args.sample}"
    )
    df = pd.read_pickle(f"{base}.pkl")
    with open(f"{base}.json") as f:
        manifest = json.load(f)
    return df, manifest


def thread(worker, jobs, threads=5):
    """
    Run `worker(job)` in parallel across `jobs` with a progress bar.

    Uses a `ThreadPool` rather than a process pool because every
    `worker` in this pipeline is I/O-bound on MongoDB round-trips —
    processes would add fork/serialize overhead without speeding
    anything up, and threads let all workers share the same pymongo
    client pool.

    Results are flattened: if `worker` returns a list (e.g. a batch
    of booking documents), each element is appended individually; if
    it returns a non-list, the value itself is appended. Falsy
    results are silently dropped.

    Args:
        worker  — callable taking a single `job` argument.
        jobs    — iterable of inputs to pass to `worker`, one at a time.
        threads — pool size. Default 5 is a conservative setting that
                  respects read-replica connection limits; raise it
                  only if you know your cluster can take it.

    Returns:
        Flattened list of non-empty results, or None if every worker
        returned falsy (this quirk is load-bearing — the caller in
        `get_viable_rosters_demographics` checks for None).
    """
    pool = ThreadPool(threads)
    results = list()
    for result in tqdm(pool.imap_unordered(worker, jobs), total=len(jobs)):
        if result and isinstance(result, list):
            results.extend([r for r in result if r])
        elif result:
            results.append(result)
    pool.close()
    pool.join()
    if results:
        return results


def get_roster_sample(self):
    """
    Load the roster list CSV that corresponds to the caller's
    `--sample` mode from `../tmp/threshold_{t}/`.

    This is the downstream counterpart to the CSV written by
    `get_roster_list.py --save`. Every metric script calls this at the
    start of its run to populate `self.rosters`, which defines the
    set of jurisdictions the script will iterate over.

    The three sample modes map to three distinct files:
        - "all"          → rosters.csv
        - "charges"      → rosters_charges.csv
        - "demographics" → rosters_demographics.csv

    Returns:
        sorted list[str] of roster IDs. If `get_roster_list.py` has
        not yet been run for this threshold + sample combination, the
        `pd.read_csv` call will raise FileNotFoundError — that is the
        intended failure mode, since running a metric script without
        a roster list is always a user error.
    """
    if self.args.sample == "demographics":
        return sorted(
            list(
                pd.read_csv(
                    f"../tmp/threshold_{str(self.args.threshold).replace('.', '_')}/rosters_demographics.csv"
                )["rosters"].unique()
            )
        )
    elif self.args.sample == "charges":
        return sorted(
            list(
                pd.read_csv(
                    f"../tmp/threshold_{str(self.args.threshold).replace('.', '_')}/rosters_charges.csv"
                )["rosters"].unique()
            )
        )
    elif self.args.sample == "all":
        return sorted(
            list(
                pd.read_csv(
                    f"../tmp/threshold_{str(self.args.threshold).replace('.', '_')}/rosters.csv"
                )["rosters"].unique()
            )
        )


def get_path_prefix(self):
    """
    Build the matrix output directory path for the caller's current
    argument combination. Every metric script uses this to decide
    where its CSVs get written so that the on-disk layout is
    consistent across scripts.

    Resulting path shape:
        ../matrices/{state}/threshold_{t}/{sample}/{charge_split}/

    where:
        - state          = "fl" | "ga"
        - t              = threshold with `.` replaced by `_`
                           (e.g. "0_75")
        - sample         = "all" | "charges" | "demographics"
        - charge_split   = "all_charges" | "by_top_charge"

    `--by_top_charge` is only valid on the "charges" or "demographics"
    samples (the "all" sample has no top-charge split because it
    includes rosters without charge data). Calling this helper with
    `--by_top_charge --sample all` raises ValueError.

    Returns:
        str path ending in "/" so callers can append a metric
        subdirectory directly.
    """
    path = f"../matrices/{self.args.state}/threshold_{str(self.args.threshold).replace('.', '_')}/"
    if self.args.sample == "demographics":
        path += "demographics/"
    elif self.args.sample == "charges":
        path += "charges/"
    elif self.args.sample == "all":
        path += "all/"
    if self.args.sample in ["demographics", "charges"]:
        if self.args.by_top_charge:
            path += "by_top_charge/"
        else:
            path += "all_charges/"
    elif self.args.by_top_charge:
        raise ValueError(
            "if using [-btc, --by_top_charge] argument, "
            "please select a roster sample [-r, --sample] from {charges, demographics}"
        )
    else:
        path += "all_charges/"
    return path


def find_date_range(self, date):
    """
    Return the cycle index whose 28-day window contains `date`, or
    None if `date` falls outside every cycle in `self.cycles`.

    Metric scripts use this to bucket each booking into the cycle it
    belongs to for aggregation. A None return means the booking
    should be dropped from cycle-level aggregates — typically because
    it sits in a partial edge cycle that `get_cycles` trimmed away,
    or because it was pulled with a window wider than the cycle grid.

    Args:
        self — caller instance with `self.cycles` populated.
        date — a datetime-like comparable against cycle endpoints.

    Returns:
        int cycle index or None.
    """
    for i, start_date, end_date in self.cycles:
        if start_date <= date <= end_date:
            return i
    return None


def apply_exclusions(self, df):
    """
    Drop booking rows that should not participate in any analysis.

    Two categories of exclusion are applied:

      1. Flag-based: rows whose `flags` field contains any substring
         listed on `self.flags` are dropped. Each script declares its
         own flag list in `__init__` based on which quality issues
         matter for its metric (e.g. every script excludes
         "non_distinct_jdi_inmate_id"; LOS-sensitive scripts also
         exclude "left_intersects_gap"). The `flags` column is cast
         to str first so the substring check works on list-valued
         entries.

      2. Malformed person ID: rows with a NaN `id_person` or with
         `id_person` stored as a list (an ingestion bug where the
         same booking was associated with multiple inmate IDs) are
         dropped entirely, since they cannot be deduplicated or
         joined across bookings reliably.

    The `flags` column is deleted after filtering so that downstream
    aggregations don't accidentally groupby on it.

    Args:
        self — caller instance with `self.flags` populated.
        df   — DataFrame with at least `flags` and `id_person` columns.

    Returns:
        DataFrame with excluded rows removed.
    """
    df["flags"] = df["flags"].astype(str)
    for flag in self.flags:
        df = df[~df["flags"].str.contains(flag)]
    del df["flags"]
    df = df[df["id_person"].notna()]
    return df[~df["id_person"].apply(lambda o: isinstance(o, list))]
