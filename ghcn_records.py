#!/usr/bin/env python3
"""
Record-temperature pipeline, stages 1-3 (plus a checkpoint summary).

    select     Choose long-record US stations from NOAA's station + inventory files
    download   Fetch each selected station's full daily history (per-station CSV)
    detect     Walk each station's history and emit record-high / record-low events
    summarize  Decade and year ratios of record highs to record lows (checkpoint)

Typical run:
    python ghcn_records.py select
    python ghcn_records.py download --workers 8
    python ghcn_records.py detect --workers 8
    python ghcn_records.py summarize

Data source: NOAA GHCN-Daily (https://www.ncei.noaa.gov/products/land-based-station/
global-historical-climatology-network-daily). NOAA requests attribution, and modified
data must not be presented as original, unaltered NOAA data.

Requirements: Python 3.10+, pandas, requests. pyarrow is strongly recommended
(outputs are Parquet when it's installed, gzip CSV otherwise).
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd

log = logging.getLogger("ghcn_records")

# ----------------------------------------------------------------------------
# Defaults (all overridable on the command line)
# ----------------------------------------------------------------------------
NOAA_DOC_BASE = "https://www.ncei.noaa.gov/pub/data/ghcn/daily"
# Per-station files: one row per element-day, no header:
#   ID, YYYYMMDD, ELEMENT, VALUE, M-FLAG, Q-FLAG, S-FLAG, OBS-TIME
STATION_URL = NOAA_DOC_BASE + "/by_station/{id}.csv.gz"

COUNT_START = 1950        # first year whose records are counted
COUNT_END = 2025          # last *complete* year counted
BURN_IN_YEARS = 30        # years of history before a station's records count
YEAR_COMPLETENESS = 0.90  # min fraction of days present for a station-year to count
STATION_COMPLETENESS = 0.90  # min fraction of counted years that must be complete

NON_CONUS_STATES = {"AK", "HI", "PR", "VI", "GU", "AS", "MP", "UM", "FM", "MH", "PW"}

EVENT_COLUMNS = ["station", "date", "year", "type", "is_tie", "value_c10", "prior_c10"]
# type values:
#   high_max  TMAX above every earlier TMAX on that calendar day  (record high)
#   low_min   TMIN below every earlier TMIN on that calendar day  (record low)
#   high_min  TMIN above every earlier TMIN  (record warm night)
#   low_max   TMAX below every earlier TMAX  (record cold day)
# The headline ratio is high_max : low_min.


# ----------------------------------------------------------------------------
# Small I/O helpers
# ----------------------------------------------------------------------------
def _has_pyarrow() -> bool:
    try:
        import pyarrow  # noqa: F401
        return True
    except ImportError:
        return False


def write_table(df: pd.DataFrame, path_no_ext: Path) -> Path:
    """Write Parquet if pyarrow is available, else gzip CSV. Returns the path written."""
    path_no_ext.parent.mkdir(parents=True, exist_ok=True)
    if _has_pyarrow():
        p = path_no_ext.with_suffix(".parquet")
        df.to_parquet(p, index=False)
    else:
        p = path_no_ext.with_suffix(".csv.gz")
        df.to_csv(p, index=False)
    return p


def read_table(path: Path) -> pd.DataFrame:
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def find_table(path_no_ext: Path) -> Path:
    for ext in (".parquet", ".csv.gz"):
        p = path_no_ext.with_suffix(ext)
        if p.exists():
            return p
    raise FileNotFoundError(f"No {path_no_ext}.parquet or .csv.gz — run the earlier stage first.")


def http_get(url: str, dest: Path, retries: int = 4, timeout: int = 120) -> None:
    """Download url to dest atomically, with exponential backoff."""
    import requests

    tmp = dest.with_name(dest.name + ".part")
    for attempt in range(1, retries + 1):
        try:
            with requests.get(url, stream=True, timeout=timeout) as r:
                r.raise_for_status()
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
            tmp.replace(dest)
            return
        except Exception as e:  # noqa: BLE001 — we re-raise after the last attempt
            if tmp.exists():
                tmp.unlink()
            if attempt == retries:
                raise
            wait = 2 ** attempt
            log.warning("GET %s failed (%s); retrying in %ss", url, e, wait)
            time.sleep(wait)


# ----------------------------------------------------------------------------
# Stage 1a: parse NOAA metadata files
# ----------------------------------------------------------------------------
def parse_stations(path: Path) -> pd.DataFrame:
    """ghcnd-stations.txt (fixed width). See NOAA readme section IV."""
    colspecs = [(0, 11), (12, 20), (21, 30), (31, 37), (38, 40), (41, 71)]
    names = ["station", "lat", "lon", "elev_m", "state", "name"]
    df = pd.read_fwf(path, colspecs=colspecs, names=names, header=None,
                     dtype={"station": str, "state": str, "name": str})
    df["state"] = df["state"].fillna("").str.strip()
    df["name"] = df["name"].fillna("").str.strip()
    return df


def parse_inventory(path: Path) -> pd.DataFrame:
    """ghcnd-inventory.txt (fixed width): ID, LAT, LON, ELEMENT, FIRSTYEAR, LASTYEAR."""
    colspecs = [(0, 11), (31, 35), (36, 40), (41, 45)]
    names = ["station", "element", "first_year", "last_year"]
    return pd.read_fwf(path, colspecs=colspecs, names=names, header=None,
                       dtype={"station": str, "element": str})


def select_stations(stations: pd.DataFrame, inventory: pd.DataFrame, *,
                    first_year_by: int, last_year_at_least: int,
                    conus_only: bool) -> pd.DataFrame:
    """US stations with both TMAX and TMIN spanning [first_year_by, last_year_at_least]."""
    inv = inventory[inventory["element"].isin(["TMAX", "TMIN"])]
    inv = inv[(inv["first_year"] <= first_year_by) & (inv["last_year"] >= last_year_at_least)]
    both = inv.groupby("station")["element"].nunique()
    ids = both[both == 2].index

    sel = stations[stations["station"].isin(ids) & stations["station"].str.startswith("US")]
    if conus_only:
        sel = sel[~sel["state"].isin(NON_CONUS_STATES) & (sel["state"] != "")]

    span = (inv[inv["station"].isin(sel["station"])]
            .groupby("station").agg(first_year=("first_year", "max"),
                                    last_year=("last_year", "min")))
    return sel.merge(span, on="station").sort_values("station").reset_index(drop=True)


def cmd_select(args) -> None:
    meta = Path(args.data_dir) / "meta"
    meta.mkdir(parents=True, exist_ok=True)
    for fname in ("ghcnd-stations.txt", "ghcnd-inventory.txt"):
        dest = meta / fname
        if dest.exists() and not args.refresh:
            log.info("Using cached %s", dest)
        else:
            log.info("Downloading %s", fname)
            http_get(f"{args.meta_base}/{fname}", dest)

    stations = parse_stations(meta / "ghcnd-stations.txt")
    inventory = parse_inventory(meta / "ghcnd-inventory.txt")
    first_by = args.count_start - args.burn_in
    sel = select_stations(stations, inventory, first_year_by=first_by,
                          last_year_at_least=args.count_end,
                          conus_only=not args.include_non_conus)
    out = Path(args.data_dir) / "selected_stations.csv"
    sel.to_csv(out, index=False)
    log.info("Selected %d stations with TMAX+TMIN from <=%d to >=%d -> %s",
             len(sel), first_by, args.count_end, out)


# ----------------------------------------------------------------------------
# Stage 1b: download per-station files
# ----------------------------------------------------------------------------
def cmd_download(args) -> None:
    sel = pd.read_csv(Path(args.data_dir) / "selected_stations.csv", dtype={"station": str})
    raw = Path(args.data_dir) / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    todo = [s for s in sel["station"]
            if args.refresh or not (raw / f"{s}.csv.gz").exists()]
    log.info("%d stations selected, %d to download", len(sel), len(todo))

    failures = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futs = {pool.submit(http_get, args.station_url.format(id=s), raw / f"{s}.csv.gz"): s
                for s in todo}
        for i, fut in enumerate(as_completed(futs), 1):
            s = futs[fut]
            try:
                fut.result()
            except Exception as e:  # noqa: BLE001
                failures.append(s)
                log.error("Failed %s: %s", s, e)
            if i % 100 == 0 or i == len(todo):
                log.info("  %d/%d downloaded", i, len(todo))
    if failures:
        log.warning("%d downloads failed; rerun `download` to retry: %s",
                    len(failures), ", ".join(failures[:10]))


# ----------------------------------------------------------------------------
# Stage 2: load + clean one station
# ----------------------------------------------------------------------------
RAW_NAMES = ["station", "date", "element", "value", "mflag", "qflag", "sflag", "obstime"]


def load_station(path: Path) -> pd.DataFrame:
    """Read a per-station file and return clean TMAX/TMIN rows.

    Cleaning rules:
      * keep TMAX and TMIN only
      * drop missing values (-9999) and anything with a quality flag
      * drop Feb 29 (it would get a 'record' every 4 years for free)
      * keep one value per element-day
    """
    df = pd.read_csv(path, header=None, names=RAW_NAMES, usecols=range(8),
                     dtype={"station": str, "date": str, "element": str, "value": "Int64",
                            "mflag": str, "qflag": str, "sflag": str, "obstime": str},
                     keep_default_na=False, na_values={"value": [""]},
                     low_memory=False)
    if len(df) and df.iloc[0]["station"] == "ID":   # tolerate a header row
        df = df.iloc[1:]
    df = df[df["element"].isin(["TMAX", "TMIN"])]
    df = df[(df["qflag"].str.strip() == "") & df["value"].notna() & (df["value"] != -9999)]

    df = df.assign(date=pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce"))
    df = df[df["date"].notna()]
    df = df[~((df["date"].dt.month == 2) & (df["date"].dt.day == 29))]
    df = (df.drop_duplicates(["element", "date"])
            .sort_values(["element", "date"])
            [["station", "date", "element", "value"]]
            .astype({"value": "int32"})
            .reset_index(drop=True))
    return df


# ----------------------------------------------------------------------------
# Stage 3: record detection
# ----------------------------------------------------------------------------
def _records_for_series(s: pd.DataFrame, element: str) -> pd.DataFrame:
    """For one element's daily series (sorted by date), compute prior extremes per
    calendar day and flag record breaks and ties. Vectorized: no Python loop over days."""
    md = s["date"].dt.strftime("%m%d")
    v = s["value"].astype("float64")   # float so the first-ever value's prior is NaN
    prior_max = v.groupby(md).cummax().groupby(md).shift()
    prior_min = v.groupby(md).cummin().groupby(md).shift()

    hi_type, lo_type = ("high_max", "low_max") if element == "TMAX" else ("high_min", "low_min")
    frames = []
    for typ, prior, brk in ((hi_type, prior_max, v > prior_max),
                            (lo_type, prior_min, v < prior_min)):
        tie = v == prior
        hit = (brk | tie) & prior.notna()
        if hit.any():
            frames.append(pd.DataFrame({
                "station": s.loc[hit, "station"].to_numpy(),
                "date": s.loc[hit, "date"].to_numpy(),
                "type": typ,
                "is_tie": tie[hit].to_numpy(),
                "value_c10": s.loc[hit, "value"].to_numpy(),
                "prior_c10": prior[hit].astype("int32").to_numpy(),
            }))
    if not frames:
        return pd.DataFrame(columns=[c for c in EVENT_COLUMNS if c != "year"])
    return pd.concat(frames, ignore_index=True)


def station_years(df: pd.DataFrame, completeness: float) -> pd.DataFrame:
    """Days present per station-element-year, and whether the year is complete enough."""
    sy = (df.assign(year=df["date"].dt.year)
            .groupby(["station", "element", "year"]).size().rename("n_days").reset_index())
    sy["complete"] = sy["n_days"] >= int(np.ceil(365 * completeness))
    return sy


def detect_station(df: pd.DataFrame, *, count_start: int, count_end: int,
                   burn_in: int, year_completeness: float,
                   station_completeness: float) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Return (events, station_years, qc_row) for one cleaned station.

    Every QC-passing value updates the running record, even in incomplete years
    (a real 105°F reading is still the bar to beat). Events are only *counted*
    when the year is inside the count window, past burn-in, and complete.
    """
    sid = df["station"].iat[0] if len(df) else "?"
    sy = station_years(df, year_completeness)
    qc = {"station": sid, "included": False, "reason": ""}

    window = sy[(sy["year"] >= count_start) & (sy["year"] <= count_end)]
    n_window = count_end - count_start + 1
    for element in ("TMAX", "TMIN"):
        e = sy[sy["element"] == element]
        if e.empty:
            qc["reason"] = f"no {element}"
            return _empty_events(), sy, qc
        first = int(e["year"].min())
        qc[f"{element.lower()}_first_year"] = first
        if first + burn_in > count_start:
            qc["reason"] = f"{element} starts {first}, burn-in ends after {count_start}"
            return _empty_events(), sy, qc
        frac = window[(window["element"] == element) & window["complete"]].shape[0] / n_window
        qc[f"{element.lower()}_complete_frac"] = round(frac, 3)
        if frac < station_completeness:
            qc["reason"] = f"{element} only {frac:.0%} of {count_start}-{count_end} complete"
            return _empty_events(), sy, qc

    events = pd.concat([_records_for_series(df[df["element"] == el].reset_index(drop=True), el)
                        for el in ("TMAX", "TMIN")], ignore_index=True)
    if events.empty:
        qc["included"] = True
        return _empty_events(), sy, qc

    events["date"] = pd.to_datetime(events["date"])
    events["year"] = events["date"].dt.year.astype("int32")
    element_of = {"high_max": "TMAX", "low_max": "TMAX", "high_min": "TMIN", "low_min": "TMIN"}
    events["element"] = events["type"].map(element_of)
    ok = sy[sy["complete"]][["element", "year"]]
    events = events.merge(ok, on=["element", "year"], how="inner")
    events = events[(events["year"] >= count_start) & (events["year"] <= count_end)]
    qc["included"] = True
    return events[EVENT_COLUMNS].reset_index(drop=True), sy, qc


def _empty_events() -> pd.DataFrame:
    return pd.DataFrame({c: pd.Series(dtype=t) for c, t in
                         zip(EVENT_COLUMNS, ["str", "datetime64[ns]", "int32", "str",
                                             "bool", "int32", "int32"])})


def _detect_worker(path_str: str, out_dir_str: str, params: dict) -> tuple[dict, pd.DataFrame]:
    path = Path(path_str)
    try:
        df = load_station(path)
        if df.empty:
            return {"station": path.name.split(".")[0], "included": False,
                    "reason": "no usable TMAX/TMIN"}, pd.DataFrame()
        events, sy, qc = detect_station(df, **params)
        if qc["included"]:
            write_table(events, Path(out_dir_str) / "events" / qc["station"])
        qc["n_events"] = len(events)
        return qc, sy
    except Exception as e:  # noqa: BLE001 — one bad file shouldn't kill the run
        return {"station": path.name.split(".")[0], "included": False,
                "reason": f"error: {e}"}, pd.DataFrame()


def cmd_detect(args) -> None:
    data = Path(args.data_dir)
    out = Path(args.out_dir)
    sel = pd.read_csv(data / "selected_stations.csv", dtype={"station": str})
    files = [data / "raw" / f"{s}.csv.gz" for s in sel["station"]]
    files = [f for f in files if f.exists()]
    log.info("Detecting records in %d station files", len(files))

    params = dict(count_start=args.count_start, count_end=args.count_end,
                  burn_in=args.burn_in, year_completeness=args.year_completeness,
                  station_completeness=args.station_completeness)
    qc_rows, sy_frames = [], []
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futs = [pool.submit(_detect_worker, str(f), str(out), params) for f in files]
        for i, fut in enumerate(as_completed(futs), 1):
            qc, sy = fut.result()
            qc_rows.append(qc)
            if not sy.empty:
                sy_frames.append(sy)
            if i % 100 == 0 or i == len(futs):
                log.info("  %d/%d stations processed", i, len(futs))

    qc = pd.DataFrame(qc_rows).merge(sel[["station", "state", "lat", "lon", "name"]],
                                     on="station", how="left")
    qc.to_csv(out / "station_qc.csv", index=False)
    if sy_frames:
        write_table(pd.concat(sy_frames, ignore_index=True), out / "station_years")
    n_in = int(qc["included"].sum())
    log.info("%d of %d stations passed QC (see %s)", n_in, len(qc), out / "station_qc.csv")
    if n_in < len(qc):
        log.info("Top exclusion reasons:\n%s",
                 qc.loc[~qc["included"], "reason"].str.replace(r"\d+", "N", regex=True)
                   .value_counts().head(5).to_string())


# ----------------------------------------------------------------------------
# Checkpoint: decade ratios
# ----------------------------------------------------------------------------
def load_events(out_dir: Path) -> pd.DataFrame:
    files = sorted((out_dir / "events").glob("*.parquet")) + \
            sorted((out_dir / "events").glob("*.csv.gz"))
    if not files:
        raise FileNotFoundError("No event files — run `detect` first.")
    frames = [read_table(f) for f in files]
    return pd.concat([f for f in frames if not f.empty], ignore_index=True)


def ratio_table(events: pd.DataFrame, by: str, include_ties: bool) -> pd.DataFrame:
    ev = events if include_ties else events[~events["is_tie"].astype(bool)]
    ev = ev[ev["type"].isin(["high_max", "low_min"])]
    t = ev.groupby([by, "type"]).size().unstack(fill_value=0)
    for col in ("high_max", "low_min"):
        if col not in t:
            t[col] = 0
    t = t[["high_max", "low_min"]].rename(columns={"high_max": "record_highs",
                                                   "low_min": "record_lows"})
    t["ratio"] = (t["record_highs"] / t["record_lows"].replace(0, np.nan)).round(2)
    return t


def cmd_summarize(args) -> None:
    out = Path(args.out_dir)
    events = load_events(out)
    events["decade"] = (events["year"] // 10 * 10).astype(int)
    qc = pd.read_csv(out / "station_qc.csv", dtype={"station": str})
    n_stations = int(qc["included"].sum())

    yearly = ratio_table(events, "year", args.include_ties)
    decade = ratio_table(events, "decade", args.include_ties)
    years_in = events.groupby("decade")["year"].agg(["min", "max"])
    decade.index = [f"{d}s" + ("" if years_in.loc[d, "max"] - years_in.loc[d, "min"] == 9
                               else f" ({years_in.loc[d, 'min']}-{years_in.loc[d, 'max']})")
                    for d in decade.index]

    yearly.to_csv(out / "ratio_by_year.csv")
    decade.to_csv(out / "ratio_by_decade.csv")
    ties = "including ties" if args.include_ties else "excluding ties"
    print(f"\nRecord highs (TMAX) vs record lows (TMIN), {n_stations} stations, {ties}\n")
    print(decade.to_string())
    print("\nPublished benchmark (Meehl et al. 2009, ~1,800 CONUS stations):"
          "\n  1950s slightly above 1:1, 1960s-70s below 1:1, rising since the 1980s,"
          "\n  about 2:1 in the 2000s.\n")


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--data-dir", default="data", help="raw downloads + station list (default: data)")
    p.add_argument("--out-dir", default="out", help="events and summaries (default: out)")
    p.add_argument("--count-start", type=int, default=COUNT_START)
    p.add_argument("--count-end", type=int, default=COUNT_END)
    p.add_argument("--burn-in", type=int, default=BURN_IN_YEARS)
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("select", help="choose stations from NOAA metadata")
    s.add_argument("--meta-base", default=NOAA_DOC_BASE)
    s.add_argument("--include-non-conus", action="store_true",
                   help="keep AK, HI and territories (default: lower 48 only)")
    s.add_argument("--refresh", action="store_true", help="re-download metadata")

    d = sub.add_parser("download", help="fetch per-station daily files")
    d.add_argument("--station-url", default=STATION_URL,
                   help="URL template with {id} (default: NCEI by_station .csv.gz)")
    d.add_argument("--workers", type=int, default=8)
    d.add_argument("--refresh", action="store_true", help="re-download existing files")

    t = sub.add_parser("detect", help="find record events per station")
    t.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) - 1))
    t.add_argument("--year-completeness", type=float, default=YEAR_COMPLETENESS)
    t.add_argument("--station-completeness", type=float, default=STATION_COMPLETENESS)

    m = sub.add_parser("summarize", help="decade/year ratios (validation checkpoint)")
    m.add_argument("--exclude-ties", dest="include_ties", action="store_false",
                   help="count only strict record breaks")
    return p


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    Path(args.out_dir).mkdir(parents=True, exist_ok=True)
    {"select": cmd_select, "download": cmd_download,
     "detect": cmd_detect, "summarize": cmd_summarize}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
