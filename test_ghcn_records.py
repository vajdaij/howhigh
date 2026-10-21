#!/usr/bin/env python3
"""Self-checks for ghcn_records.py. Run: python test_ghcn_records.py
(also works under pytest). Uses synthetic data, so no network needed."""
from __future__ import annotations

import gzip
import shutil
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

import ghcn_records as gr

try:  # under pytest, give tests a `tmp` directory fixture
    import pytest

    @pytest.fixture
    def tmp(tmp_path):
        return tmp_path
except ImportError:
    pass


# ---------------------------------------------------------------- helpers
def _series(station, element, dates, values):
    return pd.DataFrame({"station": station, "date": pd.to_datetime(dates),
                         "element": element, "value": np.array(values, dtype="int32")})


def _write_station_file(path: Path, station: str, start: int, end: int,
                        tmax_fn, tmin_fn, drop_frac=0.0, rng=None,
                        flag_some=False):
    """Write a per-station file in NOAA's by_station CSV layout (no header)."""
    rng = rng or np.random.default_rng(0)
    days = pd.date_range(f"{start}-01-01", f"{end}-12-31", freq="D")
    rows = []
    for d in days:
        if drop_frac and rng.random() < drop_frac:
            continue
        ymd = d.strftime("%Y%m%d")
        q = "X" if flag_some and rng.random() < 0.001 else ""
        rows.append(f"{station},{ymd},TMAX,{int(tmax_fn(d, rng))},,{q},7,0700")
        rows.append(f"{station},{ymd},TMIN,{int(tmin_fn(d, rng))},,,7,0700")
        rows.append(f"{station},{ymd},PRCP,0,,,7,0700")
    with gzip.open(path, "wt") as f:
        f.write("\n".join(rows) + "\n")


# ---------------------------------------------------------------- unit tests
def test_record_logic_breaks_and_ties():
    # Same calendar day (Jul 4) across 5 years: 30.0, 31.0, 31.0 (tie), 29.0, 32.5 °C
    dates = [f"{y}-07-04" for y in range(2000, 2005)]
    s = _series("S", "TMAX", dates, [300, 310, 310, 290, 325])
    ev = gr._records_for_series(s, "TMAX")
    hi = ev[ev["type"] == "high_max"].sort_values("date")
    assert list(hi["date"].dt.year) == [2001, 2002, 2004], hi
    assert list(hi["is_tie"]) == [False, True, False]
    assert list(hi["prior_c10"]) == [300, 310, 310]
    lo = ev[ev["type"] == "low_max"]
    assert list(lo["date"].dt.year) == [2003], lo   # 29.0 is a record cold day
    # First observation of a calendar day is never a record
    assert 2000 not in set(ev["date"].dt.year)


def test_calendar_days_are_independent():
    # A hot Jul 5 must not affect Jul 4 records
    s = _series("S", "TMAX", ["2000-07-04", "2000-07-05", "2001-07-04"], [300, 400, 310])
    ev = gr._records_for_series(s, "TMAX")
    hi = ev[ev["type"] == "high_max"]
    assert len(hi) == 1 and hi["date"].iloc[0] == pd.Timestamp("2001-07-04")


def test_tmin_types():
    s = _series("S", "TMIN", ["2000-01-01", "2001-01-01", "2002-01-01"], [-50, -80, -20])
    ev = gr._records_for_series(s, "TMIN")
    assert set(ev["type"]) == {"low_min", "high_min"}
    assert ev.loc[ev["type"] == "low_min", "date"].dt.year.tolist() == [2001]
    assert ev.loc[ev["type"] == "high_min", "date"].dt.year.tolist() == [2002]


def test_load_station_cleaning(tmp: Path):
    p = tmp / "USC00000001.csv.gz"
    lines = [
        "USC00000001,20000228,TMAX,100,,,7,0700",
        "USC00000001,20000229,TMAX,500,,,7,0700",   # leap day -> dropped
        "USC00000001,20000301,TMAX,-9999,,,7,0700",  # missing -> dropped
        "USC00000001,20000302,TMAX,600,,X,7,0700",   # QC flagged -> dropped
        "USC00000001,20000303,TMIN,50,,,7,",
        "USC00000001,20000303,PRCP,12,,,7,0700",     # other element -> dropped
    ]
    with gzip.open(p, "wt") as f:
        f.write("\n".join(lines) + "\n")
    df = gr.load_station(p)
    assert len(df) == 2, df
    assert set(df["element"]) == {"TMAX", "TMIN"}
    assert df["value"].dtype == "int32"


def test_metadata_parsing_and_selection(tmp: Path):
    st = tmp / "ghcnd-stations.txt"
    inv = tmp / "ghcnd-inventory.txt"
    srows = [("USW00094728", 40.7789, -73.9692, 39.6, "NY", "NEW YORK CNTRL PK TWR"),
             ("USC00500000", 61.2, -149.9, 30.0, "AK", "ANCHORAGE"),
             ("USC00050848", 39.9919, -105.2667, 1671.5, "CO", "BOULDER"),
             ("USC00051111", 39.7, -104.9, 1600.0, "CO", "SHORT RECORD"),
             ("CA001234567", 45.0, -75.0, 100.0, "ON", "CANADA")]
    with open(st, "w") as f:
        for r in srows:
            f.write(f"{r[0]:11s} {r[1]:8.4f} {r[2]:9.4f} {r[3]:6.1f} {r[4]:2s} {r[5]:30s}\n")
    irows = [("USW00094728", "TMAX", 1869, 2026), ("USW00094728", "TMIN", 1869, 2026),
             ("USC00500000", "TMAX", 1900, 2026), ("USC00500000", "TMIN", 1900, 2026),
             ("USC00050848", "TMAX", 1893, 2026), ("USC00050848", "TMIN", 1893, 2026),
             ("USC00050848", "PRCP", 1893, 2026),
             ("USC00051111", "TMAX", 1960, 2026), ("USC00051111", "TMIN", 1960, 2026),
             ("CA001234567", "TMAX", 1880, 2026), ("CA001234567", "TMIN", 1880, 2026)]
    with open(inv, "w") as f:
        for r in irows:
            f.write(f"{r[0]:11s} {0:8.4f} {0:9.4f} {r[1]:4s} {r[2]:4d} {r[3]:4d}\n")

    stations, inventory = gr.parse_stations(st), gr.parse_inventory(inv)
    assert stations.loc[stations["station"] == "USC00050848", "state"].item() == "CO"
    assert stations.loc[0, "name"] == "NEW YORK CNTRL PK TWR"
    sel = gr.select_stations(stations, inventory, first_year_by=1920,
                             last_year_at_least=2025, conus_only=True)
    assert sorted(sel["station"]) == ["USC00050848", "USW00094728"], sel


def test_sample_stations_for_smoke_tests():
    states = ["AZ"] * 10 + ["CO"] * 10 + ["KS"] * 10 + ["NY"] * 10
    sel = pd.DataFrame({"station": [f"USC{i:08d}" for i in range(40)], "state": states})
    picked = gr.sample_stations(sel, 4)
    assert len(picked) == 4
    assert set(picked["state"]) == {"AZ", "CO", "KS", "NY"}   # spread, not clustered
    assert picked.equals(gr.sample_stations(sel, 4))          # deterministic
    # --also stations are always kept and count toward the limit
    picked = gr.sample_stations(sel, 3, ["USC00000015", "NOT_A_STATION"])
    assert len(picked) == 3 and "USC00000015" in set(picked["station"])
    assert len(gr.sample_stations(sel, 100)) == 40            # limit larger than list


# ---------------------------------------------------------------- end-to-end
def test_end_to_end_synthetic(tmp: Path):
    """Two synthetic climates through detect + summarize:
    flat climate -> highs:lows near 1:1; warming after 1980 -> ratio clearly > 1."""
    data, out = tmp / "data", tmp / "out"
    (data / "raw").mkdir(parents=True)

    def seasonal(d):
        return 150 * np.sin(2 * np.pi * (d.dayofyear - 100) / 365)

    def flat_max(d, rng): return 200 + seasonal(d) + rng.normal(0, 40)
    def flat_min(d, rng): return 50 + seasonal(d) + rng.normal(0, 40)
    def warm(d):  # +2.0 °C by 2025, starting 1980
        return max(0.0, (d.year - 1980) / 45) * 20
    def warm_max(d, rng): return flat_max(d, rng) + warm(d)
    def warm_min(d, rng): return flat_min(d, rng) + warm(d)

    specs = [("USC00000010", flat_max, flat_min, "KS"),
             ("USC00000011", flat_max, flat_min, "KS"),
             ("USC00000020", warm_max, warm_min, "AZ"),
             ("USC00000021", warm_max, warm_min, "AZ"),
             ("USC00000030", warm_max, warm_min, "AZ")]
    for i, (sid, fmax, fmin, _) in enumerate(specs):
        _write_station_file(data / "raw" / f"{sid}.csv.gz", sid, 1900, 2025,
                            fmax, fmin, drop_frac=0.02, rng=np.random.default_rng(i),
                            flag_some=True)
    # A station with a big data gap in 1990-1999 must be excluded by QC
    gap = "USC00000040"
    _write_station_file(data / "raw" / f"{gap}.csv.gz", gap, 1900, 1989, flat_max, flat_min)
    pd.DataFrame({"station": [s[0] for s in specs] + [gap],
                  "state": [s[3] for s in specs] + ["KS"],
                  "lat": 0.0, "lon": 0.0, "name": "SYNTH"}) \
      .to_csv(data / "selected_stations.csv", index=False)

    common = ["--data-dir", str(data), "--out-dir", str(out)]
    gr.main(common + ["detect", "--workers", "2"])
    qc = pd.read_csv(out / "station_qc.csv", dtype={"station": str}).set_index("station")
    assert not qc.loc[gap, "included"], qc
    assert qc["included"].sum() == 5

    events = gr.load_events(out)
    assert events["year"].between(1950, 2025).all()
    assert set(events["type"]) == {"high_max", "low_min", "high_min", "low_max"}

    ev = events.assign(decade=events["year"] // 10 * 10)
    flat = gr.ratio_table(ev[ev["station"].str.startswith("USC0000001")], "decade", True)
    warmed = gr.ratio_table(ev[ev["station"].isin(["USC00000020", "USC00000021",
                                                   "USC00000030"])], "decade", True)
    print("\nflat climate:\n", flat, "\nwarming climate:\n", warmed)
    flat_all = flat[["record_highs", "record_lows"]].sum()
    assert 0.75 < flat_all["record_highs"] / flat_all["record_lows"] < 1.33
    assert warmed.loc[2010, "ratio"] > 2.0
    assert warmed.loc[2010, "ratio"] > warmed.loc[1960, "ratio"]

    gr.main(common + ["summarize"])
    assert (out / "ratio_by_decade.csv").exists() and (out / "ratio_by_year.csv").exists()


if __name__ == "__main__":
    import inspect
    tests = [(n, f) for n, f in globals().items() if n.startswith("test_") and callable(f)]
    for name, fn in tests:
        tmp = Path(tempfile.mkdtemp())
        try:
            fn(tmp) if "tmp" in inspect.signature(fn).parameters else fn()
            print(f"PASS {name}")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
    print(f"\nAll {len(tests)} tests passed.")
