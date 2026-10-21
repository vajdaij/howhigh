# howhigh

[![tests](https://github.com/vajdaij/howhigh/actions/workflows/tests.yml/badge.svg)](https://github.com/vajdaij/howhigh/actions/workflows/tests.yml)

**How often are US weather stations setting record highs, compared with record lows?**

A data pipeline (and, eventually, a web app) tracking how often US weather stations set record
high temperatures compared with record lows, from 1950 to today.

When the climate is stable, new record highs and record lows should turn up at about the same
rate. A ratio that climbs above 1:1 over the decades is a simple, robust signal of warming.
Published research found the US ratio was close to 1:1 in the 1950s, dipped below that in the
1960s and 70s, and reached about 2:1 in the 2000s (Meehl et al., 2009 and 2016). This project
reproduces that analysis and extends it through the present.

## Data

[NOAA GHCN-Daily](https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily):
daily maximum and minimum temperatures from thousands of US weather stations, some going back
to the 1800s. Raw files are downloaded at run time and are not stored in this repo.

## Pipeline

| Stage | Command | What it does |
|---|---|---|
| 1. Select | `python ghcn_records.py select` | Picks lower-48 stations with daily max and min temperatures from 1920 or earlier through 2025 |
| 1. Download | `python ghcn_records.py download` | Fetches each selected station's full daily history |
| 2–3. Detect | `python ghcn_records.py detect` | Cleans the data and finds every record high and record low |
| Check | `python ghcn_records.py summarize` | Prints the ratio by decade, to compare with the published results |

### Method

- **Records are per station and per calendar day.** For example, a station's July 4 high is
  compared with every earlier July 4 high at that station.
- **Quality filtering.** Values that failed NOAA's quality checks are dropped. Feb 29 is skipped,
  since it would set a new record almost every time it comes around.
- **30-year burn-in.** A station's first 30 years only build up its records and are not
  counted, because early years set "records" too easily.
- **Complete years only.** A year counts only if at least 90% of its days have data, and a
  station is kept only if at least 90% of 1950–2025 meets that bar.
- **Four record types are saved.** Record highs and record lows make up the headline ratio.
  Record warm nights and record cold days are also kept.
- **Ties** are counted by default. Use `summarize --exclude-ties` to count strict breaks only.

The defaults can be changed with command-line flags. Run `python ghcn_records.py --help` for the list.

## Setup

```
pip install -r requirements.txt
python test_ghcn_records.py     # self-checks on synthetic data, no network needed
```

The self-checks also run automatically on GitHub for every push and pull request.

## Manual testing

The self-checks use synthetic data. These steps check the pipeline against real NOAA data,
from a quick run to the full analysis.

### 1. Smoke test on a few stations

A full run downloads hundreds of station files. Try the whole pipeline on a handful first:

```
python ghcn_records.py select --limit 5 --also USC00050848
python ghcn_records.py download
python ghcn_records.py detect
python ghcn_records.py summarize
```

`--limit 5` keeps five stations spread across different states, and `--also` adds specific
stations (here Boulder, CO). Every stage should finish without errors, and `detect` should
report that most of the stations passed QC. If one didn't, `out/station_qc.csv` says why.

### 2. Spot-check one station

Print Boulder's most recent record highs in °F:

```
python -c "import pandas as pd; e = pd.read_parquet('out/events/USC00050848.parquet'); h = e[e.type == 'high_max'].copy(); h['temp_f'] = (h.value_c10 / 10 * 9 / 5 + 32).round(); print(h[['date', 'temp_f', 'is_tie']].tail(15))"
```

Compare a few of them with the National Weather Service's published daily records for Boulder
(NOWData on weather.gov). Most should match. Some differences are expected, since NWS sometimes
merges several nearby stations into one city record and this pipeline treats each station
separately. NOAA stores whole tenths of °C, so a reading can also be off by 1°F after converting.

### 3. Sanity-check the scale

New records get rarer as a station's history grows: with *n* years of data, expect about
365 ÷ *n* record highs per station per year. With most stations starting around 1900–1920,
that's roughly 8–12 per station per year in the 1950s and 3–5 by the 2010s. Divide the yearly
counts in `out/ratio_by_year.csv` by the number of stations to compare. Counts far off that
scale usually mean a cleaning bug, such as missing or duplicated days.

### 4. Full run and the benchmark

Run `select` again without `--limit`, then the other three stages. The decade ratios from
`summarize` should follow the published pattern: a little above 1:1 in the 1950s, below 1:1
in the 1960s and 70s, then rising to about 2:1 in the 2000s. The exact numbers will differ
because the station set is different, but the shape should match.

### 5. Check the result is robust

Rerun with different settings and confirm the rising trend holds:

```
python ghcn_records.py summarize --exclude-ties
python ghcn_records.py --burn-in 20 select
python ghcn_records.py --burn-in 20 detect
python ghcn_records.py summarize
```

The ratios will shift, but the direction shouldn't. Note that `--burn-in 20` changes which
stations qualify, so run `download` too if `select` adds stations you don't have yet.

## Outputs

Everything is written to `out/` (git-ignored):

- `events/`: one file per station listing every record event
- `station_years`: days of data per station, temperature type, and year
- `station_qc.csv`: which stations were kept, and why the others weren't
- `ratio_by_decade.csv`, `ratio_by_year.csv`: the headline numbers

## Attribution

Data: NOAA National Centers for Environmental Information, Global Historical Climatology
Network – Daily. The outputs here are derived from NOAA data and are not original, unaltered
NOAA data.

## License

Code is released under the [BSD 3-Clause License](LICENSE). NOAA data is public and is not
covered by this license.
