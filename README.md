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

### Smoke test before a full run

A full run downloads hundreds of station files. Try the whole pipeline on a handful
first:

```
python ghcn_records.py select --limit 5 --also USC00050848
python ghcn_records.py download
python ghcn_records.py detect
python ghcn_records.py summarize
```

`--limit 5` keeps five stations spread across different states, and `--also` adds specific
stations (here Boulder, CO), which is handy for spot-checking against published local records.
Run `select` again without `--limit` for the full set.

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
