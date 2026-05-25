# Airflow Integration Contract

## Purpose

This document defines what the `data-orchestration-airflow` repo is allowed
to assume about the importable surface of `economic-data-etl`. It is the
contract that lets the orchestration repo extend its DAG without reading
every line of this repo's source. The orchestration container bind-mounts
this repository at `/opt/airflow/etl` and adds it to `sys.path`; every
import statement called out below is verified to resolve against the
current source tree as of the audit run on branch
`audit/airflow-integration-readiness`.

## Macro pipeline contract

The existing `economic_data_pipeline` DAG defines four sequential tasks
(`extract` → `transform` → `load` → `dbt_transform`). The first three
import directly from this repo's `src/` modules; `dbt_transform` runs
inside the dbt project under the orchestration repo and is out of scope
here. The symbols imported from this repo are listed below alongside
the verification result captured by `tmp/verify_dag_imports.py` during
the audit.

| Module        | Symbol                  | Resolves? | Role                                                                     |
|---------------|-------------------------|-----------|--------------------------------------------------------------------------|
| `src.extract` | `fetch_fred_data`       | PASS      | Pulls one FRED series, persists raw JSON, returns parsed observations.   |
| `src.extract` | `fetch_bls_data`        | PASS      | Pulls a batch of BLS series for a year range, persists raw JSON.         |
| `src.transform` | `parse_fred_observations` | PASS  | Parses one FRED series JSON into a tidy DataFrame.                       |
| `src.transform` | `parse_bls_batch`     | PASS      | Parses BLS batch JSON into a tidy DataFrame.                             |
| `src.transform` | `build_dim_series`    | PASS      | Builds the `dim_series` rows from the FRED/BLS/ERS series catalogues.    |
| `src.transform` | `combine_fact_tables` | PASS      | Concatenates per-source frames into one `fact_economic_observations`.    |
| `src.load`    | `ensure_tables_exist`   | PASS      | Idempotent DDL for `fact_economic_observations` and `dim_series`.        |
| `src.load`    | `upsert_observations`   | PASS      | Upserts the fact frame; returns `{inserted, updated, unchanged}` stats.  |
| `src.load`    | `upsert_dim_series`     | PASS      | Upserts the dim frame; returns `{inserted, unchanged}`.                  |
| `src.config` | `FRED_SERIES`            | PASS      | Dict mapping human-readable name to FRED series id.                      |
| `src.config` | `BLS_SERIES`             | PASS      | Dict mapping human-readable name to BLS series id.                       |

Verification ran an import script that put `/path/to/economic-data-etl`
on `sys.path` and asserted every symbol resolves with `hasattr`. All 11
symbols passed; summary line read `summary: 0 failure(s)`.

Import-time behavior to be aware of when wiring DAG tasks:

- Importing `src.config` calls `dotenv.load_dotenv()` to read a `.env`
  file from the repo root, and creates `data/raw/`, `data/processed/`,
  and `data/metadata/` under `BASE_DIR` (the repo root) via
  `Path.mkdir(parents=True, exist_ok=True)`. Inside the bind-mounted
  Airflow container, `BASE_DIR` resolves to `/opt/airflow/etl`, so
  these directories will be created there on first import. This is
  benign for the existing DAG (those directories are where raw JSON
  snapshots are persisted) but worth knowing if the orchestration repo
  ever imports `src.config` from a worker without write access.
- Importing `src.extract`, `src.transform`, `src.load` does not
  configure logging, mutate `os.environ`, or perform IO. They are pure
  function-definition modules at import time.
- `src.main.run_pipeline` is the in-repo CLI orchestrator. The DAG
  bypasses it and calls the lower-level functions directly, which means
  the macro DAG does not call `configure_logging()` from `src.main`'s
  module-scope side effect. If a future task imports `src.main`,
  `src.observability.configure_logging()` runs at import time.

## Grocery pipeline contract

The grocery side of this repo has not yet been wired into the DAG. Each
of the five modules below is audited for the entry points an Airflow
task could call, import-time side effects, and the data contract on
disk.

### `src/sim_ingest.py`

**Public entry points**

- `load_store_summaries(root: Path) -> Iterator[StoreSummaryRecord]` —
  walks `{root}/daily/{MM}/{DD}/{YYYY}/store_summary.csv` in sorted
  order and yields one typed record per CSV row.
- `load_department_sales(root: Path) -> Iterator[DepartmentSalesRecord]` —
  same walk pattern, reading `department_sales.csv` from each date
  directory.
- `load_dim_stores(root: Path) -> pd.DataFrame` — reads
  `{root}/dimensions/dim_stores.csv` and returns a DataFrame with
  `store_id` coerced to `int`.

**Import-time side effects:** none. Pure function-definition module.

**Data contract**

- Reads (paths relative to the `root` argument):
  `daily/{MM}/{DD}/{YYYY}/store_summary.csv`,
  `daily/{MM}/{DD}/{YYYY}/department_sales.csv`,
  `dimensions/dim_stores.csv`.
- Returns: lazy iterators of `StoreSummaryRecord` /
  `DepartmentSalesRecord` (defined in `src.schemas`); `pd.DataFrame`
  for `load_dim_stores`.
- Writes: nothing.

**Airflow callability assessment:** clean

### `src/sim_transform.py`

**Public entry points**

- `build_store_daily_metrics(summaries: Iterable[StoreSummaryRecord], dim_stores: pd.DataFrame) -> pd.DataFrame` —
  normalizes store-summary records into the `store_daily_metrics` frame,
  validates referential integrity against `dim_stores`, sorts by
  `(date, store_id)`.
- `build_department_daily_metrics(records: Iterable[DepartmentSalesRecord], dim_stores: pd.DataFrame) -> pd.DataFrame` —
  same, at the store-day-department grain. Sorts by
  `(date, store_id, department_id)`. Raises `ReconciliationError` on an
  empty input iterable.

**Import-time side effects:** none. Pure pandas + numpy + schema imports.

**Data contract**

- Reads: nothing (in-memory transforms only).
- Returns: `pd.DataFrame` with columns matching
  `STORE_DAILY_METRICS_COLUMNS` or `DEPARTMENT_DAILY_METRICS_COLUMNS`
  from `src.schemas`.
- Writes: nothing.

**Airflow callability assessment:** clean

### `src/sim_cli.py`

**Public entry points**

- `run(input_root: Path, output_dir: Path) -> Path` — loads
  `dim_stores`, walks store summaries, builds the store-day frame,
  enforces a row-count reconciliation, and writes
  `store_daily_metrics.parquet` to `output_dir`. Returns the parquet
  path.
- `run_department_grain(input_root: Path, output_dir: Path) -> Path` —
  the department-grain counterpart to `run`. Writes
  `department_daily_metrics.parquet` and returns its path.
- `write_dim_stores_parquet(dim_stores: pd.DataFrame, output_dir: Path) -> Path` —
  writes `dim_stores.parquet` from a previously loaded `dim_stores`
  DataFrame. Returns the parquet path.
- `main(argv: list[str] | None = None) -> int` — argparse + logging
  wrapper around `run`, `run_department_grain`, and
  `write_dim_stores_parquet`. Returns the process exit code.

**Import-time side effects:** none observable. Module-scope binds three
filename constants and gets one structlog logger. `configure_logging()`
and `os.environ["LOG_LEVEL"] = "debug"` (when `--verbose` is set) only
run inside `main()`.

**Data contract**

- Reads (via `sim_ingest`): the sim engine's `output/` tree.
- Returns: the path of the written parquet file (from `run` and
  `run_department_grain`).
- Writes (under `output_dir`): `store_daily_metrics.parquet`,
  `department_daily_metrics.parquet`, `dim_stores.parquet`.

**Airflow callability assessment:** clean-with-side-effects-noted. The
four public functions are callable directly. To produce all three
parquet artifacts the way `python -m src.sim_cli` does, an Airflow task
needs to call `run`, `load_dim_stores` + `write_dim_stores_parquet`,
and `run_department_grain` in sequence. The side effects (logging
configuration, env-var mutation) only fire inside `main()`, so calling
the four functions directly avoids them.

### `src/detect_rules.py`

**Public entry points**

- `load_rules_config(path: Path) -> dict` — loads and structurally
  validates `config/detection_rules.yaml`. Raises `DetectionConfigError`
  on missing sections, missing rules, or unknown profile keys.
- `run_all_rules(metrics_df: pd.DataFrame, dim_stores_df: pd.DataFrame, rules_config: dict, *, department_metrics_df: pd.DataFrame | None = None) -> pd.DataFrame` —
  evaluates every enabled statistical-band rule against `metrics_df`,
  and additionally evaluates the `department_coverage` structural rule
  against `department_metrics_df` when that frame is supplied. Returns
  the `anomaly_flags` frame with columns matching `ANOMALY_FLAG_COLUMNS`.
  When `department_metrics_df` is `None` the structural rule is silently
  skipped.

**Import-time side effects:** none. Module-scope `_RULE_FUNCS` dict is
populated after function defs but does not run side-effecting code.

**Data contract**

- Reads (in `load_rules_config` only): the rules YAML at the supplied
  path.
- Returns: `dict` (rules config) and `pd.DataFrame` (anomaly flags).
- Writes: nothing.

**Airflow callability assessment:** clean

### `src/detect_cli.py`

**Public entry points**

- `run(metrics_path: Path, rules_path: Path, output_dir: Path, *, sim_output_root: Path | None = None, dim_stores_path: Path | None = None, department_metrics_path: Path | None = None) -> Path` —
  loads the rules config, loads `dim_stores` from either a sim engine
  output tree (`sim_output_root`) or a committed `dim_stores.parquet`
  (`dim_stores_path`) — exactly one of the two must be supplied — reads
  the metrics parquet, validates required columns, evaluates rules,
  writes `anomaly_flags.parquet`. When `department_metrics_path` is
  supplied the department-grain parquet is read and the
  `department_coverage` structural rule is evaluated; when it is `None`
  the structural rule is skipped. Returns the output path.
- `main(argv: list[str] | None = None) -> int` — argparse wrapper
  around `run`. The `--sim-output-root` and `--dim-stores-path` flags
  are wired as a mutually exclusive, required group; the
  `--department-metrics-path` flag is optional. Returns the process exit
  code.

**Import-time side effects:** none observable. Same shape as
`sim_cli.py`: filename constant + structlog logger at module scope;
`configure_logging()` and `os.environ` mutation only inside `main()`.

**Data contract**

- Reads: `store_daily_metrics.parquet` (from `metrics_path`);
  `detection_rules.yaml` (from `rules_path`, defaulting on the CLI to
  `config/detection_rules.yaml`); either `dimensions/dim_stores.csv`
  under `sim_output_root` or a committed `dim_stores.parquet` at
  `dim_stores_path`; `department_daily_metrics.parquet` (from
  `department_metrics_path`) when supplied.
- Returns: the path of the written parquet file.
- Writes: `anomaly_flags.parquet` to `output_dir`.

**Airflow callability assessment:** clean-with-side-effects-noted. The
public `run()` function is the obvious entry point. As with `sim_cli`,
calling `main()` from an Airflow task would trigger logging
configuration and an environment-variable mutation, both undesirable
inside a managed worker process; orchestration tasks should call `run`
directly. The `dim_stores` input can come from either `sim_output_root`
or `dim_stores_path`, which lets detection re-run against the committed
canonical parquets without the upstream sim engine output on hand.

## Simulation engine boundary

This ETL repo expects the sim engine to write a directory tree of the
shape below. The boundary is documented in this repo's README at the
"Simulation engine ingestion" section; the audit confirmed the shape against
`sim_ingest.py`'s actual read paths.

```
output/
├── daily/{MM}/{DD}/{YYYY}/store_summary.csv
├── daily/{MM}/{DD}/{YYYY}/department_sales.csv
└── dimensions/dim_stores.csv
```

Required CSV columns are declared in `src/schemas.py`:

- `store_summary.csv`: `STORE_SUMMARY_REQUIRED_COLUMNS` —
  `date_key, store_id, net_sales_total, transactions_total, labor_cost_pct`.
- `department_sales.csv`: `DEPARTMENT_SALES_REQUIRED_COLUMNS` —
  `date_key, store_id, department_id, net_sales, transactions, units_sold, gross_margin_pct`.
- `dim_stores.csv`: `DIM_STORES_REQUIRED_COLUMNS` — only `store_id` is
  contractually required; other columns flow through to
  `dim_stores.parquet` per `DIM_STORES_FULL_COLUMNS`.

Extra columns on either CSV are silently ignored, so additions on the
sim engine side do not break ingest. The QA artifact `anomaly_log.csv`
is explicitly NOT consumed by `sim_ingest`; it is read only by
`scripts/evaluate_detection.py`, which is not imported by any module
under `src/`.

## Data contracts between layers

| Layer transition                              | File(s) and format(s) at the boundary                                                                |
|-----------------------------------------------|------------------------------------------------------------------------------------------------------|
| sim engine → `sim_ingest` input               | `output/daily/{MM}/{DD}/{YYYY}/{store_summary,department_sales}.csv`, `output/dimensions/dim_stores.csv` |
| `sim_ingest` → `sim_transform` input          | In-memory `Iterable[StoreSummaryRecord]` / `Iterable[DepartmentSalesRecord]` + `dim_stores` DataFrame |
| `sim_transform` output                        | In-memory `pd.DataFrame` matching `STORE_DAILY_METRICS_COLUMNS` / `DEPARTMENT_DAILY_METRICS_COLUMNS`  |
| `sim_cli` writes (canonical artifacts)        | `store_daily_metrics.parquet`, `department_daily_metrics.parquet`, `dim_stores.parquet` (pyarrow)    |
| `detect_cli` reads                            | `store_daily_metrics.parquet` (from `--metrics-path`); `output/dimensions/dim_stores.csv` (from `--sim-output-root`) or `dim_stores.parquet` (from `--dim-stores-path`); `config/detection_rules.yaml` (from `--rules-path`); `department_daily_metrics.parquet` (from `--department-metrics-path`, optional) |
| `detect_cli` writes                           | `anomaly_flags.parquet` (pyarrow)                                                                    |

The four committed parquet files at `data/processed/canonical/` are the
authoritative downstream input for the `economic-data-api` repo's
bundled fixtures. They are byte-identical reproducible from a fixed sim
engine output via `scripts/build_canonical_fixtures.py`.

## Known refactor needs

No known refactor needs at this time. All five grocery modules are
callable from an Airflow task in their current shape. `sim_cli.py` and
`detect_cli.py` are rated `clean-with-side-effects-noted` rather than
`requires-refactor` because the side effects (logging configuration,
env-var mutation) live inside `main()` and are bypassed by calling the
public functions directly.

## Open questions for orchestration extension

These questions are unresolved by the current source tree and would
need the user's judgment to settle before the DAG extension is drafted.

1. The grocery pipeline assumes a date-windowed sim engine output tree
   under `--input-root`. Does the DAG need to parameterize the date
   window (e.g., one Airflow run = one date), or does it run the full
   tree the sim engine produces and let the parquets accumulate? The
   CLIs accept a directory, not a date.
2. `detect_cli.run` accepts `dim_stores` from either the sim-output
   tree (`--sim-output-root`) or a committed `dim_stores.parquet`
   (`--dim-stores-path`). The orchestration repo can therefore wire the
   detection task to depend only on the parquets `sim_cli` produced and
   clean up the sim-output tree between tasks; the open question is
   which of the two the DAG should prefer in practice.
3. Should the orchestration repo run the simulation engine itself
   (e.g., a `BashOperator` or `KubernetesPodOperator` invoking the sim
   engine container) before kicking off `sim_cli`? This determines
   whether the DAG's first grocery-side task is "run sim engine" or
   "ingest sim output that already exists on a shared volume."
4. After `detect_cli` finishes, the API's bundled fixtures need to be
   refreshed. Is that a downstream Airflow task that calls
   `scripts/build_canonical_fixtures.py`, or does the API repo poll for
   fresh parquets independently? The contract for invalidating the
   portal's cache is also undefined here.
5. Where does the rules YAML live in the orchestration container? The
   `--rules-path` defaults to `config/detection_rules.yaml` resolved
   from the working directory; an Airflow task running with
   `cwd=/opt/airflow/etl` would resolve that to
   `/opt/airflow/etl/config/detection_rules.yaml`, which works given
   the bind mount, but the dependency should be explicit in the DAG.
