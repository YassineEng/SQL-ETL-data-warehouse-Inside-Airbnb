Calendar loader notes and changes

What I changed

- `sql/data/04_load_calendar.sql` now includes a pre-step that computes the min/max date from the loaded temp table (`#temp_calendar`) and inserts any missing dates into `dim_dates` for that range. This prevents zero-row inserts when `dim_dates` is missing rows for the file's date range.

Why

- The loader previously did an INNER JOIN to `dim_dates` using `CONVERT(DATE, c.date) = d.full_date`. If `dim_dates` lacked rows for the dates in the calendar file, the INSERT returned zero rows silently.

How it works now

1. The calendar CSV is BULK INSERTed into `#temp_calendar`.
2. The SQL computes `@min_date` and `@max_date` from `#temp_calendar`.
3. It generates the full date range between `@min_date` and `@max_date` and inserts any missing `dim_dates` rows.
4. The existing INSERT into `fact_calendar` then joins to `dim_dates` and `dim_listings` as before.

Notes and safe-guards

- The date-range population uses a recursive CTE and `OPTION (MAXRECURSION 0)` — this is safe for reasonable ranges (e.g., a few years). If you expect extremely wide ranges, consider limiting or batching.
- We compute weekdays via `DATEPART(weekday, ...)` — be aware SQL Server's first-day-of-week setting affects weekday numbering; that only changes which days are considered weekends in the is_weekend flag.

How to run a sample (recommended)

1. Create a sample (already provided): `scripts/run_sample_calendar.py` — this creates a gz sample in `data/cleaned_data/`.
2. Uncompress and run the SQL using the debug runner (keeps the temp CSV for inspection):

```powershell
.venv\Scripts\python.exe scripts\calendar_debug_unzip_and_run.py
```

3. Or run the full loader option (after reviewing logs):

```powershell
.venv\Scripts\python.exe main.py 3
```

Files added/changed

- Modified: `sql/data/04_load_calendar.sql` — added dim_dates population pre-step
- New: `docs/README_calendar_loader.md` — this document
- Supporting debug scripts already in `scripts/` (e.g., `calendar_debug_unzip_and_run.py`, `run_sample_calendar.py`, `debug_calendar_match.py`, `ensure_dim_dates_for_sample.py`)

If you want, I can also:
- Move the dim_dates pre-step into `modules/data_loader.py` so the SQL files remain pure DDL/DML templates.
- Add an explicit test that runs the sample end-to-end and asserts `fact_calendar` rowcount increased.
