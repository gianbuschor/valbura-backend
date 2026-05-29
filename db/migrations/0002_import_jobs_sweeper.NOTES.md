# Migration 0002 — Stale-Job Sweeper (NO DDL)

This step of the Railway-timeout fix (Block C.1, Stufe 1) requires **no
schema change**. The implementation is entirely in `main.py`:

- `sweep_stale_import_jobs(conn)` — UPDATE that flips any `import_jobs`
  row stuck in `status='started'` for longer than 15 minutes to
  `status='failed'` with an explanatory `error_message`.
- Called once at app startup via FastAPI lifespan handler (cleans up
  zombies left by the previous container).
- New `GET /sync/status/{job_id}` admin-token-protected endpoint.

## Why no DDL / no index?

The sweeper's predicate is

```sql
WHERE status = 'started' AND started_at < now() - interval '15 minutes'
```

Today `import_jobs` is **1643 rows / 2.5 MB**. A sequential scan over
the whole table takes well under a millisecond. The query runs at most
a handful of times per cron tick — vastly more than fast enough.

## When this would change

If `import_jobs` grows past ~100k rows AND the sweeper noticeably shows
up in `pg_stat_statements`, a **partial index** on the predicate is the
surgical fix — at steady state the predicate matches ~0 rows because
the sweeper itself drains them, so the index stays tiny:

```sql
-- Future migration if needed (do NOT apply now):
CREATE INDEX IF NOT EXISTS import_jobs_stale_started_idx
  ON public.import_jobs (started_at)
  WHERE status = 'started';
```

Until that signal exists, this file is intentionally a note, not SQL.
The numeric slot `0002_` is reserved so the next real DDL migration
starts at `0003_`.
