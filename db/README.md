# db/ — versioned database schema

This folder holds the versioned source of truth for the Valbura backend's
Supabase Postgres database (project `vccmozgivgcvvapmpfot`, `public` schema).
Until this folder existed, the schema lived only inside the running database;
this folder makes it reproducible from code.

## Layout

```
db/
├── schema.sql              Baseline — complete public schema at the
│                           starting point (16 tables, 24 views,
│                           2 functions, RLS, event trigger).
├── schema.sql.gaps.md      Documented exclusions (Supabase platform
│                           objects we intentionally do not reproduce).
└── migrations/
    ├── 0001_xirr_and_mwr.sql
    └── 0002_*.sql          ← next migration goes here
```

## Setting up a fresh database

1. Apply the baseline:
   ```bash
   psql "$DATABASE_URL" -f db/schema.sql
   ```
2. Apply every file in `db/migrations/` in numeric filename order:
   ```bash
   for f in db/migrations/*.sql; do psql "$DATABASE_URL" -f "$f"; done
   ```

This reproduces the current production schema byte-for-byte (verified
on 2026-05-29).

## Adding a new migration

1. Pick the next number (`0002`, `0003`, …) — never edit existing
   migrations or the baseline.
2. File header: one paragraph describing what the migration does and why.
3. Write it **idempotent**: `CREATE OR REPLACE FUNCTION/VIEW`,
   `CREATE TABLE IF NOT EXISTS`, `ALTER TABLE … ADD COLUMN IF NOT EXISTS`,
   `DO $$ BEGIN IF NOT EXISTS … END $$` for event triggers, etc. Re-applying
   a migration must be a no-op, not an error.
4. Keep it additive when possible. Destructive changes (DROP, type changes)
   need a brief comment explaining the safety reasoning.

## Workflow rule — write the migration FIRST, then run it

**Always:** commit the migration file, then apply it to the DB.
**Never:** make a change in the Supabase SQL editor and "remember to write
the migration later." That is the exact drift this folder was created to fix.
If a hotfix went into the DB out-of-band, write the migration immediately
afterward (same PR) so the repo stays the source of truth.

## What's deliberately NOT here

See `schema.sql.gaps.md`. Short version: Supabase-platform-managed objects
(`pg_stat_statements`, `supabase_vault`, platform event triggers, `anon`/
`authenticated`/`service_role` roles, default ACLs). The platform recreates
these on any fresh Supabase project, so duplicating them in our baseline
would cause ownership conflicts.
