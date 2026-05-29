# Baseline gaps — explicit list of items not reproduced in `db/schema.sql`

The baseline reconstructs the `public` schema of the Valbura Supabase database
via MCP introspection (no `pg_dump`). It deliberately scopes itself to
**user-owned objects in `public`**. The following items are not in the file —
each with reasoning so nothing is silently dropped.

## 1. Supabase-platform-managed (intentionally excluded)

These exist in every Supabase project and are recreated automatically by the
platform. Including them in the user-owned baseline would risk owner/permission
mismatches on apply.

- **Extensions owned by the Supabase platform**
  - `pg_stat_statements` 1.11 in schema `extensions`
  - `supabase_vault` 0.3.1 in schema `vault`
  - `plpgsql` 1.0 (always present)
- **Platform event triggers** (owned by `supabase_admin`)
  - `pgrst_ddl_watch`, `pgrst_drop_watch` — PostgREST schema-cache invalidation
  - `issue_pg_cron_access`, `issue_pg_graphql_access`, `issue_pg_net_access` — extension permission grants
  - `issue_graphql_placeholder` — pg_graphql lifecycle
- **Roles** `anon`, `authenticated`, `service_role`, `authenticator`, `postgres`, `supabase_admin`, `dashboard_user`, … — created by the platform.
- **Default privileges on `public`** (`pg_default_acl`)
  - Owned by `postgres` and `supabase_admin`; auto-grant ALL on new tables/funcs/sequences to `anon`/`authenticated`/`service_role`.
- **Per-object grants on existing tables/views** to `anon`/`authenticated`/`postgres`/`service_role`
  - These are a *consequence* of the default ACLs above when the platform creates tables; they re-materialize automatically when the baseline is applied on a fresh Supabase project. We don't emit explicit `GRANT` statements.

## 2. Reproduced behaviorally, not byte-for-byte

- **User event trigger `ensure_rls`** — emitted via `CREATE EVENT TRIGGER` inside a `DO` block (idempotent). The trigger function `public.rls_auto_enable()` is included with its full body. `ALTER EVENT TRIGGER ... OWNER TO postgres` is **not** emitted (owner defaults to the role running the script).
- **`SECURITY DEFINER` on `rls_auto_enable()`** — preserved verbatim in `CREATE OR REPLACE FUNCTION`. Owner attribution again defers to the applying role.

## 3. Confirmed absent in the source DB (nothing to reproduce)

These were checked by introspection and found empty. Listed so the reader knows the baseline didn't simply forget them.

- **No regular triggers** on any `public` table (`pg_trigger WHERE NOT tgisinternal` → 0 rows).
- **No RLS policies** on any `public` table (`pg_policies` filtered on schema `public` → 0 rows). RLS is enabled, no policy = backend uses `service_role` (BYPASSRLS) exclusively.
- **No sequences** in `public` (`pg_sequence` → 0 rows). All PKs are uuid with `gen_random_uuid()` (pgcrypto) or `uuid_generate_v4()` (uuid-ossp).
- **No CHECK constraints** on any table.
- **No COMMENT** on any table, view, column, or function (`pg_description` filtered on schema `public` → 0 rows).
- **No materialized views.**
- **No generated columns** (`pg_attribute.attgenerated` empty everywhere).
- **No identity columns** (`pg_attribute.attidentity` empty everywhere).

## 4. Subtle behavioral notes (not gaps, but to flag)

- **Compound uniqueness on `realized_pnl_events`**: the table has BOTH a `UNIQUE` *constraint* `(broker, external_id)` AND a separate partial `UNIQUE INDEX … WHERE external_id IS NOT NULL`. The redundancy is real (preserved verbatim) — looks intentional to allow strict NULL handling.
- **`portfolio_cashflows` uniqueness** is implemented as a `UNIQUE INDEX` only (not a constraint). Behaves identically for INSERT…ON CONFLICT, but `pg_constraint` won't list it.
- **`search_path TO public, extensions`** is set at the top of `schema.sql` so that defaults like `gen_random_uuid()` resolve. On a fresh Supabase project the per-role search_path normally already includes `extensions`; the explicit `SET` makes the script robust.
- **`get_trades_enriched()` uses `public.trades` etc. with explicit schema** — preserved verbatim; couples the function body to the schema name `public` (acceptable for production, awkward for the temp-schema verification — see Step 3 notes).
- **`rls_auto_enable()` checks `cmd.schema_name IN ('public')`** as a literal — same `public` coupling. Intended behavior.
