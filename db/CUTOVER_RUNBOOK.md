# Cutover-Runbook — Wechsel von Test- auf echte Konten

**Status:** Entwurf zur Freigabe. Dieses Dokument führt **nichts** aus.
Kein Löschen, keine ENV-Änderung, kein Code wird durch das Schreiben dieser Datei verändert.

**Letzte Aktualisierung des Kontexts:** Kapital-Start verschoben auf **01.07.**
Der Cutover ist dadurch **zweiphasig**:

| Phase | Wann | Inhalt | Technik? |
|-------|------|--------|----------|
| **Phase 1 — Verbindungs-Cutover** | **jetzt** | Echte (leere) Konten technisch anbinden + verifizieren, Testdaten löschen, echte Keys/Query-IDs setzen | **ja** (dieses Runbook) |
| **Phase 2 — Kapital-Start** | **01.07.** | User transferiert Geld, beginnt Trades | **nein** (kein Technik-Schritt) |

Weil Phase 1 gegen **echte, aber leere** Konten läuft, sind die Wallets (USDT-M / USDC-M / Coin-M / Spot) lesbar und der **40009-Blocker** (USDC-M/Coin-M auf dem Testkey nicht aktiviert) löst sich von selbst. Die **NAV-Vervollständigung** kann daher **direkt nach** dem Verbindungs-Cutover an echten Wallets gebaut und verifiziert werden.

---

## Grundregeln (gelten für jeden Schritt)

- **Sicherheit:** Das DB-Passwort kommt bewusst **nicht** in die Session. Alle DB-Operationen laufen über die Supabase-MCP-Introspektion oder über das Supabase-Dashboard. Secrets (API-Key/Secret/Passphrase) landen **niemals** in Logs, `metadata` oder `error_message` — nur `creds_source` (Name).
- **Jeder Schritt hat einen STOPP-/Verifikationspunkt.** Erst bei grünem Ergebnis weitergehen.
- **Reihenfolge ist bindend:** erst Backup → dann Löschen → dann ENV umstellen → dann erster echter Sync → dann NAV-Vervollständigung → dann Cron reaktivieren.
- **Rollback-Bereitschaft:** Vor dem Löschen wird ein vollständiger In-DB-Snapshot angelegt (Schritt 2). Jeder folgende Schritt nennt seinen eigenen Rollback.

---

## Schritt 0 — Vorbedingungen-Checkliste (BLOCKIEREND)

Phase 1 startet **erst**, wenn **alle** Punkte abgehakt sind. Fehlt einer, abbrechen.

### 0.1 Bitget — beide Konten
- [ ] Echter **Bitget-API-Key für Global** vorhanden (Key + Secret + Passphrase).
- [ ] Echter **Bitget-API-Key für Alternatives** vorhanden (Key + Secret + Passphrase) — **physisch anderes Konto** als Global (sonst keine echte Trennung, nur Spiegelung).
- [ ] Am echten Bitget-Konto sind **USDC-M Futures** und **Coin-M Futures** als Produktlinien **aktiviert**.
- [ ] Key-Scope deckt **alle benötigten Produktlinien** ab: USDT-M, USDC-M, Coin-M Futures **und** Spot (Read-Permission genügt; **kein** Trade/Withdraw nötig).
- [ ] IP-Whitelist des Keys (falls gesetzt) enthält die **Railway-Ausgangs-IP** — sonst 40xx trotz korrektem Key.

> **Warum:** Phase 1 verifiziert u. a., dass **keine** 40037/40009-Fehler mehr auftreten. Das geht nur, wenn echte Keys mit voller Produktlinien-Abdeckung gesetzt sind.

### 0.2 IBKR — beide Konten
- [ ] Echte **Flex-Query-ID für Global** (`IBKR_ACTIVITY_QUERY_ID_GLOBAL`).
- [ ] Echte **Flex-Query-ID für Alternatives** (`IBKR_ACTIVITY_QUERY_ID_ALTERNATIVES`) — **eigene** Query, **nicht** dieselbe wie Global (sonst greift die Fallback-Falle, siehe Schritt 5).
- [ ] `IBKR_FLEX_TOKEN` gültig und deckt beide Queries ab.

### 0.3 Backup-Fähigkeit vorab klären (plan-abhängig!)
- [ ] Im **Supabase-Dashboard → Database → Backups** prüfen, ob **automatische Backups / PITR überhaupt verfügbar** sind. Das ist **plan-abhängig** — auf kleineren Plänen (z. B. Free) gibt es **kein** Plattform-Backup/PITR.
- [ ] Ergebnis dokumentieren:
  - **Verfügbar** → Schritt 2 nutzt 2.a **und** 2.b (zwei Ebenen).
  - **Nicht verfügbar** → **2.a (In-DB-Snapshot) ist der alleinige Primärpfad.** Das ist bewusst zu vermerken; **nicht** auf ein evtl. nicht existierendes Plattform-Backup verlassen. Optional zusätzlich: User macht eigenständig (außerhalb der Session) ein lokales `pg_dump` als zweite Ebene.

### 0.4 Bestätigung dokumentieren
- [ ] User bestätigt schriftlich in der Session: „Alle 0.1/0.2-Punkte erfüllt, Backup-Fähigkeit aus 0.3 geklärt." → erst dann Schritt 1.

**STOPP 0:** Ohne vollständige Checkliste kein Schritt 1.

---

## Schritt 1 — Vorher-Bild (Ist-Aufnahme, nur Lesen)

Bevor irgendetwas verändert wird, das **aktuelle** Bild je Tabelle **und je Broker** festhalten. Bekanntes Baseline-Soll über die 7 transaktionalen Tabellen: **6366 Zeilen gesamt (alle Broker)**. Davon werden **nur** die `Bitget`- und `IBKR`-Zeilen gelöscht; **MT5 bleibt vollständig erhalten**.

```sql
-- Vorher-Bild: Zeilen je Tabelle je Broker
SELECT 'trades'                  AS tbl, broker, count(*) FROM trades                  GROUP BY broker
UNION ALL SELECT 'portfolio_cashflows',      broker, count(*) FROM portfolio_cashflows      GROUP BY broker
UNION ALL SELECT 'realized_pnl_events',      broker, count(*) FROM realized_pnl_events      GROUP BY broker
UNION ALL SELECT 'portfolio_nav_snapshots',  broker, count(*) FROM portfolio_nav_snapshots  GROUP BY broker
UNION ALL SELECT 'positions',                broker, count(*) FROM positions                GROUP BY broker
UNION ALL SELECT 'portfolio_cash',           broker, count(*) FROM portfolio_cash           GROUP BY broker
UNION ALL SELECT 'import_jobs',              broker, count(*) FROM import_jobs              GROUP BY broker
ORDER BY tbl, broker;
```

- [ ] Ergebnis als **Vorher-Bild** notieren (Screenshot/Copy in die Session).
- [ ] Summe aller Zeilen ≈ 6366 (Drift ist normal, da seit der Inventur weitere Syncs liefen — exakte Zahl ist das aktuelle Vorher-Bild, nicht 6366 als Dogma).
- [ ] **MT5-Zeilen je Tabelle separat notieren** — diese Zahlen müssen nach dem Löschen **unverändert** sein.

**STOPP 1:** Vorher-Bild liegt vollständig vor (inkl. expliziter MT5-Zahlen).

---

## Schritt 2 — Backup (BLOCKIEREND, vor jedem Löschen)

Zwei Ebenen, beide ausführen:

### 2.a In-DB-Snapshot der zu löschenden Zeilen (primärer Rollback-Pfad)
Schnell, ohne externes Tooling, ohne Passwort — Kopien der **exakt zu löschenden** Zeilen in Backup-Tabellen. Datum im Namen (Beispiel `20260615`).

> **`20260615` ist ein Platzhalter.** Beim tatsächlichen Ausführen das **reale Datum** des Cutover-Tages einsetzen (Format `YYYYMMDD`) — und **denselben** Suffix konsistent in den Rollback-`INSERT`s (Schritt 3) und im `DROP TABLE` (Schritt 9) verwenden.

```sql
CREATE TABLE _bkp_trades_20260615                  AS SELECT * FROM trades                  WHERE broker IN ('Bitget','IBKR');
CREATE TABLE _bkp_portfolio_cashflows_20260615     AS SELECT * FROM portfolio_cashflows     WHERE broker IN ('Bitget','IBKR');
CREATE TABLE _bkp_realized_pnl_events_20260615     AS SELECT * FROM realized_pnl_events     WHERE broker IN ('Bitget','IBKR');
CREATE TABLE _bkp_portfolio_nav_snapshots_20260615 AS SELECT * FROM portfolio_nav_snapshots WHERE broker IN ('Bitget','IBKR');
CREATE TABLE _bkp_positions_20260615               AS SELECT * FROM positions               WHERE broker IN ('Bitget','IBKR');
CREATE TABLE _bkp_portfolio_cash_20260615          AS SELECT * FROM portfolio_cash          WHERE broker IN ('Bitget','IBKR');
CREATE TABLE _bkp_import_jobs_20260615             AS SELECT * FROM import_jobs             WHERE broker IN ('Bitget','IBKR');
```

- [ ] Für jede Backup-Tabelle gilt: `count(_bkp_*)` == `count(Original WHERE broker IN ('Bitget','IBKR'))`.

### 2.b Supabase-Plattform-Backup (zweite Sicherung — NUR falls in 0.3 als verfügbar bestätigt)
> **Voraussetzung:** Schritt **0.3** hat ergeben, dass Backups/PITR auf dem aktuellen Plan **verfügbar** sind. Ist das **nicht** der Fall, entfällt 2.b ersatzlos und **2.a ist der alleinige Primärpfad** (so in 0.3 dokumentiert) — dann diesen Unterschritt überspringen, **ohne** sich auf ein nicht existierendes Plattform-Backup zu verlassen.

- [ ] Im **Supabase-Dashboard → Database → Backups** den **letzten automatischen Backup-Zeitpunkt** notieren **oder** (falls verfügbar) ein manuelles Backup/PITR-Restore-Point setzen. Dieser Zeitstempel ist der Notfall-Restore-Punkt für die **ganze** DB.
- [ ] Restore-Zeitstempel in die Session schreiben.

> **pg_dump-Alternative:** möglich, erfordert aber den Connection-String/das Passwort — **bewusst nicht** in dieser Session. Falls der User lokal sichern will, macht er das eigenständig außerhalb der Session. Primärpfad ist 2.a (immer) + 2.b (nur wenn plan-seitig verfügbar).

**STOPP 2:** Beide Backup-Ebenen bestätigt. Ohne grünes Backup **kein** Schritt 3.

**Rollback nach Schritt 2:** keiner nötig (nur additive Tabellen). Backup-Tabellen können am Ende von Phase 1 nach finaler Verifikation gedroppt werden (Schritt 9).

---

## Schritt 3 — FK-sichere Löschung der Testdaten (Bitget + IBKR, MT5 ausgenommen)

**FK-Lage (verifiziert):** Alle Fremdschlüssel der 7 Tabellen zeigen ausschließlich auf `portfolios(id)`; **untereinander bestehen keine FKs**. `portfolios` und `fx_rates` werden **nicht** angetastet. Es gibt damit keinen FK-Zwang in der Reihenfolge — die unten gewählte Reihenfolge ist konservativ (abhängige/abgeleitete Daten zuerst, Roh-Trades zuletst) und läuft in **einer Transaktion**.

```sql
BEGIN;

DELETE FROM realized_pnl_events     WHERE broker IN ('Bitget','IBKR');
DELETE FROM positions               WHERE broker IN ('Bitget','IBKR');
DELETE FROM portfolio_nav_snapshots WHERE broker IN ('Bitget','IBKR');
DELETE FROM portfolio_cash          WHERE broker IN ('Bitget','IBKR');
DELETE FROM portfolio_cashflows     WHERE broker IN ('Bitget','IBKR');
DELETE FROM import_jobs             WHERE broker IN ('Bitget','IBKR');
DELETE FROM trades                  WHERE broker IN ('Bitget','IBKR');

-- VOR dem COMMIT prüfen (siehe Verifikation unten). Erst bei grün: COMMIT; sonst ROLLBACK;
COMMIT;
```

> **MT5 explizit ausgenommen:** Der Filter `broker IN ('Bitget','IBKR')` lässt jede `broker='MT5'`-Zeile unberührt. Es wird **kein** `TRUNCATE` verwendet (würde Broker-Filter ignorieren).

**Verifikation (im selben Transaktionsfenster, vor COMMIT, oder direkt danach):**

```sql
-- Soll: Bitget+IBKR == 0 in allen 7 Tabellen
SELECT 'trades' t, count(*) FROM trades WHERE broker IN ('Bitget','IBKR')
UNION ALL SELECT 'portfolio_cashflows',     count(*) FROM portfolio_cashflows     WHERE broker IN ('Bitget','IBKR')
UNION ALL SELECT 'realized_pnl_events',     count(*) FROM realized_pnl_events     WHERE broker IN ('Bitget','IBKR')
UNION ALL SELECT 'portfolio_nav_snapshots', count(*) FROM portfolio_nav_snapshots WHERE broker IN ('Bitget','IBKR')
UNION ALL SELECT 'positions',               count(*) FROM positions               WHERE broker IN ('Bitget','IBKR')
UNION ALL SELECT 'portfolio_cash',          count(*) FROM portfolio_cash          WHERE broker IN ('Bitget','IBKR')
UNION ALL SELECT 'import_jobs',             count(*) FROM import_jobs             WHERE broker IN ('Bitget','IBKR');

-- Soll: MT5-Zahlen identisch zum Vorher-Bild aus Schritt 1
SELECT 'trades' t, count(*) FROM trades WHERE broker='MT5'
UNION ALL SELECT 'realized_pnl_events',     count(*) FROM realized_pnl_events     WHERE broker='MT5'
UNION ALL SELECT 'portfolio_nav_snapshots', count(*) FROM portfolio_nav_snapshots WHERE broker='MT5'
UNION ALL SELECT 'portfolio_cash',          count(*) FROM portfolio_cash          WHERE broker='MT5'
UNION ALL SELECT 'portfolio_cashflows',     count(*) FROM portfolio_cashflows     WHERE broker='MT5'
UNION ALL SELECT 'import_jobs',             count(*) FROM import_jobs             WHERE broker='MT5';
```

- [ ] Bitget+IBKR == **0** in allen 7 Tabellen.
- [ ] MT5-Zahlen **exakt** wie im Vorher-Bild (Schritt 1).

**STOPP 3:** Beide Verifikationen grün. Wenn nicht → **ROLLBACK** (Transaktion) bzw. Rollback aus 2.a.

**Rollback nach Schritt 3** (falls schon committed und doch nötig):
```sql
INSERT INTO trades                  SELECT * FROM _bkp_trades_20260615;
INSERT INTO portfolio_cashflows     SELECT * FROM _bkp_portfolio_cashflows_20260615;
INSERT INTO realized_pnl_events     SELECT * FROM _bkp_realized_pnl_events_20260615;
INSERT INTO portfolio_nav_snapshots SELECT * FROM _bkp_portfolio_nav_snapshots_20260615;
INSERT INTO positions               SELECT * FROM _bkp_positions_20260615;
INSERT INTO portfolio_cash          SELECT * FROM _bkp_portfolio_cash_20260615;
INSERT INTO import_jobs             SELECT * FROM _bkp_import_jobs_20260615;
```

---

## Schritt 4 — ENV-Umstellung in Railway (echte Keys, Test-Brücke auflösen)

Bisher waren `BITGET_ALTERNATIVES_*` als **Test-Brücke** = Global-Werte gesetzt. Jetzt auf die **echten** Alternatives-Werte umstellen.

> **Bindende Reihenfolge — erst ALLE Variablen setzen, dann erst neustarten/deployen.**
> **Alle** unten gelisteten ENV-Variablen — **insbesondere `IBKR_ACTIVITY_QUERY_ID_ALTERNATIVES`** — müssen in **einem Rutsch** gesetzt sein, **bevor** Railway neu startet/deployt. Grund: Nach jedem Deploy feuert **ein** Post-Startup-Auto-Trigger (siehe Hinweis unten). Würde man deployen, solange `IBKR_ACTIVITY_QUERY_ID_ALTERNATIVES` noch leer ist, löste dieser Auto-Trigger einen IBKR-Sync aus, bei dem Alternatives **still** die Global-Query zieht (Fallback-Falle, Schritt 5) — also einen falsch gespiegelten ersten echten Sync. Deshalb: **komplett setzen → einmal deployen.** Schritt 5 ist dann nur noch ein Gegen-Check, kein Eingriff.

**Exakte Variablenliste:**

| Variable | Phase-1-Wert | Anmerkung |
|----------|--------------|-----------|
| `BITGET_GLOBAL_API_KEY` | echter Global-Key | unverändert lassen, falls schon echt |
| `BITGET_GLOBAL_SECRET` | echtes Global-Secret | |
| `BITGET_GLOBAL_PASSPHRASE` | echte Global-Passphrase | |
| `BITGET_ALTERNATIVES_API_KEY` | **echter Alternatives-Key** | **Test-Brücke (=Global) wird aufgelöst** |
| `BITGET_ALTERNATIVES_SECRET` | **echtes Alternatives-Secret** | |
| `BITGET_ALTERNATIVES_PASSPHRASE` | **echte Alternatives-Passphrase** | |
| `IBKR_ACTIVITY_QUERY_ID_GLOBAL` | echte Global-Query-ID | |
| `IBKR_ACTIVITY_QUERY_ID_ALTERNATIVES` | **echte Alternatives-Query-ID** | **explizit setzen** — siehe Schritt 5 |
| `IBKR_FLEX_TOKEN` | gültiger Flex-Token | deckt beide Queries |
| `MT5_INGEST_TOKEN` | unverändert | MT5 bleibt wie es ist |
| `SYNC_ADMIN_TOKEN` | unverändert | |
| `DATABASE_URL` | unverändert | |

- [ ] Alle 6 Bitget-Variablen gesetzt; Global ≠ Alternatives (verschiedene physische Konten).
- [ ] Beide IBKR-Query-IDs gesetzt, **verschieden**.
- [ ] **Erst danach** Service einmal sauber neugestartet (Railway-Redeploy) — **nicht** vorher (sonst feuert der Auto-Trigger gegen unvollständige ENV).

> **Achtung Auto-Trigger:** Nach jedem Railway-Deploy kam in der Vergangenheit **ein** Post-Startup-`/sync/bitget`-POST (kein periodischer Trigger — verifiziert). Das ist harmlos, kann aber den „ersten echten Sync" in Schritt 6 vorwegnehmen. Deshalb in Schritt 6 **immer** den tatsächlich laufenden/letzten Job prüfen, nicht blind neu triggern.

**STOPP 4:** ENV vollständig, Service läuft (`GET /health` == 200).

**Rollback nach Schritt 4:** ENV-Variablen auf vorherige Werte zurücksetzen (Railway hält Versionshistorie der Variablen / vorherige Werte sind dem User bekannt). Test-Brücke wiederherstellbar, indem `BITGET_ALTERNATIVES_*` erneut = Global gesetzt wird.

---

## Schritt 5 — IBKR-Fallback-Falle entschärfen (`main.py:1807`)

**Code (unverändert, nur zur Awareness):**
```python
alternatives_query = os.getenv("IBKR_ACTIVITY_QUERY_ID_ALTERNATIVES") or global_query
```
Ist `IBKR_ACTIVITY_QUERY_ID_ALTERNATIVES` **leer/ungesetzt**, zieht Alternatives **still** die **Global-Query** → beide Konten lägen wieder identisch übereinander, **ohne** Fehler. Das ist die gefährlichste stille Fehlerquelle des Cutovers (kein Crash, nur falsche Daten).

- [ ] In Railway ist `IBKR_ACTIVITY_QUERY_ID_ALTERNATIVES` **explizit** und **≠** `IBKR_ACTIVITY_QUERY_ID_GLOBAL` gesetzt (bereits in Schritt 4 erledigt — hier nur Gegen-Check).
- [ ] Optionaler Härtungs-Vorschlag (separater Code-Schritt, **nicht** Teil dieses Runbooks): Fallback entfernen und bei fehlender Alternatives-Query **hart** fehlschlagen (analog zur Bitget-No-Silent-Fallback-Policy). **Entscheidung beim User**, nicht jetzt.

**STOPP 5:** Alternatives-Query explizit und verschieden.

---

## Schritt 6 — Erster echter Sync + Sanity-Checks (Konten sind LEER)

**Wichtig:** Am 15.–30.06. sind die echten Konten **leer** (Kapital kommt erst 01.07.). Wir verifizieren daher **nicht die Höhe** der Zahlen, sondern **Korrektheit der Anbindung**.

> Weil in Schritt 4 **alle** ENV-Variablen **vor** dem Deploy gesetzt wurden, ist der Post-Deploy-Auto-Trigger jetzt **unkritisch**: Er läuft bereits mit vollständigen echten Keys + beiden IBKR-Query-IDs. Er darf daher als gültiger „erster echter Sync" gewertet werden — einfach den letzten Job prüfen statt blind neu zu triggern.

**Trigger (nur falls nicht schon durch Post-Deploy-Auto-Trigger geschehen):**
```
POST /sync/bitget   (Header: x-admin-token: <SYNC_ADMIN_TOKEN>)
POST /sync/ibkr     (Header: x-admin-token: <SYNC_ADMIN_TOKEN>)
```
Beide Jobs je Portfolio bis terminal pollen: `GET /sync/status/{job_id}`.

**Erwartung & Checks:**

- [ ] **(a) Fehlerfrei:** Beide Bitget-Jobs `success`, **keine** `40037`/`40009`/sign-Fehler in `error_message`. (Beweist: echte Keys, alle Produktlinien lesbar — 40009-Blocker gelöst.) IBKR beide `success`.
- [ ] **(b) Echte Trennung statt Spiegelung:** Global- und Alternatives-Daten sind **nicht mehr identisch**. Bei leeren Konten heißt das i. d. R.: beide nahe null, aber **unabhängig** erhoben (verschiedene Konten-IDs/Snapshots). Gegen-Check:
  ```sql
  -- Es darf NICHT mehr exakt gespiegelt sein (vor dem Cutover waren Counts Global==Alternatives identisch)
  SELECT portfolio_name, broker, count(*)
  FROM import_jobs
  WHERE broker IN ('Bitget','IBKR') AND status='success'
  GROUP BY portfolio_name, broker
  ORDER BY broker, portfolio_name;
  ```
  Bei leeren Konten primär über `creds_source`/Job-Metadaten und Konto-IDs prüfen, nicht über Zeilenzahl-Differenz.
- [ ] **(c) creds_source je Portfolio korrekt:** im `metadata` jedes Bitget-Jobs: Global → `GLOBAL`, Alternatives → `ALTERNATIVES`.
  ```sql
  SELECT id, portfolio_name, status,
         metadata->>'creds_source' AS creds_source, finished_at
  FROM import_jobs
  WHERE broker='Bitget'
  ORDER BY started_at DESC LIMIT 4;
  ```
- [ ] **(d) NAV-Plausibilität bei leerem Konto:** NAVs nahe null, keine/kaum Trades/Cashflows — das ist **erwartet und korrekt**, **kein** Fehler.
- [ ] **(e) FX-NULL-Warnungen** (cashflows/funding `skipped_no_fx`) sind erwartet, solange `fx_rates` Lücken hat — **kein** Cutover-Blocker.

**STOPP 6:** (a)(b)(c) grün. Bei **irgendeinem** `40037`/`40009`/sign-Fehler **sofort stoppen** und `error_message` zeigen (enthält nur die Bitget-Antwort, **nie** den Key) → zurück zu Schritt 0.1 (Key-Scope/Produktlinien) oder Schritt 4 (ENV-Wert).

**Rollback nach Schritt 6:** Bei falscher Anbindung ENV zurück (Schritt 4-Rollback) und ggf. die in Phase 1 erzeugten leeren echten Sync-Zeilen wieder löschen (`broker IN ('Bitget','IBKR')` + Zeitfenster), Backup aus 2.a unberührt.

---

## Schritt 7 — NAV-Vervollständigung (eigener Schritt, NACH dem Verbindungs-Cutover)

Jetzt erst sinnvoll, weil die echten Wallets lesbar sind (USDT-M/USDC-M/Coin-M/Spot). Die Mini-Spec liegt bereits vor (Composite-NAV: `USDT-FUTURES.accountEquity + USDC-FUTURES + COIN-FUTURES (coin×lastPr) + Spot (Stables face + coins×spot lastPr)`, alles in USDT, **eine** Zeile pro `(portfolio,'Bitget',date,currency='USDT')`).

- [ ] **Vorbedingung:** Schritt 6 grün (echte Wallets fehlerfrei abrufbar).
- [ ] **Dry-Run zuerst:** an echten (leeren) Wallets die Composite-Summe lesen, gegen das gespeicherte futures-only NAV vergleichen — **read-only, kein DB-Write, kein Commit** (analog zum bisherigen A2.3-Trockenlauf).
- [ ] **Ticker-Lücken-Policy** (aus Spec): Coin ohne Ticker → **überspringen + protokollieren**, Snapshot **niemals** fehlschlagen lassen; Stable USDC≈USDT≈1.
- [ ] **TWR/MWR-Schutz:** TWR/MWR hängen ausschließlich an `nav` + `portfolio_cashflows`; `cash`/`market_value`/`open_pnl` sind unkritisch. Die **eine-Zeile-pro-Broker**-Invariante (`v_portfolio_daily_nav` nimmt `LIMIT 1` je `(portfolio,broker)`) **muss** erhalten bleiben.
- [ ] Erst nach grünem Dry-Run: Code-Diff zur Freigabe (separater Vorgang, **STOPP vor Commit**), dann Deploy + Verifikation.

**STOPP 7:** Dry-Run plausibel; Implementierung ist ein **eigener** freigabepflichtiger Vorgang, **nicht** Teil dieses Runbook-Laufs.

---

## Schritt 8 — Cron reaktivieren

Der Bitget-Cron auf **cron-job.org** ist seit den Tests **pausiert**.

- [ ] Erst reaktivieren, **wenn** Schritte 6 (und idealerweise 7) grün sind.
- [ ] Cron-Intervall/Job prüfen: zielt auf `POST /sync/bitget` mit `x-admin-token`.
- [ ] Nach Reaktivierung **einen** Cron-Lauf beobachten: 202 → beide Jobs terminal `success`.
- [ ] Falls weitere Crons existieren (IBKR/MT5/FX), Status dokumentieren — keine Doppel-Trigger.

**STOPP 8:** Ein Cron-Lauf grün beobachtet.

**Rollback nach Schritt 8:** Cron erneut pausieren.

---

## Schritt 9 — Abschluss Phase 1

- [ ] Vorher/Nachher-Bilder + alle STOPP-Häkchen archiviert (Session/Notiz).
- [ ] Backup-Tabellen `_bkp_*_20260615` **behalten bis nach 01.07.** (Phase-2-Sicherheitsnetz), danach kontrolliert droppen:
  ```sql
  DROP TABLE IF EXISTS _bkp_trades_20260615, _bkp_portfolio_cashflows_20260615,
    _bkp_realized_pnl_events_20260615, _bkp_portfolio_nav_snapshots_20260615,
    _bkp_positions_20260615, _bkp_portfolio_cash_20260615, _bkp_import_jobs_20260615;
  ```
- [ ] Supabase-Restore-Punkt aus 2.b dokumentiert lassen.

**Phase 1 ist abgeschlossen, wenn:** echte Konten fehlerfrei syncen, Global/Alternatives getrennt, `creds_source` korrekt, NAV-Vervollständigung verifiziert, Cron läuft.

---

## Phase 2 — 01.07. Kapital-Start (KEIN Technik-Schritt)

Am 01.07. ist **keine** Code-/ENV-/DB-Aktion durch dieses Runbook nötig. Der User:

1. **Transferiert Geld** auf die echten Konten (Bitget Global + Alternatives, IBKR Global + Alternatives).
2. **Beginnt zu traden.**

Die bestehende Sync-Pipeline (Cron + `/sync/*`) erfasst ab dann automatisch reale NAVs, Trades, Cashflows und Funding-Fees. **Erwartung:** NAVs steigen von ~0 auf reale Werte; erste echte Trades/Cashflows erscheinen.

**Optionaler Sanity-Check am 01.07.** (rein beobachtend, kein Eingriff):
- [ ] Nach dem ersten Transfer einen Sync abwarten → NAV je Portfolio spiegelt den Transfer wider.
- [ ] Keine `40037`/`40009`-Fehler (wären sonst schon in Phase 1 aufgefallen).

---

## Offene Punkte / Deferred (nach Phase 1 zu terminieren)

- Inkrementelle Funding-Fees (statt Vollabzug bei jedem Sync).
- FX-Backfill (`fx_rates`-Lücken → weniger `skipped_no_fx`).
- `/sync/ibkr/trigger`-Alias nach Go-Live entfernen.
- Optionale Härtung des IBKR-Alternatives-Fallbacks (Schritt 5, „hart fehlschlagen statt Global ziehen").
