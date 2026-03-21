"""
ANAC Procurement Intelligence — Tool implementations.

Data source: ANAC Open Data Portal (CKAN)
  https://dati.anticorruzione.it/opendata/
  License: CC-BY-SA 4.0

Architecture: DuckDB in-memory database populated from ANAC monthly CSV ZIPs.
  - Startup: downloads stazioni_appaltanti (~2.7 MB) + last 3 months of cig data
  - Background thread extends coverage to 12 months
  - All tools run DuckDB SQL queries (sub-second responses, zero timeout risk)

WAF note: ANAC's Volterra WAF blocks default curl/Python UAs with fake HTTP 200
responses. All requests MUST include a browser User-Agent.

Key datasets:
  cig-YYYY   — tender notices (one ZIP per month). 60 columns including:
               cig, oggetto_gara, importo_lotto, cod_cpv, descrizione_cpv,
               cf_amministrazione_appaltante, denominazione_amministrazione_appaltante,
               sezione_regionale, tipo_scelta_contraente, data_pubblicazione,
               stato, anno_pubblicazione, mese_pubblicazione, FLAG_PNRR_PNC
  stazioni_appaltanti — contracting authority registry (~2.7 MB total)
"""

import io
import os
import threading
import time
import zipfile
from datetime import date, datetime
from typing import Optional

import duckdb
import httpx

# ─────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────

ANAC_DOWNLOAD = "https://dati.anticorruzione.it/opendata/download/dataset"

BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

CITATION = (
    "Fonte: ANAC — Banca Dati Nazionale Contratti Pubblici (BDNCP). "
    "Licenza: CC-BY-SA 4.0. "
    "URL: https://dati.anticorruzione.it"
)

DETAIL_URL = (
    "https://dati.anticorruzione.it/superset/dashboard/dettaglio_cig/?cig={cig}"
)

# ─────────────────────────────────────────────
# Database state
# ─────────────────────────────────────────────

_db: Optional[duckdb.DuckDBPyConnection] = None
_db_lock = threading.Lock()
_db_status: dict = {
    "state": "initializing",   # initializing | ready | error
    "months_loaded": [],
    "row_count": 0,
    "authorities_count": 0,
    "last_updated": None,
    "error": None,
}

# ─────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────


def _http_get(url: str, timeout: float = 120.0) -> httpx.Response:
    """GET with browser User-Agent — required to bypass ANAC Volterra WAF."""
    with httpx.Client(
        follow_redirects=True,
        timeout=timeout,
        headers={"User-Agent": BROWSER_UA, "Accept": "*/*"},
    ) as client:
        return client.get(url)


def _download_csv_from_zip(url: str) -> bytes:
    """Download a ZIP archive and return the bytes of the first .csv inside."""
    resp = _http_get(url)
    resp.raise_for_status()
    zf = zipfile.ZipFile(io.BytesIO(resp.content))
    csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
    if not csv_names:
        raise ValueError(
            f"No CSV file found in ZIP from {url}. Contents: {zf.namelist()}"
        )
    return zf.read(csv_names[0])


# ─────────────────────────────────────────────
# Schema
# ─────────────────────────────────────────────

_CREATE_CIG = """
CREATE TABLE IF NOT EXISTS cig (
    cig                 VARCHAR PRIMARY KEY,
    oggetto_gara        VARCHAR,
    importo_lotto       DOUBLE,
    cod_cpv             VARCHAR,
    descrizione_cpv     VARCHAR,
    cf_sa               VARCHAR,
    denominazione_sa    VARCHAR,
    sezione_regionale   VARCHAR,
    tipo_scelta_contraente VARCHAR,
    data_pubblicazione  VARCHAR,
    stato               VARCHAR,
    anno                INTEGER,
    mese                INTEGER,
    flag_pnrr           VARCHAR
)
"""

_CREATE_SA = """
CREATE TABLE IF NOT EXISTS stazioni_appaltanti (
    codice_fiscale      VARCHAR PRIMARY KEY,
    denominazione       VARCHAR,
    codice_ausa         VARCHAR,
    natura_giuridica    VARCHAR,
    provincia           VARCHAR,
    citta               VARCHAR
)
"""

# ─────────────────────────────────────────────
# Data loading
# ─────────────────────────────────────────────


def _load_stazioni(conn: duckdb.DuckDBPyConnection) -> int:
    """Download and load the contracting authorities registry (~2.7 MB)."""
    url = f"{ANAC_DOWNLOAD}/stazioni-appaltanti/filesystem/stazioni-appaltanti_csv.zip"
    csv_bytes = _download_csv_from_zip(url)
    tmp = "/tmp/anac_stazioni_appaltanti.csv"
    with open(tmp, "wb") as f:
        f.write(csv_bytes)
    conn.execute("DELETE FROM stazioni_appaltanti")
    conn.execute(f"""
        INSERT OR IGNORE INTO stazioni_appaltanti
        SELECT
            codice_fiscale,
            denominazione,
            codice_ausa,
            natura_giuridica_descrizione,
            provincia_nome,
            citta_nome
        FROM read_csv('{tmp}',
            delim=';', header=true, quote='"',
            ignore_errors=true, all_varchar=true)
        WHERE codice_fiscale IS NOT NULL AND codice_fiscale != ''
    """)
    os.unlink(tmp)
    return conn.execute("SELECT COUNT(*) FROM stazioni_appaltanti").fetchone()[0]


def _load_cig_month(conn: duckdb.DuckDBPyConnection, year: int, month: int) -> int:
    """Download and load one month of CIG tender data. Returns rows inserted."""
    # Skip if already loaded
    existing = conn.execute(
        "SELECT COUNT(*) FROM cig WHERE anno = ? AND mese = ?", [year, month]
    ).fetchone()[0]
    if existing > 0:
        return existing

    url = f"{ANAC_DOWNLOAD}/cig-{year}/filesystem/cig_csv_{year}_{month:02d}.zip"
    csv_bytes = _download_csv_from_zip(url)  # raises on 404 / non-200
    tmp = f"/tmp/anac_cig_{year}_{month:02d}.csv"
    with open(tmp, "wb") as f:
        f.write(csv_bytes)

    # importo_lotto in ANAC CSVs uses period as decimal separator (verified).
    # TRY_CAST handles nulls / malformed values gracefully.
    conn.execute(f"""
        INSERT OR IGNORE INTO cig
        SELECT
            cig,
            oggetto_gara,
            TRY_CAST(importo_lotto AS DOUBLE)       AS importo_lotto,
            cod_cpv,
            descrizione_cpv,
            cf_amministrazione_appaltante           AS cf_sa,
            denominazione_amministrazione_appaltante AS denominazione_sa,
            sezione_regionale,
            tipo_scelta_contraente,
            data_pubblicazione,
            stato,
            TRY_CAST(anno_pubblicazione AS INTEGER)  AS anno,
            TRY_CAST(mese_pubblicazione AS INTEGER)  AS mese,
            FLAG_PNRR_PNC                            AS flag_pnrr
        FROM read_csv('{tmp}',
            delim=';', header=true, quote='"',
            ignore_errors=true, all_varchar=true)
        WHERE cig IS NOT NULL AND cig != ''
    """)

    try:
        os.unlink(tmp)
    except OSError:
        pass

    return conn.execute(
        "SELECT COUNT(*) FROM cig WHERE anno = ? AND mese = ?", [year, month]
    ).fetchone()[0]


def _recent_months(n: int) -> list[tuple[int, int]]:
    """Return (year, month) pairs for the last n complete published months."""
    today = date.today()
    y, m = today.year, today.month
    # Step back 1: current month's data may not be published yet
    m -= 1
    if m == 0:
        m, y = 12, y - 1
    result = []
    for _ in range(n):
        result.append((y, m))
        m -= 1
        if m == 0:
            m, y = 12, y - 1
    return result


def _background_loader() -> None:
    """
    Background thread: initialises DuckDB, loads data progressively.

    Priority order:
      1. stazioni_appaltanti (tiny — ~2.7 MB, loads in ~10s)
      2. Last 3 months of cig data (makes server usable ASAP)
      3. Extend to 12 months in background (non-blocking for tool calls)
    """
    global _db
    try:
        conn = duckdb.connect(":memory:")
        conn.execute(_CREATE_CIG)
        conn.execute(_CREATE_SA)
        with _db_lock:
            _db = conn
        print("[ANAC DB] schema initialised")

        # 1. Authorities registry (tiny, fast)
        try:
            n = _load_stazioni(conn)
            _db_status["authorities_count"] = n
            print(f"[ANAC DB] stazioni_appaltanti: {n:,} rows")
        except Exception as exc:
            print(f"[ANAC DB] stazioni_appaltanti failed: {exc}")

        # 2. Last 3 months — server becomes useful after this block
        priority = _recent_months(3)
        for year, month in priority:
            try:
                n = _load_cig_month(conn, year, month)
                key = f"{year}-{month:02d}"
                with _db_lock:
                    _db_status["months_loaded"].append(key)
                    _db_status["row_count"] = conn.execute(
                        "SELECT COUNT(*) FROM cig"
                    ).fetchone()[0]
                print(
                    f"[ANAC DB] {key}: {n:,} rows "
                    f"(total: {_db_status['row_count']:,})"
                )
            except Exception as exc:
                print(f"[ANAC DB] {year}-{month:02d} failed: {exc}")

        _db_status["state"] = "ready"
        _db_status["last_updated"] = datetime.utcnow().isoformat()
        print(
            f"[ANAC DB] ready — {_db_status['row_count']:,} contracts, "
            f"{_db_status['authorities_count']:,} authorities"
        )

        # 3. Extend to 12 months in background
        extra = _recent_months(12)[3:]
        for year, month in extra:
            key = f"{year}-{month:02d}"
            if key in _db_status["months_loaded"]:
                continue
            try:
                n = _load_cig_month(conn, year, month)
                with _db_lock:
                    _db_status["months_loaded"].append(key)
                    _db_status["row_count"] = conn.execute(
                        "SELECT COUNT(*) FROM cig"
                    ).fetchone()[0]
                print(
                    f"[ANAC DB] extended {key}: {n:,} rows "
                    f"(total: {_db_status['row_count']:,})"
                )
            except Exception as exc:
                print(f"[ANAC DB] {key} failed: {exc}")
            time.sleep(15)  # gentle on ANAC servers

    except Exception as exc:
        _db_status["state"] = "error"
        _db_status["error"] = str(exc)
        print(f"[ANAC DB] fatal: {exc}")


_loader_thread = threading.Thread(
    target=_background_loader, daemon=True, name="anac-db-loader"
)
_loader_thread.start()


# ─────────────────────────────────────────────
# Query helpers
# ─────────────────────────────────────────────


def _check_ready() -> dict | None:
    """Return an error dict if DB is not yet ready, otherwise None."""
    state = _db_status["state"]
    if state == "ready" and _db_status["row_count"] > 0:
        return None
    if state == "error":
        return {
            "status": "error",
            "message": f"Errore database ANAC: {_db_status['error']}",
        }
    loaded = _db_status["months_loaded"]
    if loaded:
        return {
            "status": "loading",
            "message": (
                f"Database parzialmente caricato: {_db_status['row_count']:,} contratti "
                f"({', '.join(sorted(loaded))}). Caricamento in corso. "
                "Riprova tra 30 secondi per dati più completi."
            ),
            "months_loaded": loaded,
            "row_count": _db_status["row_count"],
        }
    return {
        "status": "initializing",
        "message": (
            "Database ANAC in inizializzazione — primo avvio, download dati in corso "
            "(circa 2-3 minuti). Riprova tra 60 secondi."
        ),
    }


def _qry(sql: str, params: list | None = None) -> list[dict]:
    """Execute SQL on the shared DuckDB connection, return list of dicts."""
    with _db_lock:
        rel = _db.execute(sql, params or [])
        cols = [d[0] for d in rel.description]
        return [dict(zip(cols, row)) for row in rel.fetchall()]


def _fmt_eur(v: float | None) -> str:
    """Format a euro amount in Italian style: € 1.234.567"""
    if v is None or v <= 0:
        return "N/D"
    # Python formats 1234567 as "1,234,567" — convert to Italian "1.234.567"
    return "€ " + f"{v:,.0f}".replace(",", ".")


def _coverage() -> dict:
    return {
        "months_loaded": sorted(_db_status["months_loaded"]),
        "total_contracts": _db_status["row_count"],
        "authorities_in_registry": _db_status["authorities_count"],
        "last_updated": _db_status["last_updated"],
    }


# ─────────────────────────────────────────────
# Tools
# ─────────────────────────────────────────────


async def search_contracts(
    keyword: Optional[str] = None,
    cpv_prefix: Optional[str] = None,
    region: Optional[str] = None,
    tipo_procedura: Optional[str] = None,
    importo_min: Optional[float] = None,
    importo_max: Optional[float] = None,
    stato: Optional[str] = None,
    limit: int = 20,
) -> dict:
    """
    Search public procurement contracts in ANAC's BDNCP database.

    Queries tens of thousands of real contracts across the last 3–12 months.
    All parameters are optional and combinable.

    Parameters
    ----------
    keyword       : Italian search term matched against contract description.
                    E.g. 'pulizia uffici', 'servizi informatici', 'manutenzione strade'.
                    Always use Italian. Partial matches work.
    cpv_prefix    : CPV code prefix (2–8 digits). Examples:
                    '45'=Costruzione, '48'=Software, '72'=Servizi IT,
                    '79'=Servizi aziendali, '85'=Sanità, '90'=Ambiente/Pulizia.
    region        : Italian region. E.g. 'Campania', 'Lombardia', 'Sicilia', 'Lazio'.
    tipo_procedura: Procedure type partial match. E.g. 'AFFIDAMENTO DIRETTO',
                    'PROCEDURA APERTA', 'PROCEDURA NEGOZIATA'.
    importo_min   : Minimum contract value in euros.
    importo_max   : Maximum contract value in euros.
    stato         : Status filter. E.g. 'AGGIUDICATA', 'PUBBLICATA', 'ANNULLATA'.
    limit         : Max results to return (default 20, capped at 25).

    Returns contracts sorted by publication date descending with full details.
    Always includes coverage info and citation.

    ROUTING: Primary discovery tool. Use first for any "find contracts" query.
    For price analysis use benchmark_market_prices.
    For a specific CIG use get_contract_by_cig.
    """
    not_ready = _check_ready()
    if not_ready:
        return not_ready

    limit = min(max(1, limit), 25)

    wheres: list[str] = []
    params: list = []

    if keyword:
        wheres.append("lower(oggetto_gara) LIKE lower(?)")
        params.append(f"%{keyword}%")
    if cpv_prefix:
        wheres.append("cod_cpv LIKE ?")
        params.append(f"{cpv_prefix.strip()}%")
    if region:
        wheres.append("lower(sezione_regionale) LIKE lower(?)")
        params.append(f"%{region}%")
    if tipo_procedura:
        wheres.append("lower(tipo_scelta_contraente) LIKE lower(?)")
        params.append(f"%{tipo_procedura}%")
    if importo_min is not None:
        wheres.append("importo_lotto >= ?")
        params.append(importo_min)
    if importo_max is not None:
        wheres.append("importo_lotto <= ?")
        params.append(importo_max)
    if stato:
        wheres.append("lower(stato) LIKE lower(?)")
        params.append(f"%{stato}%")

    where_clause = f"WHERE {' AND '.join(wheres)}" if wheres else ""

    rows = _qry(
        f"""
        SELECT
            cig,
            LEFT(oggetto_gara, 120)         AS oggetto,
            importo_lotto,
            cod_cpv,
            LEFT(descrizione_cpv, 60)       AS descrizione_cpv,
            LEFT(denominazione_sa, 80)      AS stazione_appaltante,
            sezione_regionale               AS regione,
            tipo_scelta_contraente          AS procedura,
            data_pubblicazione,
            stato,
            flag_pnrr
        FROM cig
        {where_clause}
        ORDER BY data_pubblicazione DESC NULLS LAST
        LIMIT {limit}
        """,
        params,
    )

    total = _qry(f"SELECT COUNT(*) AS n FROM cig {where_clause}", params)[0]["n"]

    contracts = [
        {
            "cig": r["cig"],
            "oggetto": r["oggetto"],
            "importo": _fmt_eur(r["importo_lotto"]),
            "importo_raw": r["importo_lotto"],
            "cpv": r["cod_cpv"],
            "categoria_cpv": r["descrizione_cpv"],
            "stazione_appaltante": r["stazione_appaltante"],
            "regione": r["regione"],
            "procedura": r["procedura"],
            "data_pubblicazione": r["data_pubblicazione"],
            "stato": r["stato"],
            "pnrr": str(r["flag_pnrr"]).upper() in ("1", "SI", "S", "TRUE"),
            "detail_url": DETAIL_URL.format(cig=r["cig"]),
        }
        for r in rows
    ]

    tip = f"Trovati {total:,} contratti corrispondenti."
    if total > limit:
        tip += f" Mostrati i {limit} più recenti. Usa filtri per restringere la ricerca."

    return {
        "contracts": contracts,
        "returned": len(contracts),
        "total_matching": total,
        "filters_applied": {
            k: v
            for k, v in {
                "keyword": keyword,
                "cpv_prefix": cpv_prefix,
                "region": region,
                "tipo_procedura": tipo_procedura,
                "importo_min": importo_min,
                "importo_max": importo_max,
                "stato": stato,
            }.items()
            if v is not None
        },
        "coverage": _coverage(),
        "tip": tip,
        "citation": CITATION,
    }


async def get_contract_by_cig(cig: str) -> dict:
    """
    Look up a specific contract by its CIG (Codice Identificativo Gara).

    CIG is the 10-character alphanumeric identifier assigned to every Italian
    public contract (e.g. 'A05C622C05'). Case-insensitive.

    Returns full contract details: oggetto, importo, stazione appaltante with
    registry info (type, province, city), CPV, region, procedure, status.

    If the CIG is not found in the loaded months, provides a direct link to
    the ANAC portal for manual lookup.

    ROUTING: Use when the user provides a specific CIG code.
    For searching without a CIG, use search_contracts.
    """
    not_ready = _check_ready()
    if not_ready:
        return not_ready

    cig_clean = cig.strip().upper()

    rows = _qry(
        """
        SELECT
            c.cig,
            c.oggetto_gara,
            c.importo_lotto,
            c.cod_cpv,
            c.descrizione_cpv,
            c.cf_sa,
            c.denominazione_sa,
            c.sezione_regionale,
            c.tipo_scelta_contraente,
            c.data_pubblicazione,
            c.stato,
            c.flag_pnrr,
            s.natura_giuridica,
            s.provincia,
            s.citta
        FROM cig c
        LEFT JOIN stazioni_appaltanti s ON c.cf_sa = s.codice_fiscale
        WHERE upper(c.cig) = ?
        LIMIT 1
        """,
        [cig_clean],
    )

    if not rows:
        return {
            "found": False,
            "cig": cig_clean,
            "message": (
                f"CIG {cig_clean} non trovato nei dati caricati "
                f"({', '.join(sorted(_db_status['months_loaded']))}). "
                "Potrebbe appartenere a un periodo non ancora caricato o molto recente."
            ),
            "detail_url": DETAIL_URL.format(cig=cig_clean),
            "coverage": _coverage(),
        }

    r = rows[0]
    return {
        "found": True,
        "cig": r["cig"],
        "oggetto": r["oggetto_gara"],
        "importo": _fmt_eur(r["importo_lotto"]),
        "importo_raw": r["importo_lotto"],
        "cpv": r["cod_cpv"],
        "categoria_cpv": r["descrizione_cpv"],
        "stazione_appaltante": {
            "denominazione": r["denominazione_sa"],
            "codice_fiscale": r["cf_sa"],
            "natura_giuridica": r.get("natura_giuridica"),
            "provincia": r.get("provincia"),
            "citta": r.get("citta"),
        },
        "regione": r["sezione_regionale"],
        "procedura": r["tipo_scelta_contraente"],
        "data_pubblicazione": r["data_pubblicazione"],
        "stato": r["stato"],
        "pnrr": str(r["flag_pnrr"]).upper() in ("1", "SI", "S", "TRUE"),
        "detail_url": DETAIL_URL.format(cig=r["cig"]),
        "citation": CITATION,
    }


# ─────────────────────────────────────────────
# Authority-type patterns for narrowing
# ─────────────────────────────────────────────

_ENTE_PATTERNS: dict[str, list[str]] = {
    "comune": ["comune di %", "comune %"],
    "asl": ["asl %", "azienda sanitaria%", "a.s.l.%", "ausl %"],
    "provincia": ["provincia di %", "provincia %", "citta metropolitana%"],
    "regione": ["regione %", "giunta regionale%"],
    "universita": ["universita%", "università%", "politecnico%"],
    "ministero": ["ministero%", "min. %"],
    "azienda ospedaliera": ["azienda ospedaliera%", "a.o. %", "a.o.u.%", "ospedale%"],
}

# Common Italian stop words to exclude from keyword matching
_STOP_WORDS = frozenset([
    "servizi", "servizio", "fornitura", "lavori", "attivita", "attività",
    "gestione", "affidamento", "appalto", "procedura", "esecuzione",
    "relativo", "relativa", "relativi", "relative", "anno", "anni",
    "periodo", "mesi", "tramite", "mediante", "della", "delle", "dello",
    "degli", "nella", "nelle", "nello", "negli", "alla", "alle", "allo",
    "agli", "dalla", "dalle", "dallo", "dagli", "sulla", "sulle", "sullo",
    "come", "anche", "sono", "essere", "fare", "avere", "altro", "altra",
    "altri", "altre", "ogni", "tutto", "tutti", "tutta", "tutte", "tipo",
    "vari", "varie", "vario", "varia", "sede", "sedi", "lotto", "lotti",
    "numero", "numeri", "codice", "euro", "importo", "base", "gara",
    "contratto", "determina", "delibera",
])


def _extract_keywords(text: str) -> list[str]:
    """Extract meaningful Italian keywords from procurement description.

    Filters out stop words and short words to keep only content-bearing terms.
    """
    words = []
    for w in text.lower().split():
        w = w.strip(".,;:!?()[]{}\"'«»–—/\\")
        if len(w) > 3 and w not in _STOP_WORDS:
            words.append(w)
    return words


def _ente_where(tipo_ente: str) -> str | None:
    """Build SQL clause to filter by authority type."""
    key = tipo_ente.lower().strip()
    patterns = _ENTE_PATTERNS.get(key)
    if not patterns:
        # Try partial match against keys
        for k, v in _ENTE_PATTERNS.items():
            if key in k or k in key:
                patterns = v
                break
    if not patterns:
        return None
    clauses = " OR ".join([f"lower(denominazione_sa) LIKE '{p}'" for p in patterns])
    return f"({clauses})"


def _progressive_narrow(
    keywords: list[str],
    tipo_ente: str | None,
    cpv_prefix: str | None,
    region: str | None,
    importo_previsto: float | None,
    min_sample: int = 5,
    max_sample: int = 50,
) -> tuple[str, list, str, int]:
    """Try progressively broader queries until we get min_sample–max_sample results.

    Returns (where_clause, params, narrowing_description, attempt_number).
    """

    # Value band: 0.2x–5x of target amount
    value_lo = importo_previsto * 0.2 if importo_previsto else None
    value_hi = importo_previsto * 5.0 if importo_previsto else None

    ente_clause = _ente_where(tipo_ente) if tipo_ente else None

    def _base_wheres() -> tuple[list[str], list]:
        w = ["importo_lotto > 0", "importo_lotto IS NOT NULL"]
        p: list = []
        if region:
            w.append("lower(sezione_regionale) LIKE lower(?)")
            p.append(f"%{region}%")
        return w, p

    def _add_value_band(w: list[str], p: list) -> None:
        if value_lo is not None:
            w.append(f"importo_lotto >= {value_lo}")
            w.append(f"importo_lotto <= {value_hi}")

    def _add_cpv(w: list[str], p: list) -> None:
        if cpv_prefix:
            w.append("cod_cpv LIKE ?")
            p.append(f"{cpv_prefix.strip()}%")

    def _count(where_clause: str, params: list) -> int:
        r = _qry(f"SELECT COUNT(*) AS n FROM cig {where_clause}", params)
        return int(r[0]["n"]) if r else 0

    attempts = []

    # ── Attempt 1: ALL keywords AND + ente type + value band ─────────
    if keywords:
        w, p = _base_wheres()
        kw_and = " AND ".join([f"lower(oggetto_gara) LIKE ?" for _ in keywords])
        w.append(f"({kw_and})")
        p.extend([f"%{k}%" for k in keywords])
        if ente_clause:
            w.append(ente_clause)
        _add_value_band(w, p)
        _add_cpv(w, p)
        wc = "WHERE " + " AND ".join(w)
        n = _count(wc, p)
        desc = (
            f"Tutti i termini ({', '.join(keywords)})"
            + (f" + tipo ente: {tipo_ente}" if ente_clause else "")
            + (f" + fascia importo {_fmt_eur(value_lo)}–{_fmt_eur(value_hi)}" if value_lo else "")
            + (f" + CPV {cpv_prefix}" if cpv_prefix else "")
            + (f" + regione {region}" if region else "")
        )
        attempts.append((wc, p, desc, n, 1))
        if min_sample <= n <= max_sample:
            return wc, p, desc, 1

    # ── Attempt 2: ALL keywords AND + value band (drop ente type) ────
    if keywords:
        w, p = _base_wheres()
        kw_and = " AND ".join([f"lower(oggetto_gara) LIKE ?" for _ in keywords])
        w.append(f"({kw_and})")
        p.extend([f"%{k}%" for k in keywords])
        _add_value_band(w, p)
        _add_cpv(w, p)
        wc = "WHERE " + " AND ".join(w)
        n = _count(wc, p)
        desc = (
            f"Tutti i termini ({', '.join(keywords)})"
            + (f" + fascia importo {_fmt_eur(value_lo)}–{_fmt_eur(value_hi)}" if value_lo else "")
            + (f" + CPV {cpv_prefix}" if cpv_prefix else "")
            + (f" + regione {region}" if region else "")
        )
        attempts.append((wc, p, desc, n, 2))
        if min_sample <= n <= max_sample:
            return wc, p, desc, 2

    # ── Attempt 3: keyword scoring (match 2+ of N) + value band ──────
    if len(keywords) >= 2:
        w, p = _base_wheres()
        score_cases = " + ".join(
            [f"(CASE WHEN lower(oggetto_gara) LIKE '%{k}%' THEN 1 ELSE 0 END)"
             for k in keywords]
        )
        # We need a subquery / CTE for the score filter
        _add_value_band(w, p)
        _add_cpv(w, p)
        inner_where = "WHERE " + " AND ".join(w)
        # Count with score >= 2
        count_sql = f"""
            SELECT COUNT(*) AS n FROM (
                SELECT *, ({score_cases}) AS kw_score FROM cig {inner_where}
            ) WHERE kw_score >= 2
        """
        n = int(_qry(count_sql, p)[0]["n"])
        desc = (
            f"Almeno 2 termini su {len(keywords)} ({', '.join(keywords)})"
            + (f" + fascia importo" if value_lo else "")
            + (f" + CPV {cpv_prefix}" if cpv_prefix else "")
        )
        # Build actual where for later use as a CTE-based approach
        wc = f"__SCORED__|{inner_where}|{score_cases}"  # sentinel for scored query
        attempts.append((wc, p, desc, n, 3))
        if min_sample <= n <= max_sample:
            return wc, p, desc, 3

    # ── Attempt 4: ANY keyword OR + value band ───────────────────────
    if keywords:
        w, p = _base_wheres()
        kw_or = " OR ".join([f"lower(oggetto_gara) LIKE ?" for _ in keywords])
        w.append(f"({kw_or})")
        p.extend([f"%{k}%" for k in keywords])
        _add_value_band(w, p)
        _add_cpv(w, p)
        wc = "WHERE " + " AND ".join(w)
        n = _count(wc, p)
        desc = (
            f"Almeno un termine ({', '.join(keywords)})"
            + (f" + fascia importo" if value_lo else "")
            + (f" + CPV {cpv_prefix}" if cpv_prefix else "")
        )
        attempts.append((wc, p, desc, n, 4))
        if min_sample <= n <= max_sample:
            return wc, p, desc, 4

    # ── Attempt 5: CPV prefix + value band only (broadest) ───────────
    if cpv_prefix:
        w, p = _base_wheres()
        _add_cpv(w, p)
        _add_value_band(w, p)
        wc = "WHERE " + " AND ".join(w)
        n = _count(wc, p)
        desc = (
            f"Solo CPV {cpv_prefix}"
            + (f" + fascia importo" if value_lo else "")
            + " — campione ampio, interpretare con cautela"
        )
        attempts.append((wc, p, desc, n, 5))
        if min_sample <= n <= max_sample:
            return wc, p, desc, 5

    # ── Fallback: pick the best attempt we have ──────────────────────
    # Prefer the attempt closest to min_sample from above (i.e., smallest n >= 5),
    # otherwise the one with the most results
    valid = [(wc, p, d, n, a) for wc, p, d, n, a in attempts if n >= min_sample]
    if valid:
        # Pick the tightest (smallest n that's still >= min_sample)
        best = min(valid, key=lambda x: x[3])
        return best[0], best[1], best[2] + f" (n={best[3]})", best[4]

    # Nothing reached min_sample — pick whatever has the most results
    if attempts:
        best = max(attempts, key=lambda x: x[3])
        return best[0], best[1], best[2] + f" (n={best[3]}, campione insufficiente)", best[4]

    # Absolute fallback
    w, p = _base_wheres()
    _add_value_band(w, p)
    wc = "WHERE " + " AND ".join(w)
    return wc, p, "Nessun filtro applicabile — intero database", 6


def _run_stats_query(where_clause: str, params: list) -> dict:
    """Run aggregation query, handling scored-query sentinel."""
    if where_clause.startswith("__SCORED__"):
        _, inner_where, score_expr = where_clause.split("|", 2)
        sql = f"""
            SELECT
                COUNT(*) AS n,
                AVG(importo_lotto) AS media,
                MEDIAN(importo_lotto) AS mediana,
                MIN(importo_lotto) AS minimo,
                MAX(importo_lotto) AS massimo,
                quantile_cont(importo_lotto, 0.25) AS p25,
                quantile_cont(importo_lotto, 0.75) AS p75
            FROM (
                SELECT *, ({score_expr}) AS kw_score FROM cig {inner_where}
            ) WHERE kw_score >= 2
        """
    else:
        sql = f"""
            SELECT
                COUNT(*) AS n,
                AVG(importo_lotto) AS media,
                MEDIAN(importo_lotto) AS mediana,
                MIN(importo_lotto) AS minimo,
                MAX(importo_lotto) AS massimo,
                quantile_cont(importo_lotto, 0.25) AS p25,
                quantile_cont(importo_lotto, 0.75) AS p75
            FROM cig {where_clause}
        """
    rows = _qry(sql, params)
    return rows[0] if rows else {}


def _run_sample_query(
    where_clause: str,
    params: list,
    importo_previsto: float | None,
    limit: int = 5,
) -> list[dict]:
    """Fetch best example contracts, handling scored-query sentinel."""
    order = (
        f"ABS(importo_lotto - {float(importo_previsto)}) ASC"
        if importo_previsto and importo_previsto > 0
        else "data_pubblicazione DESC NULLS LAST"
    )

    if where_clause.startswith("__SCORED__"):
        _, inner_where, score_expr = where_clause.split("|", 2)
        sql = f"""
            SELECT * FROM (
                SELECT cig, LEFT(oggetto_gara, 150) AS oggetto, importo_lotto,
                       LEFT(denominazione_sa, 80) AS denominazione_sa,
                       tipo_scelta_contraente AS procedura,
                       data_pubblicazione, sezione_regionale,
                       ({score_expr}) AS kw_score
                FROM cig {inner_where}
            ) WHERE kw_score >= 2
            ORDER BY kw_score DESC, {order}
            LIMIT {limit}
        """
    else:
        sql = f"""
            SELECT cig, LEFT(oggetto_gara, 150) AS oggetto, importo_lotto,
                   LEFT(denominazione_sa, 80) AS denominazione_sa,
                   tipo_scelta_contraente AS procedura,
                   data_pubblicazione, sezione_regionale
            FROM cig {where_clause}
            ORDER BY {order}
            LIMIT {limit}
        """
    return _qry(sql, params)


def _run_procedure_breakdown(where_clause: str, params: list) -> list[dict]:
    """Get procedure type distribution for the matched sample."""
    if where_clause.startswith("__SCORED__"):
        _, inner_where, score_expr = where_clause.split("|", 2)
        sql = f"""
            SELECT tipo_scelta_contraente AS procedura, COUNT(*) AS n
            FROM (
                SELECT *, ({score_expr}) AS kw_score FROM cig {inner_where}
            ) WHERE kw_score >= 2
            AND tipo_scelta_contraente IS NOT NULL AND tipo_scelta_contraente != ''
            GROUP BY tipo_scelta_contraente ORDER BY n DESC LIMIT 6
        """
    else:
        sql = f"""
            SELECT tipo_scelta_contraente AS procedura, COUNT(*) AS n
            FROM cig {where_clause}
            AND tipo_scelta_contraente IS NOT NULL AND tipo_scelta_contraente != ''
            GROUP BY tipo_scelta_contraente ORDER BY n DESC LIMIT 6
        """
    return _qry(sql, params)


async def benchmark_market_prices(
    procurement_description: str,
    importo_previsto: Optional[float] = None,
    cpv_prefix: Optional[str] = None,
    region: Optional[str] = None,
    tipo_ente: Optional[str] = None,
) -> dict:
    """
    Analisi di mercato completa per la PA: benchmark prezzi e testo per il fascicolo.

    Questo strumento esegue un'analisi di mercato strutturata in 7 sezioni,
    conforme all'art. 14 D.Lgs. 36/2023. Usa una strategia di ricerca
    progressiva per trovare un campione ristretto di contratti comparabili
    (5–50) invece di restituire migliaia di risultati generici.

    Parameters
    ----------
    procurement_description : Cosa deve acquistare la PA, in italiano naturale.
                              Più specifico = migliore. Esempi:
                              'manutenzione sito web istituzionale',
                              'servizi pulizia uffici comunali',
                              'sviluppo software gestionale sanitario'.
    importo_previsto        : Budget previsto in euro. OBBLIGATORIO se l'utente
                              menziona un importo. Determina la fascia di
                              comparazione e la valutazione di congruità.
    cpv_prefix              : Prefisso CPV (2–8 cifre). Esempi:
                              '72'=IT, '90'=pulizia, '45'=costruzioni.
    region                  : Regione italiana (es. 'Campania', 'Lombardia').
    tipo_ente               : Tipo di stazione appaltante per restringere il
                              confronto. Valori: 'Comune', 'ASL', 'Provincia',
                              'Regione', 'Universita', 'Ministero',
                              'Azienda ospedaliera'. Se l'utente dice "per il
                              Comune" o "della ASL", passare questo parametro.

    Returns
    -------
    Risposta strutturata in 7 sezioni pronta per il fascicolo di gara:
    1. Ricognizione del mercato — criteri, periodo, qualità del campione
    2. Distribuzione dei prezzi — n, min, P25, mediana, P75, max
    3. Procedure utilizzate — distribuzione per tipo
    4. Esempi concreti — 5 contratti reali con CIG, importo, ente
    5. Valutazione di congruità — posizionamento vs quartili
    6. Rischi e cautele — avvertenze quando necessario
    7. Conclusioni operative — paragrafo per la determina

    ROUTING: Usare per QUALSIASI richiesta di analisi di mercato, congruità,
    benchmark prezzi, verifica importo, supporto affidamento.
    SEMPRE passare importo_previsto se l'utente menziona un budget.
    SEMPRE passare tipo_ente se l'utente menziona il tipo di ente.
    """
    not_ready = _check_ready()
    if not_ready:
        return not_ready

    today_str = date.today().strftime("%d/%m/%Y")
    months_str = ", ".join(sorted(_db_status["months_loaded"]))

    # ── 1. Progressive narrowing ─────────────────────────────────────
    keywords = _extract_keywords(procurement_description)
    where_clause, params, narrowing_desc, attempt_num = _progressive_narrow(
        keywords=keywords,
        tipo_ente=tipo_ente,
        cpv_prefix=cpv_prefix,
        region=region,
        importo_previsto=importo_previsto,
    )

    # ── 2. Statistics on the narrowed sample ─────────────────────────
    s = _run_stats_query(where_clause, params)
    n = int(s.get("n") or 0)

    # ── 3. Procedure breakdown ───────────────────────────────────────
    proc_rows = _run_procedure_breakdown(where_clause, params)
    proc_total = sum(r["n"] for r in proc_rows) or 1

    # ── 4. Best examples (by proximity to target amount) ─────────────
    sample_rows = _run_sample_query(where_clause, params, importo_previsto, limit=5)

    # ── 5. Quality assessment ────────────────────────────────────────
    if attempt_num <= 2 and n >= 5:
        qualita_campione = "alta"
        qualita_nota = "Campione ristretto con alta comparabilità (tutti i termini di ricerca corrispondenti)."
    elif attempt_num == 3 and n >= 5:
        qualita_campione = "buona"
        qualita_nota = "Campione con buona comparabilità (la maggior parte dei termini corrispondenti)."
    elif attempt_num == 4 and n >= 5:
        qualita_campione = "moderata"
        qualita_nota = "Campione più ampio con comparabilità moderata. Verificare manualmente la pertinenza degli esempi."
    elif attempt_num >= 5:
        qualita_campione = "bassa"
        qualita_nota = "Campione basato su corrispondenza CPV ampia. Interpretare le statistiche con cautela e integrare con altre fonti."
    elif n < 5:
        qualita_campione = "insufficiente"
        qualita_nota = f"Solo {n} contratti trovati — campione insufficiente per un'analisi statistica affidabile. Integrare con dataset storici ANAC e fonti Consip."
    else:
        qualita_campione = "moderata"
        qualita_nota = ""

    # ── 6. Congruity assessment (quartile-based) ─────────────────────
    congruita = {}
    rischi: list[str] = []
    if importo_previsto and importo_previsto > 0 and n >= 3:
        mediana = s.get("mediana")
        p25 = s.get("p25")
        p75 = s.get("p75")
        massimo = s.get("massimo")
        minimo = s.get("minimo")

        if mediana and p25 and p75:
            if importo_previsto < minimo:
                giudizio = "inferiore al minimo osservato"
                motivazione = (
                    f"L'importo previsto ({_fmt_eur(importo_previsto)}) è inferiore "
                    f"al minimo osservato nel campione ({_fmt_eur(minimo)}). "
                    f"Verificare la completezza della prestazione e la sostenibilità economica."
                )
                rischi.append("Importo sotto il minimo di mercato: rischio di prestazione incompleta o sotto-dimensionata.")
            elif importo_previsto <= p25:
                giudizio = "sotto il primo quartile — fascia bassa"
                motivazione = (
                    f"L'importo previsto ({_fmt_eur(importo_previsto)}) si colloca "
                    f"sotto il primo quartile ({_fmt_eur(p25)}). Prezzo competitivo, "
                    f"verificare che la prestazione sia completa e il prezzo sostenibile."
                )
            elif importo_previsto <= mediana:
                giudizio = "congruo — nella fascia inferiore del mercato"
                motivazione = (
                    f"L'importo previsto ({_fmt_eur(importo_previsto)}) si colloca "
                    f"tra il primo quartile ({_fmt_eur(p25)}) e la mediana ({_fmt_eur(mediana)}). "
                    f"Il prezzo è congruo rispetto al mercato rilevato."
                )
            elif importo_previsto <= p75:
                giudizio = "congruo — in linea con il mercato"
                motivazione = (
                    f"L'importo previsto ({_fmt_eur(importo_previsto)}) si colloca "
                    f"tra la mediana ({_fmt_eur(mediana)}) e il terzo quartile ({_fmt_eur(p75)}). "
                    f"Il prezzo è in linea con i valori di mercato."
                )
            elif importo_previsto <= massimo:
                giudizio = "sopra il terzo quartile — motivazione rafforzata consigliata"
                motivazione = (
                    f"L'importo previsto ({_fmt_eur(importo_previsto)}) supera il terzo "
                    f"quartile ({_fmt_eur(p75)}) ma resta entro il massimo osservato "
                    f"({_fmt_eur(massimo)}). Si raccomanda una motivazione rafforzata nella determina."
                )
                rischi.append(
                    f"Importo sopra il P75 ({_fmt_eur(p75)}): nella determina motivare "
                    f"la ragione del costo superiore alla prassi prevalente."
                )
            else:
                giudizio = "superiore al massimo osservato — richiede forte motivazione"
                motivazione = (
                    f"L'importo previsto ({_fmt_eur(importo_previsto)}) supera il massimo "
                    f"osservato nel campione ({_fmt_eur(massimo)}). "
                    f"Necessaria una motivazione dettagliata nella determina con evidenza "
                    f"di specificità del fabbisogno o fattori di costo eccezionali."
                )
                rischi.append(
                    f"Importo sopra il massimo di mercato ({_fmt_eur(massimo)}): "
                    f"elevato rischio di contestazione in sede di audit o controllo."
                )
                rischi.append(
                    "Valutare la possibilità di frazionare in lotti, "
                    "richiedere preventivi aggiuntivi, o adeguare le specifiche tecniche."
                )

            congruita = {
                "giudizio": giudizio,
                "motivazione": motivazione,
                "importo_previsto": _fmt_eur(importo_previsto),
                "posizione_vs_mediana": f"{importo_previsto / mediana:.2f}x" if mediana else "N/D",
                "posizione_vs_p25_p75": (
                    f"{_fmt_eur(p25)} – {_fmt_eur(p75)}"
                ),
            }
    elif importo_previsto and importo_previsto > 0 and n < 3:
        congruita = {
            "giudizio": "non valutabile — campione insufficiente",
            "motivazione": (
                f"Importo previsto: {_fmt_eur(importo_previsto)}. "
                f"Con sole {n} osservazioni non è possibile una valutazione "
                f"statistica affidabile. Integrare con altre fonti."
            ),
        }
        rischi.append(
            f"Campione di {n} contratti insufficiente per una valutazione statistica. "
            f"Integrare con dataset storici ANAC (https://dati.anticorruzione.it/opendata/dataset/cig) "
            f"e convenzioni Consip (https://www.acquistinretepa.it)."
        )

    # Additional risk checks
    if n > 100:
        rischi.append(
            f"Campione ampio ({n} contratti) — possibile eterogeneità. "
            f"Verificare che gli esempi siano effettivamente comparabili."
        )
    if qualita_campione == "bassa":
        rischi.append(
            "Ricerca basata su CPV ampio, non su corrispondenza testuale. "
            "Le statistiche potrebbero includere oggetti non comparabili."
        )

    # ── 7. Procedure reference ───────────────────────────────────────
    if importo_previsto and importo_previsto <= 140_000:
        proc_ref = (
            "Affidamento diretto ai sensi dell'art. 50, co. 1, lett. b), "
            "D.Lgs. 36/2023 (soglia servizi/forniture ≤ €140.000)."
        )
    elif importo_previsto and importo_previsto <= 215_000:
        proc_ref = (
            "Procedura negoziata senza bando previa consultazione di almeno "
            "cinque operatori economici, ai sensi dell'art. 50, co. 1, "
            "lett. c), D.Lgs. 36/2023."
        )
    elif importo_previsto and importo_previsto <= 5_382_000:
        proc_ref = (
            "Procedura negoziata o aperta ai sensi dell'art. 50, co. 1, "
            "lett. d), D.Lgs. 36/2023."
        )
    elif importo_previsto:
        proc_ref = (
            "Procedura di gara aperta ai sensi del D.Lgs. 36/2023 — "
            "importo sopra soglia comunitaria."
        )
    else:
        proc_ref = (
            "Procedura da determinare in base all'importo a base di gara "
            "(art. 50, D.Lgs. 36/2023)."
        )

    # ── Build 7-section analisi di mercato text ──────────────────────

    # Section 1: Ricognizione
    sez1 = (
        f"1. RICOGNIZIONE DEL MERCATO\n\n"
        f"Oggetto della ricerca: {procurement_description}\n"
        f"{'Importo previsto: ' + _fmt_eur(importo_previsto) if importo_previsto else ''}\n"
        f"Fonte: ANAC — Banca Dati Nazionale Contratti Pubblici (BDNCP)\n"
        f"Periodo campionato: {months_str}\n"
        f"{'Categoria CPV: ' + cpv_prefix if cpv_prefix else ''}\n"
        f"{'Area geografica: ' + region if region else 'Area geografica: nazionale'}\n"
        f"{'Tipo ente: ' + tipo_ente if tipo_ente else ''}\n"
        f"Criterio di selezione: {narrowing_desc}\n"
        f"Qualità del campione: {qualita_campione} — {qualita_nota}\n"
    )

    # Section 2: Distribuzione prezzi
    if n >= 3:
        sez2 = (
            f"2. DISTRIBUZIONE DEI PREZZI (n = {n})\n\n"
            f"  • Minimo:     {_fmt_eur(s.get('minimo'))}\n"
            f"  • P25:        {_fmt_eur(s.get('p25'))}\n"
            f"  • Mediana:    {_fmt_eur(s.get('mediana'))}\n"
            f"  • P75:        {_fmt_eur(s.get('p75'))}\n"
            f"  • Massimo:    {_fmt_eur(s.get('massimo'))}\n"
            f"  • Media:      {_fmt_eur(s.get('media'))} (da usare con cautela se distribuzione asimmetrica)\n"
        )
    elif n > 0:
        sez2 = (
            f"2. DISTRIBUZIONE DEI PREZZI (n = {n} — campione limitato)\n\n"
            f"  • Range: {_fmt_eur(s.get('minimo'))} – {_fmt_eur(s.get('massimo'))}\n"
            f"  • Media: {_fmt_eur(s.get('media'))}\n"
            f"  ⚠ Campione troppo piccolo per statistiche affidabili.\n"
        )
    else:
        sez2 = (
            f"2. DISTRIBUZIONE DEI PREZZI\n\n"
            f"  Nessun contratto comparabile trovato nel periodo campionato.\n"
        )

    # Section 3: Procedure utilizzate
    if proc_rows:
        proc_lines = []
        for r in proc_rows:
            pct = r["n"] / proc_total * 100
            proc_lines.append(f"  • {r['procedura']}: {r['n']} ({pct:.0f}%)")
        sez3 = f"3. PROCEDURE UTILIZZATE NELLA PRATICA\n\n" + "\n".join(proc_lines) + "\n"
    else:
        sez3 = "3. PROCEDURE UTILIZZATE\n\n  Dati non disponibili.\n"

    # Section 4: Esempi concreti
    if sample_rows:
        esempio_lines = []
        for i, r in enumerate(sample_rows, 1):
            esempio_lines.append(
                f"  {i}. CIG {r['cig']} — {_fmt_eur(r['importo_lotto'])}\n"
                f"     Oggetto: {r['oggetto']}\n"
                f"     Ente: {r['denominazione_sa']}\n"
                f"     Procedura: {r.get('procedura', 'N/D')}\n"
                f"     Data: {r.get('data_pubblicazione', 'N/D')}\n"
            )
        sez4 = f"4. ESEMPI CONCRETI\n\n" + "\n".join(esempio_lines)
    else:
        sez4 = "4. ESEMPI CONCRETI\n\n  Nessun esempio disponibile.\n"

    # Section 5: Valutazione di congruità
    if congruita:
        sez5 = (
            f"5. VALUTAZIONE DI CONGRUITÀ\n\n"
            f"  Giudizio: {congruita.get('giudizio', 'N/D')}\n"
            f"  {congruita.get('motivazione', '')}\n"
        )
    elif importo_previsto:
        sez5 = (
            f"5. VALUTAZIONE DI CONGRUITÀ\n\n"
            f"  Importo previsto: {_fmt_eur(importo_previsto)}\n"
            f"  Valutazione non disponibile per insufficienza del campione.\n"
        )
    else:
        sez5 = (
            f"5. VALUTAZIONE DI CONGRUITÀ\n\n"
            f"  Importo previsto non indicato — impossibile valutare la congruità.\n"
        )

    # Section 6: Rischi e cautele
    if rischi:
        rischi_lines = "\n".join([f"  ⚠ {r}" for r in rischi])
        sez6 = f"6. RISCHI E CAUTELE\n\n{rischi_lines}\n"
    else:
        sez6 = "6. RISCHI E CAUTELE\n\n  Nessun rischio specifico rilevato.\n"

    # Section 7: Conclusioni operative
    sez7 = (
        f"7. CONCLUSIONI OPERATIVE\n\n"
        f"La presente analisi di mercato, condotta ai sensi dell'art. 14 del D.Lgs. 36/2023, "
        f"ha preso in esame {n} contratti pubblici comparabili presenti nella BDNCP-ANAC "
        f"nel periodo {months_str}. "
    )
    if congruita.get("giudizio"):
        sez7 += f"L'importo previsto di {_fmt_eur(importo_previsto)} risulta {congruita['giudizio']}. "
    sez7 += (
        f"\n\nRiferimento procedurale: {proc_ref}\n\n"
        f"Fonti: ANAC BDNCP (CC-BY-SA 4.0), periodo {months_str}. "
        f"Si raccomanda di integrare con verifica delle convenzioni Consip attive "
        f"(www.acquistinretepa.it) e, ove disponibili, preventivi di mercato.\n"
    )

    analisi_text = (
        f"ANALISI DI MERCATO\n"
        f"(ai sensi dell'art. 14, D.Lgs. 36/2023 — Codice dei Contratti Pubblici)\n"
        f"Data: {today_str}\n\n"
        f"{sez1}\n{sez2}\n{sez3}\n{sez4}\n{sez5}\n{sez6}\n{sez7}"
    )

    return {
        "ricognizione": {
            "oggetto": procurement_description,
            "importo_previsto": _fmt_eur(importo_previsto) if importo_previsto else None,
            "criteri_selezione": narrowing_desc,
            "qualita_campione": qualita_campione,
            "nota_qualita": qualita_nota,
            "tentativo_utilizzato": attempt_num,
            "periodo": months_str,
        },
        "distribuzione_prezzi": {
            "n": n,
            "media": round(s["media"], 2) if s.get("media") else None,
            "mediana": round(s["mediana"], 2) if s.get("mediana") else None,
            "minimo": round(s["minimo"], 2) if s.get("minimo") else None,
            "massimo": round(s["massimo"], 2) if s.get("massimo") else None,
            "p25": round(s["p25"], 2) if s.get("p25") else None,
            "p75": round(s["p75"], 2) if s.get("p75") else None,
        },
        "procedure_utilizzate": [
            {
                "procedura": r["procedura"],
                "n": r["n"],
                "percentuale": round(r["n"] / proc_total * 100, 1),
            }
            for r in proc_rows
        ],
        "esempi_concreti": [
            {
                "cig": r["cig"],
                "oggetto": r["oggetto"],
                "importo": _fmt_eur(r["importo_lotto"]),
                "importo_raw": r["importo_lotto"],
                "stazione_appaltante": r["denominazione_sa"],
                "procedura": r.get("procedura"),
                "data": r.get("data_pubblicazione"),
                "regione": r.get("sezione_regionale"),
                "detail_url": DETAIL_URL.format(cig=r["cig"]),
            }
            for r in sample_rows
        ],
        "valutazione_congruita": congruita,
        "rischi_e_cautele": rischi,
        "riferimento_procedurale": proc_ref,
        "analisi_di_mercato_text": analisi_text,
        "coverage": _coverage(),
        "citation": CITATION,
    }


async def get_authority_procurement_profile(
    authority_name: Optional[str] = None,
    codice_fiscale: Optional[str] = None,
) -> dict:
    """
    Profile a contracting authority: their procurement history, volumes, and patterns.

    Parameters
    ----------
    authority_name  : Partial name match. E.g. 'Comune di Roma', 'ASL Napoli 1',
                      'Università degli Studi di Milano'. Case-insensitive.
    codice_fiscale  : Exact fiscal code of the authority (use for precision).

    Returns
    -------
    - Authority registry entry (legal type, province, city)
    - Procurement statistics: total contracts, total value, average value
    - Top CPV categories by volume
    - Procedure type breakdown
    - 10 most recent contracts

    ROUTING: Use for "profilo acquisti di [ente]", "quanto ha speso [ente]",
    "chi ha affidato di più per X", "contratti del Comune di Y".
    """
    not_ready = _check_ready()
    if not_ready:
        return not_ready

    if not authority_name and not codice_fiscale:
        return {"error": "Specifica almeno authority_name o codice_fiscale."}

    # Look up authority in registry
    if codice_fiscale:
        sa_rows = _qry(
            "SELECT * FROM stazioni_appaltanti WHERE codice_fiscale = ? LIMIT 1",
            [codice_fiscale.strip()],
        )
        contract_where = "WHERE cf_sa = ?"
        contract_params = [codice_fiscale.strip()]
    else:
        sa_rows = _qry(
            "SELECT * FROM stazioni_appaltanti "
            "WHERE lower(denominazione) LIKE lower(?) LIMIT 5",
            [f"%{authority_name}%"],
        )
        if sa_rows:
            cfs = [r["codice_fiscale"] for r in sa_rows]
            placeholders = ",".join(["?" for _ in cfs])
            contract_where = f"WHERE cf_sa IN ({placeholders})"
            contract_params = cfs
        else:
            # Fall back to name match in cig table
            contract_where = "WHERE lower(denominazione_sa) LIKE lower(?)"
            contract_params = [f"%{authority_name}%"]

    # Overall stats
    stats = _qry(
        f"""
        SELECT
            COUNT(*)                AS n_contratti,
            SUM(importo_lotto)      AS valore_totale,
            AVG(importo_lotto)      AS valore_medio,
            MIN(data_pubblicazione) AS prima_gara,
            MAX(data_pubblicazione) AS ultima_gara
        FROM cig
        {contract_where}
        AND importo_lotto > 0
        """,
        contract_params,
    )[0]

    if not stats["n_contratti"]:
        return {
            "found": False,
            "authority_name": authority_name,
            "message": (
                f"Nessun contratto trovato per '{authority_name}' "
                f"nel periodo {', '.join(sorted(_db_status['months_loaded']))}. "
                "L'ente potrebbe non aver pubblicato contratti in questo periodo."
            ),
            "registry_matches": sa_rows,
            "coverage": _coverage(),
        }

    cpv_breakdown = _qry(
        f"""
        SELECT
            cod_cpv,
            descrizione_cpv,
            COUNT(*)            AS n,
            SUM(importo_lotto)  AS totale
        FROM cig
        {contract_where}
        AND cod_cpv IS NOT NULL AND cod_cpv != ''
        GROUP BY cod_cpv, descrizione_cpv
        ORDER BY n DESC
        LIMIT 8
        """,
        contract_params,
    )

    proc_breakdown = _qry(
        f"""
        SELECT tipo_scelta_contraente AS procedura, COUNT(*) AS n
        FROM cig
        {contract_where}
        AND tipo_scelta_contraente IS NOT NULL AND tipo_scelta_contraente != ''
        GROUP BY tipo_scelta_contraente
        ORDER BY n DESC
        LIMIT 6
        """,
        contract_params,
    )

    recent = _qry(
        f"""
        SELECT
            cig,
            LEFT(oggetto_gara, 100)  AS oggetto,
            importo_lotto,
            cod_cpv,
            tipo_scelta_contraente,
            data_pubblicazione,
            stato
        FROM cig
        {contract_where}
        ORDER BY data_pubblicazione DESC NULLS LAST
        LIMIT 10
        """,
        contract_params,
    )

    sa_info = sa_rows[0] if sa_rows else {}
    return {
        "authority": {
            "denominazione": sa_info.get("denominazione") or authority_name,
            "codice_fiscale": sa_info.get("codice_fiscale"),
            "natura_giuridica": sa_info.get("natura_giuridica"),
            "provincia": sa_info.get("provincia"),
            "citta": sa_info.get("citta"),
        },
        "statistics": {
            "n_contratti": stats["n_contratti"],
            "valore_totale": _fmt_eur(stats["valore_totale"]),
            "valore_medio": _fmt_eur(stats["valore_medio"]),
            "prima_gara": stats["prima_gara"],
            "ultima_gara": stats["ultima_gara"],
        },
        "top_categorie_cpv": [
            {
                "cpv": r["cod_cpv"],
                "descrizione": r["descrizione_cpv"],
                "n_contratti": r["n"],
                "valore_totale": _fmt_eur(r["totale"]),
            }
            for r in cpv_breakdown
        ],
        "procedure_utilizzate": [
            {"procedura": r["procedura"], "n_contratti": r["n"]}
            for r in proc_breakdown
        ],
        "contratti_recenti": [
            {
                "cig": r["cig"],
                "oggetto": r["oggetto"],
                "importo": _fmt_eur(r["importo_lotto"]),
                "cpv": r["cod_cpv"],
                "procedura": r["tipo_scelta_contraente"],
                "data": r["data_pubblicazione"],
                "stato": r["stato"],
                "detail_url": DETAIL_URL.format(cig=r["cig"]),
            }
            for r in recent
        ],
        "coverage": _coverage(),
        "citation": CITATION,
    }


async def find_similar_contracts(
    procurement_description: str,
    cpv_prefix: Optional[str] = None,
    importo_riferimento: Optional[float] = None,
    region: Optional[str] = None,
    limit: int = 15,
) -> dict:
    """
    Find contracts similar to a given description for comparison or evidence-gathering.

    Useful for:
    - Building an analisi di mercato evidence base
    - Understanding how other PAs have procured similar goods/services
    - Identifying typical procedures and amounts for a type of procurement
    - Checking if your planned amount is in line with comparable contracts

    Parameters
    ----------
    procurement_description : What you want to compare (Italian text).
                              E.g. 'manutenzione software gestionale', 'servizi di vigilanza'.
    cpv_prefix              : CPV prefix to narrow the category.
    importo_riferimento     : Reference amount in euros. Results are sorted by proximity
                              to this value when provided.
    region                  : Limit to a specific Italian region.
    limit                   : Max results (default 15, capped at 25).

    ROUTING: Use when building an evidence base for analisi di mercato,
    or when the user wants to compare their procurement with peers.
    For aggregate statistics use benchmark_market_prices instead.
    """
    not_ready = _check_ready()
    if not_ready:
        return not_ready

    limit = min(max(1, limit), 25)

    wheres = ["importo_lotto > 0"]
    params: list = []

    words = [w for w in procurement_description.split() if len(w) > 3]
    if words:
        kw_clause = " OR ".join(
            ["lower(oggetto_gara) LIKE lower(?)" for _ in words]
        )
        wheres.append(f"({kw_clause})")
        params.extend([f"%{w}%" for w in words])

    if cpv_prefix:
        wheres.append("cod_cpv LIKE ?")
        params.append(f"{cpv_prefix.strip()}%")

    if region:
        wheres.append("lower(sezione_regionale) LIKE lower(?)")
        params.append(f"%{region}%")

    where_clause = "WHERE " + " AND ".join(wheres)

    order_clause = (
        f"ORDER BY ABS(importo_lotto - {float(importo_riferimento)})"
        if importo_riferimento and importo_riferimento > 0
        else "ORDER BY data_pubblicazione DESC NULLS LAST"
    )

    rows = _qry(
        f"""
        SELECT
            cig,
            LEFT(oggetto_gara, 120)     AS oggetto,
            importo_lotto,
            cod_cpv,
            LEFT(descrizione_cpv, 60)   AS categoria_cpv,
            LEFT(denominazione_sa, 80)  AS stazione_appaltante,
            sezione_regionale           AS regione,
            tipo_scelta_contraente      AS procedura,
            data_pubblicazione,
            stato
        FROM cig
        {where_clause}
        {order_clause}
        LIMIT {limit}
        """,
        params,
    )

    total = _qry(f"SELECT COUNT(*) AS n FROM cig {where_clause}", params)[0]["n"]

    return {
        "similar_contracts": [
            {
                "cig": r["cig"],
                "oggetto": r["oggetto"],
                "importo": _fmt_eur(r["importo_lotto"]),
                "importo_raw": r["importo_lotto"],
                "cpv": r["cod_cpv"],
                "categoria_cpv": r["categoria_cpv"],
                "stazione_appaltante": r["stazione_appaltante"],
                "regione": r["regione"],
                "procedura": r["procedura"],
                "data_pubblicazione": r["data_pubblicazione"],
                "stato": r["stato"],
                "detail_url": DETAIL_URL.format(cig=r["cig"]),
            }
            for r in rows
        ],
        "returned": len(rows),
        "total_matching": total,
        "search_criteria": {
            "description": procurement_description,
            "cpv_prefix": cpv_prefix,
            "importo_riferimento": importo_riferimento,
            "region": region,
        },
        "coverage": _coverage(),
        "citation": CITATION,
    }
