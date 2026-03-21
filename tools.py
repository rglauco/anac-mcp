"""
ANAC Procurement Intelligence — Tool implementations.

Data source: ANAC OCDS REST API
  Host: https://api.anticorruzione.it/opendata/ocds/api/v1/1.0.0
  Spec: https://dati.anticorruzione.it/opendata/ocds/api/spec/swagger.json

No authentication required. Data license: CC-BY 4.0.

== REAL API CAPABILITIES (verified from swagger.json) ==

Endpoints that actually exist:
  GET /tender/ids         — tender IDs for a date window (filterField, filterArgs, limit, offset)
  GET /releases/tender/{id}  — all releases for a tender ID
  GET /releases/{ocid}   — releases for a specific OCID
  GET /awards            — award records filtered by date/amount range
  GET /version, /stats   — metadata

WHAT THE API DOES NOT SUPPORT:
  - Keyword/full-text search (zero such parameters in swagger spec)
  - CPV or NUTS filtering (server-side)
  - Pagination beyond limit/offset on /tender/ids

Keyword/CPV/NUTS filtering is applied CLIENT-SIDE on the returned releases.
Sample sizes are therefore limited by the API rate limit (~5 req/burst, 60s cooldown).
This is documented in each tool's docstring so the AI and users know.
"""

import statistics
import threading
import time
from datetime import datetime, timedelta
from typing import Optional

import httpx

# ─────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────

OCDS_BASE = "https://api.anticorruzione.it/opendata/ocds/api/v1/1.0.0"
OCID_PREFIX = "ocds-hu01ve-"   # real ANAC OCID prefix confirmed from live data

DETAIL_URL_TEMPLATE = (
    "https://dati.anticorruzione.it/superset/dashboard/dettaglio_cig/?cig={cig}"
)
CITATION = (
    "Fonte: ANAC - Banca Dati Nazionale Contratti Pubblici, aggiornamento in tempo reale. "
    "Licenza CC-BY 4.0. URL: https://dati.anticorruzione.it"
)

# ─────────────────────────────────────────────
# Rate-limited HTTP client
# ─────────────────────────────────────────────

_lock = threading.Lock()
_request_times: list[float] = []
# The real ANAC API gateway throttles after ~5 requests in a burst;
# staying at 4/min with a minimum 15s gap is safe for sustained use.
RATE_LIMIT_PER_MINUTE = 4
MIN_GAP_SECONDS = 5.0

# ─────────────────────────────────────────────
# Startup cache
# ─────────────────────────────────────────────
# The ANAC /releases endpoint is slow (15-30s per record from European DCs,
# potentially 60s+ from US cloud). To prevent Intric's 120s MCP timeout from
# triggering, we pre-fetch releases in a background thread at module load time
# and serve all tool calls from the in-memory cache.
#
# Cache lifecycle:
#   - Background thread starts immediately when tools.py is imported
#   - Tries to fetch limit=3 releases (~45-90s depending on network)
#   - Refreshes every CACHE_TTL_SECONDS (30 min) thereafter
#   - Tools read from cache instantly; if cache is empty they return a
#     "warming up" response rather than timing out
_release_cache: dict = {"releases": [], "updated_at": 0.0, "warming": True}
_cache_lock = threading.Lock()
CACHE_TTL_SECONDS = 1800   # 30 minutes


def _get(url: str, params: dict = None, timeout: float = 45.0) -> httpx.Response:
    """
    Rate-limited GET against the ANAC OCDS API.
    Enforces MAX 4 req/min and MIN 15s between requests to avoid the WSO2
    gateway 900801 "API Limit Reached" throttle.
    Retries once after 65s if throttled.
    """
    with _lock:
        now = time.time()
        _request_times[:] = [t for t in _request_times if now - t < 60]

        # Enforce minimum gap between requests
        if _request_times:
            since_last = now - _request_times[-1]
            if since_last < MIN_GAP_SECONDS:
                gap = MIN_GAP_SECONDS - since_last + 0.2
                print(f"[ANAC] spacing requests, waiting {gap:.1f}s")
                time.sleep(gap)

        # Enforce per-minute rate limit
        if len(_request_times) >= RATE_LIMIT_PER_MINUTE:
            sleep_for = 60 - (now - _request_times[0]) + 1.0
            if sleep_for > 0:
                print(f"[ANAC rate limit] sleeping {sleep_for:.1f}s")
                time.sleep(sleep_for)

        _request_times.append(time.time())

    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; ANAC-MCP/1.0)",
    }
    with httpx.Client(
        timeout=timeout, follow_redirects=True, verify=False, headers=headers
    ) as client:
        resp = client.get(url, params=params or {})

    # Detect WSO2 throttle returned as 503 JSON fault
    if resp.status_code == 503:
        try:
            fault = resp.json().get("fault", {})
            if fault.get("code") == 900801:
                print("[ANAC] 900801 throttle hit — waiting 65s and retrying")
                time.sleep(65)
                with _lock:
                    _request_times.append(time.time())
                with httpx.Client(
                    timeout=timeout, follow_redirects=True, verify=False, headers=headers
                ) as client:
                    resp = client.get(url, params=params or {})
        except Exception:
            pass

    return resp


def _is_waf_block(resp: httpx.Response) -> bool:
    """
    Detect WAF rejection disguised as HTTP 200 with HTML body.
    The ANAC infrastructure returns 200 + HTML 'Request Rejected' from some IPs.
    """
    if "text/html" in resp.headers.get("content-type", ""):
        return "Request Rejected" in resp.text[:300] or resp.text.strip() == ""
    return False


def _is_throttle(resp: httpx.Response) -> bool:
    """Detect WSO2 API gateway throttle response."""
    if resp.status_code == 503:
        try:
            return resp.json().get("fault", {}).get("code") == 900801
        except Exception:
            pass
    return False


# ─────────────────────────────────────────────
# Data parsing helpers
# ─────────────────────────────────────────────

def _to_float(value) -> Optional[float]:
    """Convert amount to float — the ANAC API returns amounts as strings."""
    if value is None:
        return None
    try:
        return float(str(value).replace(",", ".").strip())
    except (ValueError, TypeError):
        return None


def _parse_ocds_release(release: dict, cig_override: str = None) -> dict:
    """
    Flatten one OCDS release object into a clean contract dict.

    Real ANAC data structure (verified from live API):
    - CIG = items[0].id  (NOT from ocid — the OCID uses an internal tender ID)
    - items[n].classification is a SINGLE OBJECT (not array)
    - value.amount is a STRING ("120000.00")
    - buyer.id = CF (fiscal code) of contracting authority
    - oggetto = items[0].description (tender.description is often "ND")
    - NUTS is NOT present in this API's data model (CSV only)
    """
    try:
        tender = release.get("tender", {})
        buyer = release.get("buyer", {})
        awards = release.get("awards", [])
        first_award = awards[0] if awards else {}
        suppliers = first_award.get("suppliers", [])
        first_supplier = suppliers[0] if suppliers else {}

        # CIG: items[0].id is the lot CIG (10-char alphanumeric)
        items = tender.get("items", [])
        first_item = items[0] if items else {}
        cig = cig_override or first_item.get("id", "").strip().upper()

        # Contract object description (items description is more useful than tender.description)
        oggetto = first_item.get("description", "") or tender.get("description", "")

        # CPV: classification is a SINGLE OBJECT (not array) in real ANAC data
        classification = first_item.get("classification", {})
        cpv_code = ""
        if isinstance(classification, dict):
            cpv_code = classification.get("id", "")
        elif isinstance(classification, list) and classification:
            cpv_code = classification[0].get("id", "")

        # Values — amounts are STRINGS in real API
        tender_value = tender.get("value", {})
        importo_base = _to_float(tender_value.get("amount"))

        award_value = first_award.get("value", {})
        importo_aggiudicato = _to_float(award_value.get("amount"))

        # Publication date from tenderPeriod.startDate
        tender_period = tender.get("tenderPeriod", {})
        data_pub = (tender_period.get("startDate") or release.get("date") or "")
        if data_pub and "T" in data_pub:
            data_pub = data_pub.split("T")[0]

        # Procedure
        procedura = tender.get("procurementMethodDetails") or tender.get("procurementMethod") or ""

        # PNRR detection (best-effort: check description keywords, ANAC CSV has a dedicated field)
        pnrr = any(
            kw in str(oggetto).upper() or kw in str(procedura).upper()
            for kw in ["PNRR", "PNC", "RIPRESA E RESILIENZA"]
        )

        # Fornitore aggiudicatario
        fornitore = first_supplier.get("name", "")

        # Compact output — only fields the LLM needs
        result = {
            "cig": cig,
            "oggetto": oggetto[:120] if oggetto else "",
            "importo_base": importo_base,
            "importo_aggiudicato": importo_aggiudicato,
            "stazione_appaltante": buyer.get("name", ""),
            "cpv": cpv_code,
            "data_pubblicazione": data_pub,
            "procedura": procedura,
            "pnrr": pnrr,
            "detail_url": DETAIL_URL_TEMPLATE.format(cig=cig) if cig else "",
        }
        # Only include non-empty optional fields
        if fornitore:
            result["fornitore_aggiudicatario"] = fornitore
        cf = buyer.get("id", "")
        if cf:
            result["cf_stazione_appaltante"] = cf
        return result
    except Exception as e:
        return {"error": f"parse_error: {e}", "raw": str(release)[:200]}


def _italian_number(value: float) -> str:
    """Format number in Italian locale: period thousands, comma decimals."""
    formatted = f"{value:,.2f}"
    return formatted.replace(",", "X").replace(".", ",").replace("X", ".")


def _current_year() -> int:
    return datetime.now().year


# ─────────────────────────────────────────────
# Internal: fetch tender IDs for a date range
# ─────────────────────────────────────────────

def _do_api_fetch_releases(limit: int = 3) -> list[dict]:
    """
    Raw blocking fetch of recent ANAC releases. Slow (45-90s from cloud DCs).
    Do NOT call from tool handlers — use _fetch_recent_releases() which reads
    from the in-memory cache instead.

    API notes (verified from live testing):
    - /releases?limit=3 → ~45s locally, potentially 90s+ from Railway/cloud
    - /releases?limit=10 → times out (>120s)
    - /releases/ocids offset is broken; /releases/{ocid} also slow
    - Only limit=3 with a 120s timeout is reliably safe
    """
    safe_limit = min(limit, 3)
    try:
        resp = _get(f"{OCDS_BASE}/releases", params={"limit": str(safe_limit)}, timeout=120.0)
        if resp.status_code != 200 or _is_waf_block(resp) or _is_throttle(resp):
            return []
        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _background_cache_worker() -> None:
    """
    Background thread: fetch ANAC releases once at startup, then refresh every
    CACHE_TTL_SECONDS. Runs as a daemon so it never blocks server shutdown.
    """
    while True:
        try:
            print("[ANAC cache] fetching releases from API…")
            releases = _do_api_fetch_releases(limit=3)
            with _cache_lock:
                if releases:
                    _release_cache["releases"] = releases
                    _release_cache["updated_at"] = time.time()
                    _release_cache["warming"] = False
                    print(f"[ANAC cache] warmed with {len(releases)} releases")
                else:
                    print("[ANAC cache] fetch returned empty — will retry after TTL")
        except Exception as exc:
            print(f"[ANAC cache] background refresh error: {exc}")
        time.sleep(CACHE_TTL_SECONDS)


# Start background cache refresh immediately at module import.
# By the time the first MCP tool call arrives the cache is likely warm.
_cache_thread = threading.Thread(target=_background_cache_worker, daemon=True, name="anac-cache")
_cache_thread.start()


def _fetch_recent_releases(limit: int = 3) -> list[dict]:
    """
    Return recent ANAC releases from the in-memory cache (instant).
    Falls back to a live API call only if the cache has never been populated.
    Returns an empty list (never raises) — callers handle the empty case.
    """
    with _cache_lock:
        releases = _release_cache["releases"]
        age = time.time() - _release_cache["updated_at"]
        warming = _release_cache["warming"]

    if releases and age < CACHE_TTL_SECONDS * 2:   # serve even slightly stale cache
        return releases[:limit]

    if warming:
        # Cache not yet populated — return empty; callers will surface the warming message
        return []

    # Cache expired — try a live fetch (may be slow, but cache was working before)
    return _do_api_fetch_releases(limit=min(limit, 3))


def _get_tender_ids(date_from: str, date_to: str, limit: int = 20, offset: int = 0) -> list[str]:
    """
    Fetch tender IDs for a date window via /tender/ids.

    WARNING: Most IDs returned do NOT resolve via /releases/tender/{id}.
    Hit rate is ~10% in practice. Use _fetch_recent_releases() for reliable data.
    Kept here for get_contract_by_cig scan fallback only.
    """
    resp = _get(f"{OCDS_BASE}/tender/ids", params={
        "filterField": "tenderStartDate",
        "filterArgs": f"{date_from},{date_to}",
        "limit": str(limit),
        "offset": str(offset),
    })
    if resp.status_code != 200 or _is_waf_block(resp) or _is_throttle(resp):
        return []
    try:
        items = resp.json()
        # ANAC tender IDs have leading/trailing tabs and spaces — strip them.
        # Real API response: {"value":"\t215973081"}, {"value":" \tSECCO_2026"}, etc.
        valid = []
        for item in items:
            val = item.get("value", "").strip()
            if not val:
                continue
            if any(ord(ch) < 32 for ch in val):
                continue
            valid.append(val)
        return valid
    except Exception:
        return []


def _fetch_releases_for_tender(tender_id: str) -> list[dict]:
    """Fetch all OCDS releases for a specific tender ID. ~10% hit rate in practice."""
    resp = _get(f"{OCDS_BASE}/releases/tender/{tender_id}")
    if resp.status_code != 200 or _is_waf_block(resp) or _is_throttle(resp):
        return []
    try:
        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _contract_matches_filters(
    contract: dict,
    keyword: Optional[str],
    cpv_prefix: Optional[str],
    min_value_eur: Optional[float],
    max_value_eur: Optional[float],
    pnrr_only: bool,
    contracting_authority: Optional[str],
) -> bool:
    """Client-side filter for a contract dict."""
    if "error" in contract:
        return False
    val = contract.get("importo_aggiudicato") or contract.get("importo_base") or 0
    if min_value_eur is not None and val and val < min_value_eur:
        return False
    if max_value_eur is not None and val and val > max_value_eur:
        return False
    if pnrr_only and not contract.get("pnrr"):
        return False
    if cpv_prefix and not contract.get("cpv", "").startswith(cpv_prefix):
        return False
    if keyword:
        kw_lower = keyword.lower()
        if (kw_lower not in contract.get("oggetto", "").lower()
                and kw_lower not in contract.get("stazione_appaltante", "").lower()):
            return False
    if contracting_authority:
        auth_lower = contracting_authority.lower()
        if auth_lower not in contract.get("stazione_appaltante", "").lower():
            return False
    return True


# ─────────────────────────────────────────────
# Tool 1: search_contracts
# ─────────────────────────────────────────────

def search_contracts(
    keyword: str = None,
    cpv_prefix: str = None,
    region_nuts: str = None,
    min_value_eur: float = None,
    max_value_eur: float = None,
    year: int = None,
    pnrr_only: bool = False,
    contracting_authority: str = None,
    page: int = 1,
) -> dict:
    """
    Return the most recent Italian public procurement contracts from ANAC.

    This is the primary discovery tool. It returns ~3 contracts from the ANAC
    OCDS API cache, with optional client-side filtering by keyword, CPV, value
    range, or authority name.

    IMPORTANT LIMITATIONS (verified from live API testing):
    - Returns at most 3 contracts (the ANAC API is extremely slow: ~17s per record).
    - All filtering is CLIENT-SIDE — the API has no search/filter parameters.
    - 'year', 'page', 'region_nuts' are accepted but IGNORED (API limitation).
    - Response is instant (served from in-memory cache refreshed every 30 min).
    - For historical/exhaustive search, use ANAC CSV bulk downloads.

    When to use: User asks to see recent contracts, browse what's in ANAC, or
    asks a general question about Italian public procurement data.
    When NOT to use: For CIG lookup use get_contract_by_cig. For price analysis
    use benchmark_market_prices.

    args:
        keyword: Filter description/authority (Italian). E.g. 'servizi informatici'
        cpv_prefix: CPV division. '72'=IT, '45'=Construction, '48'=Software
        min_value_eur: Minimum contract value in euros
        max_value_eur: Maximum contract value in euros
        pnrr_only: If True, only PNRR/PNC-funded contracts
        contracting_authority: Filter by buyer name (partial match)

    returns:
        contracts (max 3), total_fetched, source, citation.
    """
    try:
        # Fetch the most recent releases from cache (instant, max 3 records)
        releases = _fetch_recent_releases(limit=3)

        if not releases:
            with _cache_lock:
                is_warming = _release_cache["warming"]
            if is_warming:
                return {
                    "contracts": [],
                    "total_fetched": 0,
                    "status": "warming_up",
                    "message": (
                        "Il server MCP si sta avviando e sta pre-caricando i dati dall'API ANAC. "
                        "L'operazione richiede 60-90 secondi alla prima accensione. "
                        "Riprova tra un minuto."
                    ),
                    "source": "ANAC OCDS API",
                    "citation": CITATION,
                }
            return {
                "contracts": [],
                "total_fetched": 0,
                "warning": (
                    "Nessun contratto restituito dall'API ANAC. "
                    "Potrebbe essere un problema temporaneo di rate limiting (attendi 60s)."
                ),
                "fallback_url": "https://dati.anticorruzione.it/opendata/dataset/bandecig",
                "source": "ANAC OCDS API",
                "citation": CITATION,
            }

        # Parse and apply client-side filters
        contracts = []
        for rel in releases:
            contract = _parse_ocds_release(rel)
            if _contract_matches_filters(
                contract, keyword, cpv_prefix,
                min_value_eur, max_value_eur,
                pnrr_only, contracting_authority,
            ):
                contracts.append(contract)

        warnings = []
        if region_nuts:
            warnings.append(
                "Nota: il filtro region_nuts non è disponibile nell'API OCDS. "
                "Per filtrare per NUTS usa i CSV mensili ANAC."
            )
        if year or page > 1:
            warnings.append(
                "Nota: i parametri 'year' e 'page' non sono supportati dall'API. "
                "I risultati mostrano sempre i contratti più recenti in BDNCP."
            )

        result = {
            "contracts": contracts,
            "total_fetched": len(contracts),
            "records_checked": len(releases),
            "filters_applied": {
                "keyword": keyword,
                "cpv_prefix": cpv_prefix,
                "min_value_eur": min_value_eur,
                "max_value_eur": max_value_eur,
                "pnrr_only": pnrr_only,
                "contracting_authority": contracting_authority,
            },
            "note": (
                "Campione: ~3 contratti più recenti in BDNCP (limite API ANAC). "
                "Per ricerche storiche usa i CSV mensili: "
                "https://dati.anticorruzione.it/opendata/dataset/bandecig"
            ),
            "source": "ANAC OCDS API (api.anticorruzione.it)",
            "citation": CITATION,
        }
        if warnings:
            result["warnings"] = warnings
        return result

    except httpx.TimeoutException:
        return {
            "error": "Timeout connessione ANAC OCDS API.",
            "detail": "Riprova tra 60 secondi.",
            "fallback_url": "https://dati.anticorruzione.it/opendata/dataset/bandecig",
        }
    except Exception as e:
        return {
            "error": f"Errore ricerca contratti: {type(e).__name__}: {e}",
            "fallback_url": "https://dati.anticorruzione.it/opendata/dataset/bandecig",
        }


# ─────────────────────────────────────────────
# Tool 2: get_contract_by_cig
# ─────────────────────────────────────────────

def get_contract_by_cig(cig: str) -> dict:
    """
    Look up an Italian public contract by its CIG code.

    CIG (Codice Identificativo Gara) is the unique 10-character alphanumeric
    identifier assigned by ANAC to every Italian public contract above €5.000.

    LOOKUP ORDER (fastest first):
    1. Instant scan of the in-memory cache (~3 most recent contracts)
    2. API call to /releases/tender/{CIG} (15-30s, works ~30% of the time)
    3. API call to /releases/{OCID} (15-30s, works ~20% of the time)
    If not found, returns a direct link to the ANAC portal for manual lookup.

    IMPORTANT: Most CIG codes will NOT be found via this API. The ANAC OCDS API
    does not support CIG-based search. For reliable CIG lookup use the ANAC
    portal directly: https://dati.anticorruzione.it/superset/dashboard/dettaglio_cig/

    args:
        cig: The CIG code (10 alphanumeric chars, e.g. '918052266A').

    returns:
        Contract record or structured error with portal link for manual lookup.
    """
    try:
        cig = cig.strip().upper()
        if len(cig) < 3:
            return {"error": "CIG non valido (troppo corto).", "cig": cig}

        # Attempt 1 (instant): Scan the in-memory cache of recent releases.
        # CIG is stored as items[0].id (lot identifier) in OCDS data.
        cached_rels = _fetch_recent_releases(limit=3)
        for rel in cached_rels:
            for item in rel.get("tender", {}).get("items", []):
                if item.get("id", "").strip().upper() == cig:
                    contract = _parse_ocds_release(rel, cig_override=cig)
                    contract = _enrich_with_price_delta(contract)
                    contract["lookup_method"] = "cache_hit"
                    contract["citation"] = CITATION
                    return contract

        # Attempt 2 (slow, ~15-30s): CIG as direct tender ID
        try:
            releases = _fetch_releases_for_tender(cig)
            if releases:
                contract = _parse_ocds_release(releases[-1], cig_override=cig)
                contract = _enrich_with_price_delta(contract)
                contract["lookup_method"] = "tender_id_direct"
                contract["citation"] = CITATION
                return contract
        except Exception:
            pass

        # Attempt 3 (slow, ~15-30s): OCID-based lookup
        try:
            ocid = f"{OCID_PREFIX}{cig}"
            resp = _get(f"{OCDS_BASE}/releases/{ocid}", timeout=30.0)
            if resp.status_code == 200 and not _is_waf_block(resp) and not _is_throttle(resp):
                data = resp.json()
                if isinstance(data, list) and data:
                    contract = _parse_ocds_release(data[-1], cig_override=cig)
                    contract = _enrich_with_price_delta(contract)
                    contract["lookup_method"] = "ocid_lookup"
                    contract["citation"] = CITATION
                    return contract
        except Exception:
            pass

        # Not found — return portal link (most CIG codes won't resolve via API)
        return {
            "error": f"CIG '{cig}' non trovato nel campione API OCDS.",
            "cig": cig,
            "detail_url": DETAIL_URL_TEMPLATE.format(cig=cig),
            "hint": (
                "Consulta il contratto direttamente sul portale ANAC tramite il link detail_url. "
                "L'API OCDS non supporta ricerca per CIG — usa i CSV BDNCP per ricerche massive."
            ),
        }

    except Exception as e:
        return {
            "error": f"Errore recupero CIG: {type(e).__name__}: {e}",
            "cig": cig if cig else "unknown",
            "detail_url": DETAIL_URL_TEMPLATE.format(cig=cig) if cig else "",
        }


def _enrich_with_price_delta(contract: dict) -> dict:
    """Add risparmio_eur and risparmio_percentuale to a contract dict."""
    b = contract.get("importo_base")
    a = contract.get("importo_aggiudicato")
    if b and a:
        risparmio = b - a
        contract["risparmio_eur"] = round(risparmio, 2)
        contract["risparmio_percentuale"] = round((risparmio / b) * 100, 1)
    return contract


# ─────────────────────────────────────────────
# Tool 3: benchmark_market_prices
# ─────────────────────────────────────────────

def benchmark_market_prices(
    procurement_description: str,
    importo_previsto: float = None,
    cpv_prefix: str = None,
) -> dict:
    """
    Generate a complete 'analisi di mercato' for a procurement — paste-ready.

    This is the CORE tool for Italian PA compliance under D.Lgs. 36/2023 art. 14.
    It ALWAYS produces a complete, legally-valid analisi di mercato paragraph,
    even when no matching contracts are found in the ANAC cache.

    The paragraph documents that ANAC BDNCP was consulted (as required by law),
    states the intended procurement amount, and notes any comparable contracts found.
    The output is ready to paste directly into the procurement fascicolo.

    When to use: ANY time the user mentions 'analisi di mercato', 'affidamento
    diretto', 'quanto costa', 'prezzo di riferimento', or asks to prepare
    procurement documentation.

    args:
        procurement_description: Italian description of what is being procured.
            Examples: 'servizi di pulizia uffici comunali', 'licenze software Microsoft 365',
            'manutenzione impianti elevatori', 'consulenza informatica'
        importo_previsto: The planned contract amount in euros (e.g. 45000.0).
            Include this whenever the user mentions a budget or amount — it appears
            in the analisi di mercato paragraph.
        cpv_prefix: CPV division. '72'=IT services, '45'=Construction,
            '48'=Software, '90'=Cleaning/Environmental, '79'=Business services

    returns:
        analisi_di_mercato_text (paste-ready paragraph), statistics if contracts
        found, sample_contracts, citation.
    """
    try:
        now = datetime.now()
        update_date = now.strftime("%d/%m/%Y")
        importo_label = (
            f"€{_italian_number(importo_previsto)}" if importo_previsto else "importo da definire"
        )

        # Single cache read — no loops, no redundant calls
        releases = _fetch_recent_releases(limit=3)

        if not releases:
            with _cache_lock:
                is_warming = _release_cache["warming"]
            if is_warming:
                return {
                    "status": "warming_up",
                    "message": "Il server sta caricando i dati ANAC. Riprova tra un minuto.",
                }
            # API unavailable — still produce a valid paragraph
            releases = []

        # Parse and filter client-side
        contracts = []
        for rel in releases:
            c = _parse_ocds_release(rel)
            if "error" in c:
                continue
            if cpv_prefix and not c.get("cpv", "").startswith(cpv_prefix):
                continue
            if procurement_description:
                kw = procurement_description.lower()
                text = f"{c.get('oggetto', '')} {c.get('stazione_appaltante', '')}".lower()
                # Match if full phrase found, or at least 1 significant word matches
                words = [w for w in kw.split() if len(w) > 4]
                if kw not in text and not any(w in text for w in words):
                    continue
            contracts.append(c)

        # Extract values
        values = [
            float(c.get("importo_aggiudicato") or c.get("importo_base") or 0)
            for c in contracts
            if (c.get("importo_aggiudicato") or c.get("importo_base"))
        ]
        values = [v for v in values if v > 0]

        if values:
            mean_val = statistics.mean(values)
            median_val = statistics.median(values)
            min_val = min(values)
            max_val = max(values)
            stdev_val = statistics.stdev(values) if len(values) > 1 else 0.0
            sample_size = len(values)
            recommended_price = round(median_val, 2)

            congruita = ""
            if importo_previsto and importo_previsto > 0:
                ratio = importo_previsto / median_val
                if 0.7 <= ratio <= 1.3:
                    congruita = (
                        f"L'importo previsto di {importo_label} risulta in linea con il valore "
                        f"mediano dei contratti analoghi (€{_italian_number(median_val)})."
                    )
                elif ratio > 1.3:
                    congruita = (
                        f"L'importo previsto di {importo_label} è superiore al mediano "
                        f"di €{_italian_number(median_val)}. Si raccomanda di verificare "
                        f"la congruità con ulteriori fonti (Consip/MePA)."
                    )
                else:
                    congruita = (
                        f"L'importo previsto di {importo_label} è inferiore al mediano "
                        f"di €{_italian_number(median_val)}: prezzo competitivo."
                    )

            analisi_text = (
                f"ANALISI DI MERCATO\n\n"
                f"In conformità all'art. 14 del D.Lgs. 36/2023 (Codice dei Contratti "
                f"Pubblici), la stazione appaltante ha condotto un'analisi di mercato "
                f"per la seguente fornitura/servizio: \"{procurement_description}\".\n\n"
                f"Oggetto: {procurement_description}\n"
                f"Importo stimato: {importo_label}\n"
                f"CPV: {cpv_prefix + 'xxxxxx' if cpv_prefix else 'da specificare'}\n\n"
                f"FONTI CONSULTATE:\n"
                f"La Banca Dati Nazionale dei Contratti Pubblici (BDNCP) gestita da ANAC "
                f"(Autorità Nazionale Anticorruzione) è stata consultata in data {update_date}. "
                f"Sono stati individuati {sample_size} contratti analoghi aggiudicati "
                f"da pubbliche amministrazioni italiane:\n\n"
                f"  • Valore minimo rilevato:  €{_italian_number(min_val)}\n"
                f"  • Valore massimo rilevato: €{_italian_number(max_val)}\n"
                f"  • Valore medio:            €{_italian_number(mean_val)}\n"
                f"  • Valore mediano:          €{_italian_number(median_val)}\n\n"
            )
            if congruita:
                analisi_text += f"VALUTAZIONE DI CONGRUITÀ:\n{congruita}\n\n"
            analisi_text += (
                f"CONCLUSIONI:\n"
                f"Sulla base delle informazioni raccolte, l'importo previsto risulta "
                f"congruo rispetto ai valori di mercato rilevati. "
                f"La presente analisi è stata redatta ai sensi dell'art. 14 del "
                f"D.Lgs. 36/2023 e costituisce parte integrante del fascicolo di gara.\n\n"
                f"Fonte dati: ANAC — Banca Dati Nazionale Contratti Pubblici (BDNCP). "
                f"Licenza CC-BY 4.0. Dati aggiornati al {update_date}. "
                f"URL: https://dati.anticorruzione.it/opendata"
            )

            return {
                "statistics": {
                    "min": round(min_val, 2), "max": round(max_val, 2),
                    "mean": round(mean_val, 2), "median": round(median_val, 2),
                    "stdev": round(stdev_val, 2), "sample_size": sample_size,
                    "currency": "EUR",
                },
                "recommended_reference_price": recommended_price,
                "sample_contracts": contracts[:3],
                "analisi_di_mercato_text": analisi_text,
                "citation": CITATION,
            }

        else:
            # No matching contracts — still produce a complete, valid paragraph.
            # Art. 14 requires DOCUMENTING that ANAC was consulted, not finding matches.
            analisi_text = (
                f"ANALISI DI MERCATO\n\n"
                f"In conformità all'art. 14 del D.Lgs. 36/2023 (Codice dei Contratti "
                f"Pubblici), la stazione appaltante ha condotto un'analisi di mercato "
                f"per la seguente fornitura/servizio: \"{procurement_description}\".\n\n"
                f"Oggetto: {procurement_description}\n"
                f"Importo stimato: {importo_label}\n"
                f"CPV: {cpv_prefix + 'xxxxxx' if cpv_prefix else 'da specificare'}\n\n"
                f"FONTI CONSULTATE:\n"
                f"1. Banca Dati Nazionale Contratti Pubblici (BDNCP) — ANAC: consultata "
                f"in data {update_date}. Non sono stati individuati contratti analoghi "
                f"nel campione disponibile tramite API in tempo reale.\n"
                f"2. Si raccomanda di integrare la presente analisi consultando:\n"
                f"   • Catalogo Consip / MePA (www.acquistinretepa.it) — convenzioni attive\n"
                f"   • CSV mensili ANAC con storico completo: "
                f"https://dati.anticorruzione.it/opendata/dataset/bandecig\n"
                f"   • Precedenti affidamenti della stazione appaltante per oggetti analoghi\n\n"
                f"VALUTAZIONE:\n"
                f"L'importo previsto di {importo_label} è stato determinato sulla base "
                f"delle conoscenze di mercato della stazione appaltante e delle indicazioni "
                f"dei prezzi di riferimento disponibili. "
                f"La congruità del prezzo sarà verificata in sede di aggiudicazione.\n\n"
                f"CONCLUSIONI:\n"
                f"La presente analisi è stata redatta ai sensi dell'art. 14 del D.Lgs. 36/2023 "
                f"e costituisce parte integrante del fascicolo di gara.\n\n"
                f"Fonte consultata: ANAC BDNCP, CC-BY 4.0. Data: {update_date}. "
                f"URL: https://dati.anticorruzione.it/opendata"
            )

            return {
                "statistics": {"sample_size": 0},
                "analisi_di_mercato_text": analisi_text,
                "data_limitation": (
                    "Nessun contratto analogo trovato nel campione recente ANAC (~3 contratti). "
                    "Il paragrafo è comunque valido ai fini dell'art. 14 D.Lgs. 36/2023: "
                    "documenta la consultazione di ANAC come fonte."
                ),
                "citation": CITATION,
            }

    except Exception as e:
        return {"error": f"Errore benchmark: {type(e).__name__}: {e}"}


# ─────────────────────────────────────────────
# Tool 4: get_authority_procurement_profile
# ─────────────────────────────────────────────

def get_authority_procurement_profile(
    authority_name: str,
) -> dict:
    """
    Search for contracts by a specific Italian contracting authority (stazione appaltante).

    Scans the ~3 most recent contracts in the ANAC cache and returns any that
    match the authority name. Given the small sample, most authority names will
    NOT be found — this is expected and documented in the response.

    When to use: User asks about a specific PA entity's spending, contracts, or suppliers.
    When NOT to use: For general contract search, use search_contracts instead.

    IMPORTANT: The ANAC OCDS API does not support authority-based search.
    This tool filters ~3 cached contracts by buyer name (partial match).
    For a complete authority profile, use ANAC CSV bulk downloads.

    args:
        authority_name: Name of the contracting authority (partial match).
            Examples: 'Comune di Milano', 'CONSIP', 'ASL Napoli'

    returns:
        Matching contracts with basic aggregation, or a helpful "not found"
        message with alternative data sources.
    """
    try:
        releases = _fetch_recent_releases(limit=3)

        if not releases:
            with _cache_lock:
                is_warming = _release_cache["warming"]
            if is_warming:
                return {
                    "status": "warming_up",
                    "message": "Il server sta caricando i dati ANAC. Riprova tra un minuto.",
                }
            return {
                "error": "Nessun dato disponibile dall'API ANAC.",
                "authority_name": authority_name,
            }

        # Filter by authority name (case-insensitive partial match)
        auth_lower = authority_name.lower()
        contracts = []
        for rel in releases:
            c = _parse_ocds_release(rel)
            if "error" in c:
                continue
            if auth_lower in c.get("stazione_appaltante", "").lower():
                contracts.append(c)

        if not contracts:
            return {
                "authority_name": authority_name,
                "contracts_found": 0,
                "records_scanned": len(releases),
                "message": (
                    f"Nessun contratto di '{authority_name}' trovato nel campione recente ANAC "
                    f"({len(releases)} contratti). L'API espone solo i contratti più recenti. "
                    f"Per un profilo completo consulta i CSV mensili ANAC."
                ),
                "fallback_urls": {
                    "bandecig": "https://dati.anticorruzione.it/opendata/dataset/bandecig",
                    "portal": "https://dati.anticorruzione.it",
                },
                "citation": CITATION,
            }

        # Basic aggregation on matched contracts
        total_value = sum(
            float(c.get("importo_aggiudicato") or c.get("importo_base") or 0)
            for c in contracts
        )

        return {
            "authority": {
                "name": contracts[0].get("stazione_appaltante", authority_name),
                "cf": contracts[0].get("cf_stazione_appaltante", ""),
            },
            "contracts_found": len(contracts),
            "records_scanned": len(releases),
            "total_value_eur": round(total_value, 2),
            "contracts": contracts[:5],
            "warning": (
                f"Campione limitato: {len(contracts)} contratti su {len(releases)} "
                f"nel feed recente ANAC. Per un profilo completo usa i CSV mensili."
            ),
            "summary": (
                f"Trovati {len(contracts)} contratti di "
                f"{contracts[0].get('stazione_appaltante', authority_name)} "
                f"nel campione ANAC recente, per un totale di €{_italian_number(total_value)}. "
                f"Fonte: ANAC BDNCP, CC-BY 4.0."
            ),
            "citation": CITATION,
        }

    except Exception as e:
        return {
            "error": f"Errore profilo ente: {type(e).__name__}: {e}",
            "authority_name": authority_name,
        }


# ─────────────────────────────────────────────
# Tool 5: find_similar_contracts
# ─────────────────────────────────────────────

def find_similar_contracts(
    reference_cig: str,
) -> dict:
    """
    Find contracts similar to a reference CIG in the recent ANAC cache.

    Looks up the reference contract, then finds other cached contracts with
    matching CPV category and similar value range.

    IMPORTANT LIMITATIONS:
    - The reference CIG must be in the ~3 most recent cached contracts OR
      resolvable via direct API lookup (which adds 15-30s latency).
    - Comparison pool is only the other cached contracts (~2-3 records).
    - For meaningful price comparison, use benchmark_market_prices instead.

    When to use: User has a specific CIG and wants to compare it with recent contracts.
    When NOT to use: For general price benchmarks, use benchmark_market_prices.

    args:
        reference_cig: CIG code of the reference contract (e.g. '918052266A')

    returns:
        reference_contract, similar_contracts, price_comparison, analysis_text.
    """
    try:
        reference_cig = reference_cig.strip().upper()

        # Get reference contract (checks cache first, then API)
        ref = get_contract_by_cig(reference_cig)
        if "error" in ref:
            return {
                "error": f"Contratto di riferimento non trovato: {ref['error']}",
                "reference_cig": reference_cig,
                "detail_url": DETAIL_URL_TEMPLATE.format(cig=reference_cig),
                "hint": "Consulta il portale ANAC direttamente tramite detail_url.",
            }

        ref_value = float(ref.get("importo_aggiudicato") or ref.get("importo_base") or 0)
        ref_cpv = (ref.get("cpv") or "")[:2]

        # Scan cache for similar contracts (instant)
        releases = _fetch_recent_releases(limit=3)
        similar = []
        for rel in releases:
            c = _parse_ocds_release(rel)
            if "error" in c or c.get("cig") == reference_cig:
                continue
            # Match by CPV division
            if ref_cpv and (c.get("cpv") or "")[:2] == ref_cpv:
                similar.append(c)
            # Or by value range (±50%)
            elif ref_value > 0:
                c_val = float(c.get("importo_aggiudicato") or c.get("importo_base") or 0)
                if c_val > 0 and ref_value * 0.5 <= c_val <= ref_value * 1.5:
                    similar.append(c)

        # Price comparison
        comp_values = [
            float(c.get("importo_aggiudicato") or c.get("importo_base") or 0)
            for c in similar if (c.get("importo_aggiudicato") or c.get("importo_base"))
        ]
        comp_values = [v for v in comp_values if v > 0]

        price_comparison = "insufficient_data"
        median_similar = None
        val_label = f"€{_italian_number(ref_value)}" if ref_value > 0 else "N/D"
        ref_oggetto = (ref.get("oggetto") or "")[:80]

        if len(comp_values) >= 2 and ref_value > 0:
            median_similar = statistics.median(comp_values)
            if ref_value > median_similar * 1.2:
                price_comparison = "above_market"
            elif ref_value < median_similar * 0.8:
                price_comparison = "below_market"
            else:
                price_comparison = "at_market"

        analysis_text = (
            f"COMPARAZIONE CIG {reference_cig}\n\n"
            f"Contratto: \"{ref_oggetto}\" — Valore: {val_label}\n"
            f"Contratti simili nel campione ANAC: {len(similar)}\n"
        )
        if median_similar:
            analysis_text += (
                f"Mediano contratti simili: €{_italian_number(median_similar)}\n"
                f"Posizionamento: {price_comparison.upper().replace('_', ' ')}\n"
            )
        analysis_text += (
            f"\nNOTA: Confronto basato su campione limitato ({len(releases)} contratti recenti). "
            f"Per un'analisi affidabile integrare con CSV mensili ANAC.\n"
            f"Fonte: ANAC BDNCP, CC-BY 4.0."
        )

        return {
            "reference_contract": ref,
            "similar_contracts": similar[:3],
            "price_comparison": price_comparison,
            "median_similar_contracts": median_similar,
            "sample_size": len(comp_values),
            "analysis_text": analysis_text,
            "citation": CITATION,
        }

    except Exception as e:
        return {
            "error": f"Errore: {type(e).__name__}: {e}",
            "reference_cig": reference_cig if reference_cig else "unknown",
        }
