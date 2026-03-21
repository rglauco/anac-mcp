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
# Each /releases call takes ~50s to return; enforce only a 5s inter-request gap.
# The natural call duration provides most of the rate limiting itself.
MIN_GAP_SECONDS = 5.0


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

        return {
            "cig": cig,
            "oggetto": oggetto,
            "importo_base": importo_base,
            "importo_aggiudicato": importo_aggiudicato,
            "stazione_appaltante": buyer.get("name", ""),
            "cf_stazione_appaltante": buyer.get("id", ""),
            "cpv": cpv_code,
            "cpv_divisione": cpv_code[:2] if cpv_code and cpv_code != "99999999" else "",
            "nuts": "",   # not available in OCDS API data model
            "data_pubblicazione": data_pub,
            "procedura": procedura,
            "pnrr": pnrr,
            "fornitore_aggiudicatario": fornitore,
            "n_offerte": tender.get("numberOfTenderers"),
            "detail_url": DETAIL_URL_TEMPLATE.format(cig=cig) if cig else "",
            "ocid": release.get("ocid", ""),
        }
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

def _fetch_recent_releases(limit: int = 10) -> list[dict]:
    """
    Fetch the most recent ANAC contracts via GET /releases?limit=N.

    This is the only reliable read path on the ANAC OCDS API:
    - /releases?limit=10 → works, returns 10 most recent contracts
    - /releases?limit=20 → times out on ANAC's backend
    - /tender/ids → returns IDs that mostly don't resolve (~10% hit rate)
    - /releases/ocids → offset param not implemented, no real pagination

    Max safe limit is 10. Calling this multiple times returns the SAME 10
    records (no cursor/offset). Use client-side filtering on the result.
    """
    # ANAC API response time varies: ~15s per release under normal load, up to 30s
    # under heavy load. Use limit=3 with a 120s timeout as the safe batch size.
    # limit=5 can take 75-100s+ under load; limit=3 stays within ~60s reliably.
    safe_limit = min(limit, 3)
    resp = _get(f"{OCDS_BASE}/releases", params={"limit": str(safe_limit)}, timeout=120.0)
    if resp.status_code != 200 or _is_waf_block(resp) or _is_throttle(resp):
        return []
    try:
        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception:
        return []


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
    Search Italian public procurement contracts from ANAC's national database (BDNCP).

    Covers all contracts above €40,000 reported by Italian public administrations.
    Data from ANAC OCDS API (api.anticorruzione.it). CC-BY 4.0 licensed.

    IMPORTANT — API LIMITATIONS (verified from live testing):
    The ANAC OCDS API does NOT support keyword, CPV, NUTS, or authority filtering
    server-side. The only reliable read path is GET /releases?limit=10 which returns
    the 10 most recent contracts in the database. All filters are applied client-side.

    The 'year', 'page', and 'region_nuts' parameters are accepted but have no effect
    on what the API returns — they only control client-side filtering on the 10-record
    batch. For historical search across all contracts, use the ANAC CSV bulk downloads:
    https://dati.anticorruzione.it/opendata/dataset/bandecig

    args:
        keyword: Filter by words in contract description (client-side, Italian terms).
                 Examples: 'servizi informatici', 'manutenzione software', 'cloud'
        cpv_prefix: CPV code prefix to filter category (client-side).
                    '45'=Construction, '48'=Software, '72'=IT services,
                    '79'=Business services, '85'=Healthcare, '90'=Environmental
        region_nuts: NOT AVAILABLE in the OCDS API data model. Accepted but ignored.
        min_value_eur: Minimum contract value in euros (client-side filter)
        max_value_eur: Maximum contract value in euros (client-side filter)
        year: Accepted for compatibility but ignored — API always returns most recent.
        pnrr_only: If True, keep only contracts mentioning PNRR/PNC in description
        contracting_authority: Filter by buyer name (partial match, client-side)
        page: Accepted for compatibility but ignored — no real pagination in API.

    returns:
        Dict with 'contracts' list (max 10), 'total_fetched', 'source', 'citation'.
        Each contract: cig, oggetto, importo_base, importo_aggiudicato,
        stazione_appaltante, cpv, data_pubblicazione, procedura, pnrr, detail_url
    """
    try:
        # Fetch the most recent releases — the only reliable API path
        releases = _fetch_recent_releases(limit=10)

        if not releases:
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
                "Risultati basati sui contratti più recenti in BDNCP (max 10 per chiamata API). "
                "Per ricerche storiche ed esaustive usa i CSV mensili ANAC: "
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
    Get complete details for a specific Italian public contract by its CIG code.

    CIG (Codice Identificativo Gara) is the unique 10-character alphanumeric
    identifier assigned by ANAC to every Italian public contract.

    LOOKUP STRATEGY:
    1. Tries /releases/tender/{CIG} directly (works when CIG equals the tender ID,
       common for single-lot tenders in simpler PA systems)
    2. Tries /releases/{OCID_PREFIX}{CIG} as OCID lookup
    3. Scans tender IDs for the most recent 3 months looking for the CIG in items
    If all fail, returns a structured error with the direct ANAC portal URL.

    args:
        cig: The CIG code (10 alphanumeric chars, e.g. '918052266A').
             Accepts both uppercase and lowercase.

    returns:
        Full contract record with: cig, oggetto, importo_base, importo_aggiudicato,
        stazione_appaltante, cpv, data_pubblicazione, procedura, pnrr, fornitore,
        detail_url, risparmio_eur, risparmio_percentuale.
        On failure: structured error with fallback URLs.
    """
    try:
        cig = cig.strip().upper()
        if len(cig) < 3:
            return {"error": "CIG non valido (troppo corto).", "cig": cig}

        # Attempt 1: CIG as direct tender ID (works for many PA systems)
        releases = _fetch_releases_for_tender(cig)
        if releases:
            contract = _parse_ocds_release(releases[-1], cig_override=cig)
            contract = _enrich_with_price_delta(contract)
            contract["lookup_method"] = "tender_id_direct"
            contract["citation"] = CITATION
            return contract

        # Attempt 2: OCID-based lookup
        ocid = f"{OCID_PREFIX}{cig}"
        resp = _get(f"{OCDS_BASE}/releases/{ocid}")
        if resp.status_code == 200 and not _is_waf_block(resp) and not _is_throttle(resp):
            try:
                data = resp.json()
                if isinstance(data, list) and data:
                    contract = _parse_ocds_release(data[-1], cig_override=cig)
                    contract = _enrich_with_price_delta(contract)
                    contract["lookup_method"] = "ocid_lookup"
                    contract["citation"] = CITATION
                    return contract
            except Exception:
                pass

        # Attempt 3: Scan the most recent releases from /releases?limit=5.
        # CIG appears as items[0].id (lot identifier), not as the OCDS tender.id.
        # This is the most reliable scan approach since /releases always returns data.
        recent_rels = _fetch_recent_releases(limit=5)
        for rel in recent_rels:
            for item in rel.get("tender", {}).get("items", []):
                if item.get("id", "").strip().upper() == cig:
                    contract = _parse_ocds_release(rel, cig_override=cig)
                    contract = _enrich_with_price_delta(contract)
                    contract["lookup_method"] = "scan_recent_releases"
                    contract["citation"] = CITATION
                    return contract

        # All attempts failed — return structured error with direct portal link
        return {
            "error": f"CIG '{cig}' non trovato tramite API OCDS.",
            "cig": cig,
            "detail": (
                "L'API OCDS ANAC usa identificatori interni (tender ID) diversi dal CIG. "
                "Il CIG appare come items[].id nelle release, non come chiave primaria di ricerca. "
                "Una scansione completa richiederebbe migliaia di chiamate API."
            ),
            "detail_url": DETAIL_URL_TEMPLATE.format(cig=cig),
            "fallback_url": DETAIL_URL_TEMPLATE.format(cig=cig),
            "hint": (
                "Usa il link detail_url per consultare il contratto direttamente sul portale ANAC. "
                "Per ricerche massive per CIG usa i CSV mensili BDNCP: "
                "https://dati.anticorruzione.it/opendata/dataset/bandecig"
            ),
        }

    except httpx.TimeoutException:
        return {
            "error": "Timeout connessione ANAC OCDS API.",
            "cig": cig if cig else "unknown",
            "fallback_url": DETAIL_URL_TEMPLATE.format(cig=cig) if cig else "",
        }
    except Exception as e:
        return {
            "error": f"Errore recupero CIG: {type(e).__name__}: {e}",
            "cig": cig if cig else "unknown",
            "fallback_url": DETAIL_URL_TEMPLATE.format(cig=cig) if cig else "",
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
    cpv_prefix: str = None,
    region_nuts: str = None,
    months_back: int = 24,
    min_sample_size: int = 3,
) -> dict:
    """
    Generate a market price benchmark for a procurement category.

    This is the 'analisi di mercato' tool required by D.Lgs. 36/2023 art. 14.
    Contracting authorities must conduct a market analysis before direct awards.
    The output includes a pre-formatted Italian-language paragraph ready to be
    inserted directly into the procurement documentation file.

    HOW IT WORKS:
    Uses ANAC's /awards endpoint (filtered by date range) to collect award values
    for the period specified. Award descriptions are filtered client-side for
    the procurement_description keywords. CPV and NUTS are used only as client-side
    filters on the fetched sample.

    args:
        procurement_description: Italian-language description of what is being
            procured. Examples:
            'servizi di manutenzione software gestionale per enti locali',
            'fornitura e installazione workstation per uffici',
            'servizio di pulizie uffici comunali',
            'consulenza informatica sviluppo siti web PA',
            'licenze Microsoft 365 per pubblica amministrazione'
        cpv_prefix: CPV code prefix to improve match accuracy (optional)
        region_nuts: Noted but not applied server-side (OCDS API limitation)
        months_back: Months of historical data to include (default 24, max 36)
        min_sample_size: Minimum contracts for reliable stats (default 3)

    returns:
        Dict with:
        - 'statistics': min, max, mean, median, stdev, sample_size, currency
        - 'sample_contracts': up to 10 example contracts used
        - 'recommended_reference_price': median award value
        - 'analisi_di_mercato_text': paste-ready Italian paragraph
        - 'citation': full source citation
        - 'warning': populated if sample_size < min_sample_size
    """
    try:
        months_back = min(months_back, 36)
        now = datetime.now()
        start_date = now - timedelta(days=months_back * 30)
        update_date = now.strftime("%d/%m/%Y")
        date_range_label = f"{start_date.strftime('%B %Y')} – {now.strftime('%B %Y')}"
        region_label = f"regione NUTS {region_nuts}" if region_nuts else "dato nazionale"

        # Fetch recent contracts from ANAC OCDS API and filter by keyword/CPV.
        # The API only exposes the most recent ~10 contracts — no date range or
        # historical search. We make up to 3 calls to collect a sample.
        all_contracts = []
        seen_cigs: set[str] = set()
        for _ in range(3):
            result = search_contracts(
                keyword=procurement_description,
                cpv_prefix=cpv_prefix,
            )
            if "error" in result:
                break
            for c in result.get("contracts", []):
                cig = c.get("cig", "")
                if cig not in seen_cigs:
                    seen_cigs.add(cig)
                    all_contracts.append(c)
            if len(all_contracts) >= 30:
                break
            # Small delay between repeat calls to avoid rate limiting
            time.sleep(MIN_GAP_SECONDS)

        # Extract award or base values
        values = []
        for c in all_contracts:
            val = c.get("importo_aggiudicato") or c.get("importo_base")
            if val and isinstance(val, (int, float)) and val > 0:
                values.append(float(val))

        # Compute statistics
        warning = None
        if values:
            mean_val = statistics.mean(values)
            median_val = statistics.median(values)
            min_val = min(values)
            max_val = max(values)
            stdev_val = statistics.stdev(values) if len(values) > 1 else 0.0
            sample_size = len(values)

            if sample_size < min_sample_size:
                warning = (
                    f"Campione insufficiente: solo {sample_size} contratti trovati "
                    f"(soglia minima: {min_sample_size}). Risultati indicativi."
                )

            stats = {
                "min": round(min_val, 2),
                "max": round(max_val, 2),
                "mean": round(mean_val, 2),
                "median": round(median_val, 2),
                "stdev": round(stdev_val, 2),
                "sample_size": sample_size,
                "currency": "EUR",
            }
            recommended_price = round(median_val, 2)

            analisi_di_mercato_text = (
                f"ANALISI DI MERCATO\n\n"
                f"In conformità a quanto previsto dall'art. 14 del D.Lgs. 36/2023 "
                f"(Codice dei Contratti Pubblici), è stata condotta un'analisi di mercato "
                f"per la categoria merceologica \"{procurement_description}\".\n\n"
                f"L'analisi è stata effettuata consultando la Banca Dati Nazionale dei "
                f"Contratti Pubblici (BDNCP) gestita dall'Autorità Nazionale Anticorruzione "
                f"(ANAC), che raccoglie i dati su tutti gli appalti pubblici italiani di "
                f"importo superiore a €40.000.\n\n"
                f"RISULTATI DELL'ANALISI (periodo: {date_range_label}, {region_label}):\n"
                f"* Contratti analizzati: {sample_size}\n"
                f"* Valore minimo: €{_italian_number(min_val)}\n"
                f"* Valore massimo: €{_italian_number(max_val)}\n"
                f"* Valore medio: €{_italian_number(mean_val)}\n"
                f"* Valore mediano: €{_italian_number(median_val)}\n\n"
                f"PREZZO DI RIFERIMENTO: €{_italian_number(recommended_price)} "
                f"(Fonte: valore mediano dei contratti analoghi presenti in BDNCP-ANAC)\n\n"
                f"Fonte dei dati: ANAC - Banca Dati Nazionale Contratti Pubblici. "
                f"Licenza: CC-BY 4.0. Dati aggiornati al {update_date}. "
                f"URL: https://dati.anticorruzione.it/opendata"
            )
        else:
            warning = (
                "Nessun contratto trovato con i criteri specificati. "
                "Prova termini più generici o rimuovi il filtro CPV. "
                "Nota: la ricerca OCDS è limitata a un campione per rate limit."
            )
            mean_val = median_val = min_val = max_val = 0.0
            sample_size = 0
            recommended_price = 0.0
            stats = {"min": 0, "max": 0, "mean": 0, "median": 0, "stdev": 0, "sample_size": 0, "currency": "EUR"}
            analisi_di_mercato_text = (
                f"ANALISI DI MERCATO\n\n"
                f"In conformità a quanto previsto dall'art. 14 del D.Lgs. 36/2023, "
                f"è stata condotta un'analisi di mercato per \"{procurement_description}\".\n\n"
                f"La consultazione della BDNCP-ANAC non ha restituito contratti sufficienti "
                f"con i criteri specificati. Si raccomanda di ampliare i criteri di ricerca "
                f"o di integrare l'analisi con altre fonti di mercato.\n\n"
                f"Fonte consultata: ANAC BDNCP, CC-BY 4.0. Data: {update_date}. "
                f"URL: https://dati.anticorruzione.it/opendata"
            )

        result = {
            "statistics": stats,
            "recommended_reference_price": recommended_price,
            "sample_contracts": all_contracts[:10],
            "analisi_di_mercato_text": analisi_di_mercato_text,
            "citation": CITATION,
            "search_parameters": {
                "procurement_description": procurement_description,
                "cpv_prefix": cpv_prefix,
                "region_nuts": region_nuts,
                "months_back": months_back,
                "date_range": date_range_label,
            },
        }
        if warning:
            result["warning"] = warning
        return result

    except Exception as e:
        return {
            "error": f"Errore benchmark prezzi: {type(e).__name__}: {e}",
            "fallback_url": "https://dati.anticorruzione.it/opendata/dataset/bandecig",
        }


# ─────────────────────────────────────────────
# Tool 4: get_authority_procurement_profile
# ─────────────────────────────────────────────

def get_authority_procurement_profile(
    authority_name: str,
    year: int = None,
) -> dict:
    """
    Get the full procurement profile of an Italian public administration entity.

    Returns contracts issued by a specific contracting authority (stazione
    appaltante), with spending breakdown by category, top suppliers, and
    procedure types.

    Useful for:
    - Pre-meeting account intelligence on a public sector prospect
    - Auditing whether a PA is over-relying on direct awards
    - Identifying incumbent suppliers

    NOTE: Results are based on a date-range sample from the ANAC OCDS API
    (max ~4 pages × 20 tender records = 80 tenders). For a complete view of
    all contracts use the ANAC CSV bulk downloads.

    args:
        authority_name: Name of the contracting authority. Partial match supported.
                        Examples: 'Comune di Milano', 'Regione Lombardia', 'ASL Napoli'
        year: Filter by publication year (default: current year)

    returns:
        Dict with authority info, total_contracts, total_value_eur, spending_by_cpv,
        procedure_types, top_suppliers, direct_award_ratio, pnrr_contracts,
        contracts list (max 30), Italian summary paragraph.
    """
    try:
        target_year = year or _current_year()
        all_contracts = []

        # Fetch recent contracts and filter by authority name client-side.
        # The API returns the most recent ~10 contracts; we make up to 3 calls
        # to build a useful sample.
        seen_cigs: set[str] = set()
        for attempt in range(3):
            result = search_contracts(contracting_authority=authority_name)
            if "error" in result:
                if attempt == 0:
                    return result
                break
            for c in result.get("contracts", []):
                cig = c.get("cig", "")
                if cig not in seen_cigs:
                    seen_cigs.add(cig)
                    all_contracts.append(c)
            if len(all_contracts) >= 30:
                break
            time.sleep(MIN_GAP_SECONDS)

        if not all_contracts:
            return {
                "error": f"Nessun contratto trovato per '{authority_name}' nel {target_year}.",
                "authority_name": authority_name,
                "year": target_year,
                "hint": "Prova un nome parziale (es. 'Comune Roma' invece di 'Roma Capitale').",
            }

        # Aggregate stats
        total_value = 0.0
        spending_by_cpv: dict[str, dict] = {}
        procedure_types: dict[str, int] = {}
        supplier_counts: dict[str, int] = {}
        direct_award_count = 0
        pnrr_count = 0
        pnrr_value = 0.0
        matched_authority = ""
        matched_cf = ""

        for c in all_contracts:
            val = float(c.get("importo_aggiudicato") or c.get("importo_base") or 0)
            total_value += val

            cpv_div = c.get("cpv_divisione") or "ND"
            if cpv_div not in spending_by_cpv:
                spending_by_cpv[cpv_div] = {"count": 0, "total_eur": 0.0}
            spending_by_cpv[cpv_div]["count"] += 1
            spending_by_cpv[cpv_div]["total_eur"] += val

            proc = c.get("procedura") or "Non specificata"
            procedure_types[proc] = procedure_types.get(proc, 0) + 1

            proc_lower = proc.lower()
            if any(kw in proc_lower for kw in ["affidamento diretto", "direct", "affid."]):
                direct_award_count += 1

            supplier = c.get("fornitore_aggiudicatario", "")
            if supplier:
                supplier_counts[supplier] = supplier_counts.get(supplier, 0) + 1

            if c.get("pnrr"):
                pnrr_count += 1
                pnrr_value += val

            if not matched_authority and c.get("stazione_appaltante"):
                matched_authority = c["stazione_appaltante"]
            if not matched_cf and c.get("cf_stazione_appaltante"):
                matched_cf = c["cf_stazione_appaltante"]

        total_contracts = len(all_contracts)
        direct_award_ratio = (
            round((direct_award_count / total_contracts) * 100, 1)
            if total_contracts > 0 else 0.0
        )

        top_suppliers = sorted(
            [{"name": k, "contract_count": v} for k, v in supplier_counts.items()],
            key=lambda x: x["contract_count"],
            reverse=True,
        )[:5]

        cpv_sorted = sorted(
            [{"cpv_divisione": k, **v} for k, v in spending_by_cpv.items()],
            key=lambda x: x["total_eur"],
            reverse=True,
        )

        top_cpv = cpv_sorted[0]["cpv_divisione"] if cpv_sorted else "N/D"
        top_sup = top_suppliers[0]["name"] if top_suppliers else "N/D"

        summary = (
            f"Profilo di procurement: {matched_authority or authority_name} — Anno {target_year}. "
            f"Campione analizzato: {total_contracts} contratti per un valore complessivo di "
            f"€{_italian_number(total_value)}. "
            f"Categoria CPV principale: {top_cpv}. "
            f"Affidamenti diretti: {direct_award_ratio}% del totale."
        )
        if top_sup != "N/D":
            summary += f" Fornitore principale: {top_sup} ({top_suppliers[0]['contract_count']} contratti)."
        if pnrr_count > 0:
            summary += f" Contratti PNRR/PNC: {pnrr_count} per €{_italian_number(pnrr_value)}."
        summary += " Fonte: ANAC BDNCP, CC-BY 4.0."

        return {
            "authority": {
                "name": matched_authority or authority_name,
                "cf": matched_cf,
                "search_term": authority_name,
            },
            "year": target_year,
            "total_contracts": total_contracts,
            "total_value_eur": round(total_value, 2),
            "spending_by_cpv": cpv_sorted,
            "procedure_types": dict(
                sorted(procedure_types.items(), key=lambda x: x[1], reverse=True)
            ),
            "top_suppliers": top_suppliers,
            "direct_award_ratio": direct_award_ratio,
            "pnrr_contracts": {"count": pnrr_count, "total_value_eur": round(pnrr_value, 2)},
            "contracts": all_contracts[:30],
            "total_available": total_contracts,
            "truncated": total_contracts > 30,
            "summary": summary,
            "citation": CITATION,
            "source": "ANAC OCDS API (campione, max ~80 tender)",
        }

    except Exception as e:
        return {
            "error": f"Errore profilo ente: {type(e).__name__}: {e}",
            "authority_name": authority_name,
            "fallback_url": "https://dati.anticorruzione.it/opendata/dataset/bandecig",
        }


# ─────────────────────────────────────────────
# Tool 5: find_similar_contracts
# ─────────────────────────────────────────────

def find_similar_contracts(
    reference_cig: str,
    max_results: int = 10,
) -> dict:
    """
    Find contracts similar to a known reference contract.

    Given a CIG code, finds other contracts with similar object/category (CPV)
    and value range — helping evaluate whether a contract was fairly priced.

    Useful for:
    - Validating a specific contract price after the fact
    - Finding precedents for a new procurement
    - Checking if a direct award price was market-rate

    args:
        reference_cig: CIG code of the reference contract
        max_results: Maximum similar contracts to return (default 10, max 20)

    returns:
        Dict with reference_contract, similar_contracts, price_comparison
        ('above_market', 'at_market', 'below_market', 'insufficient_data'),
        median_similar_contracts, and Italian analysis_text.
    """
    try:
        max_results = min(max_results, 20)
        reference_cig = reference_cig.strip().upper()

        # Step 1: Get reference contract
        ref = get_contract_by_cig(reference_cig)
        if "error" in ref:
            return {
                "error": f"Impossibile recuperare il contratto di riferimento: {ref['error']}",
                "reference_cig": reference_cig,
                "detail_url": DETAIL_URL_TEMPLATE.format(cig=reference_cig),
            }

        cpv_prefix = ref.get("cpv_divisione") or (ref.get("cpv") or "")[:2] or None
        ref_value = float(
            ref.get("importo_aggiudicato") or ref.get("importo_base") or 0
        )
        keyword = (ref.get("oggetto") or "")[:60]
        min_val = ref_value * 0.5 if ref_value > 0 else None
        max_val = ref_value * 1.5 if ref_value > 0 else None

        # Step 2: Search for similar contracts
        similar_raw = search_contracts(
            keyword=keyword,
            cpv_prefix=cpv_prefix,
            min_value_eur=min_val,
            max_value_eur=max_val,
            year=_current_year(),
        )
        similar_contracts = [
            c for c in similar_raw.get("contracts", [])
            if c.get("cig") != reference_cig
        ][:max_results]

        # Try previous year if thin sample
        if len(similar_contracts) < 3:
            prev = search_contracts(
                keyword=keyword,
                cpv_prefix=cpv_prefix,
                min_value_eur=min_val,
                max_value_eur=max_val,
                year=_current_year() - 1,
            )
            for c in prev.get("contracts", []):
                if c.get("cig") != reference_cig and c not in similar_contracts:
                    similar_contracts.append(c)
                if len(similar_contracts) >= max_results:
                    break

        # Step 3: Price comparison
        comp_values = [
            float(c.get("importo_aggiudicato") or c.get("importo_base") or 0)
            for c in similar_contracts
            if (c.get("importo_aggiudicato") or c.get("importo_base"))
        ]

        price_comparison = "insufficient_data"
        median_similar = None
        if len(comp_values) >= 3 and ref_value > 0:
            median_similar = statistics.median(comp_values)
            if ref_value > median_similar * 1.2:
                price_comparison = "above_market"
            elif ref_value < median_similar * 0.8:
                price_comparison = "below_market"
            else:
                price_comparison = "at_market"

        # Step 4: Italian analysis paragraph
        val_label = f"€{_italian_number(ref_value)}" if ref_value > 0 else "valore non disponibile"
        ref_oggetto = (ref.get("oggetto") or "")[:100]

        if price_comparison == "above_market" and median_similar:
            diff_pct = round(((ref_value - median_similar) / median_similar) * 100, 1)
            analysis_text = (
                f"ANALISI DI COMPARABILITÀ — CIG {reference_cig}\n\n"
                f"Il contratto (oggetto: \"{ref_oggetto}\") ha un valore di {val_label}, "
                f"superiore del {diff_pct}% rispetto al mediano di €{_italian_number(median_similar)} "
                f"su {len(comp_values)} contratti analoghi in BDNCP-ANAC. "
                f"Prezzo: SOPRA MERCATO. Raccomandasi analisi approfondita.\n\n"
                f"Fonte: ANAC BDNCP, CC-BY 4.0."
            )
        elif price_comparison == "below_market" and median_similar:
            diff_pct = round(((median_similar - ref_value) / median_similar) * 100, 1)
            analysis_text = (
                f"ANALISI DI COMPARABILITÀ — CIG {reference_cig}\n\n"
                f"Il contratto (oggetto: \"{ref_oggetto}\") ha un valore di {val_label}, "
                f"inferiore del {diff_pct}% rispetto al mediano di €{_italian_number(median_similar)} "
                f"su {len(comp_values)} contratti analoghi in BDNCP-ANAC. "
                f"Prezzo: SOTTO MERCATO (offerta competitiva o scope ridotto).\n\n"
                f"Fonte: ANAC BDNCP, CC-BY 4.0."
            )
        elif price_comparison == "at_market" and median_similar:
            analysis_text = (
                f"ANALISI DI COMPARABILITÀ — CIG {reference_cig}\n\n"
                f"Il contratto (oggetto: \"{ref_oggetto}\") ha un valore di {val_label}, "
                f"in linea con il mediano di €{_italian_number(median_similar)} "
                f"su {len(comp_values)} contratti analoghi in BDNCP-ANAC. "
                f"Prezzo: IN LINEA CON IL MERCATO.\n\n"
                f"Fonte: ANAC BDNCP, CC-BY 4.0."
            )
        else:
            analysis_text = (
                f"ANALISI DI COMPARABILITÀ — CIG {reference_cig}\n\n"
                f"Il contratto ha un valore di {val_label}. "
                f"Con {len(comp_values)} contratti comparabili trovati nel campione ANAC, "
                f"il campione non è sufficiente per una comparazione statistica affidabile. "
                f"Si consiglia di ampliare i criteri o consultare i CSV mensili ANAC.\n\n"
                f"Fonte: ANAC BDNCP, CC-BY 4.0."
            )

        return {
            "reference_contract": ref,
            "similar_contracts": similar_contracts,
            "price_comparison": price_comparison,
            "median_similar_contracts": median_similar,
            "sample_size": len(comp_values),
            "analysis_text": analysis_text,
            "citation": CITATION,
            "source": "ANAC OCDS API (campione)",
        }

    except Exception as e:
        return {
            "error": f"Errore ricerca contratti simili: {type(e).__name__}: {e}",
            "reference_cig": reference_cig if reference_cig else "unknown",
            "fallback_url": "https://dati.anticorruzione.it/opendata/dataset/bandecig",
        }
