"""
ANAC Procurement Intelligence MCP Server

Provides AI-powered access to Italy's national public procurement database (BDNCP)
managed by ANAC (Autorità Nazionale Anticorruzione).

No authentication required — all data is public, CC-BY 4.0 licensed.

Run with:
    uvicorn server:app --host 0.0.0.0 --port 8000
"""

from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse

from fastmcp import FastMCP

from tools import (
    search_contracts,
    get_contract_by_cig,
    benchmark_market_prices,
    get_authority_procurement_profile,
    find_similar_contracts,
)
from resources import (
    get_procurement_law_guide,
    get_nuts_codes,
    get_cpv_categories,
)

# ─────────────────────────────────────────────
# IP allowlist middleware
# ─────────────────────────────────────────────

ALLOWED_IPS = ["*"]  # Open to all — no authentication required


class IPAllowlistMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if "*" in ALLOWED_IPS:
            return await call_next(request)
        client_ip = request.client.host if request.client else None
        if client_ip not in ALLOWED_IPS:
            return JSONResponse({"error": "Forbidden"}, status_code=403)
        return await call_next(request)


middleware = [
    Middleware(IPAllowlistMiddleware),
]

# ─────────────────────────────────────────────
# FastMCP server
# ─────────────────────────────────────────────

mcp = FastMCP(
    name="ANAC Procurement Intelligence",
    instructions="""Accesso alla Banca Dati Nazionale Contratti Pubblici (BDNCP) gestita da ANAC.
Dati pubblici, licenza CC-BY-SA 4.0. Nessuna credenziale richiesta.

== CAPACITÀ REALI DEL SERVER ==
Il server scarica i dati CSV mensili di ANAC (decine di migliaia di contratti per mese)
e li carica in un database DuckDB in-memory. Tutti gli strumenti eseguono query SQL
con risposte in meno di 1 secondo. Copertura: ultimi 3–12 mesi.

Al primo avvio il database impiega 2–3 minuti a caricarsi. Se lo stato è "initializing"
o "loading", comunica all'utente di riprovare tra 60 secondi.

== DECISION TREE: quale strumento usare ==

1. Cercare contratti (per keyword, CPV, regione, importo, procedura, stato)
   → search_contracts(keyword=..., cpv_prefix=..., region=..., importo_min=..., importo_max=...)
   PRIMO strumento da chiamare per qualsiasi ricerca di contratti.
   Restituisce fino a 25 contratti con tutti i dettagli, ordinati per data.
   SEMPRE usare termini italiani per keyword (es. 'servizi informatici', non 'IT services').

2. Cercare un CIG specifico
   → get_contract_by_cig(cig="XXXXXXXXXX")
   Lookup diretto per codice CIG (10 caratteri alfanumerici).
   Include dati dell'ente dall'anagrafica ANAC (tipo, provincia, città).

3. Analisi di mercato / benchmark prezzi / congruità importo
   → benchmark_market_prices(procurement_description=..., importo_previsto=..., cpv_prefix=...)
   SEMPRE passare importo_previsto se l'utente menziona un budget o importo.
   Restituisce SEMPRE un paragrafo completo pronto per il fascicolo,
   conforme all'art. 14 D.Lgs. 36/2023, anche con campione piccolo.
   Il paragrafo è legalmente valido: documenta la consultazione ANAC.

4. Profilo acquisti di un ente
   → get_authority_procurement_profile(authority_name=...) o (codice_fiscale=...)
   Statistiche complete: n. contratti, valore totale/medio, top CPV, procedure usate,
   ultimi 10 contratti. Funziona con nome parziale (es. 'Comune di Roma').

5. Trovare contratti simili per comparazione
   → find_similar_contracts(procurement_description=..., cpv_prefix=..., importo_riferimento=...)
   Ordinati per prossimità a importo_riferimento se fornito.
   Utile per costruire la base evidenziale dell'analisi di mercato.

== PARAMETRI CHIAVE ==
cpv_prefix (2+ cifre): '45'=Costruzione, '48'=Software, '72'=Servizi IT,
  '79'=Servizi aziendali, '85'=Sanità, '90'=Ambiente/Pulizia
region: nome regione italiana (es. 'Campania', 'Lombardia', 'Lazio')
tipo_procedura: 'AFFIDAMENTO DIRETTO', 'PROCEDURA APERTA', 'PROCEDURA NEGOZIATA'

== LINEE GUIDA RISPOSTE ==
- Includere sempre la copertura temporale (campo coverage.months_loaded)
- Includere sempre la citazione ANAC dal campo citation
- Se total_matching > returned, suggerire filtri più specifici
- Per dati storici oltre i 12 mesi: https://dati.anticorruzione.it/opendata/dataset/cig""",
    version="1.0.0",
    website_url="https://dati.anticorruzione.it",
)

# ─────────────────────────────────────────────
# Register tools
# ─────────────────────────────────────────────

_no_perm = {"requires_permission": False}

mcp.tool(name="search_contracts", meta=_no_perm)(search_contracts)
mcp.tool(name="get_contract_by_cig", meta=_no_perm)(get_contract_by_cig)
mcp.tool(name="benchmark_market_prices", meta=_no_perm)(benchmark_market_prices)
mcp.tool(name="get_authority_procurement_profile", meta=_no_perm)(get_authority_procurement_profile)
mcp.tool(name="find_similar_contracts", meta=_no_perm)(find_similar_contracts)

# ─────────────────────────────────────────────
# Register resources
# ─────────────────────────────────────────────

mcp.resource("resource://anac/procurement_law_guide")(get_procurement_law_guide)
mcp.resource("resource://anac/nuts_codes")(get_nuts_codes)
mcp.resource("resource://anac/cpv_categories")(get_cpv_categories)

# ─────────────────────────────────────────────
# Custom routes
# ─────────────────────────────────────────────


@mcp.custom_route("/health", methods=["GET"])
async def health_check(request: Request) -> PlainTextResponse:
    return PlainTextResponse("OK")


# ─────────────────────────────────────────────
# ASGI app
# ─────────────────────────────────────────────

app = mcp.http_app(middleware=middleware)
