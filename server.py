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

== CAPACITÀ DEL SERVER ==
Il server carica centinaia di migliaia di contratti ANAC reali in DuckDB in-memory.
Query SQL in meno di 1 secondo. Copertura: ultimi 3–12 mesi.
Al primo avvio: 2–3 minuti per caricarsi. Se "initializing"/"loading", riprovare tra 60s.

== DECISION TREE ==

SE l'utente chiede analisi di mercato, congruità prezzo, benchmark, "quanto costa",
   verifica importo, supporto affidamento, "è un prezzo giusto", comparazione:
   → benchmark_market_prices()
   QUESTO È LO STRUMENTO PRINCIPALE. Produce un'analisi completa in 7 sezioni.
   REGOLE OBBLIGATORIE:
   - SEMPRE passare importo_previsto se l'utente menziona un budget/importo
   - SEMPRE passare tipo_ente se l'utente menziona il tipo di ente:
     "per il Comune" → tipo_ente="Comune"
     "della ASL" → tipo_ente="ASL"
     "per la Provincia" → tipo_ente="Provincia"
     "dell'Università" → tipo_ente="Universita"
     "dell'ospedale" → tipo_ente="Azienda ospedaliera"
   - procurement_description deve essere SPECIFICO, non generico:
     BENE: "manutenzione sito web istituzionale"
     MALE: "servizi informatici" (troppo generico)
   - Se il CPV è noto o deducibile, passare cpv_prefix
   Lo strumento usa ricerca progressiva: parte stretto e allarga automaticamente.
   Restituisce 7 sezioni: ricognizione, prezzi, procedure, esempi, congruità,
   rischi, conclusioni. Il testo è pronto per il fascicolo di gara.

SE l'utente cerca contratti genericamente (senza bisogno di analisi statistica):
   → search_contracts(keyword=..., cpv_prefix=..., region=..., importo_min=..., importo_max=...)
   Fino a 25 contratti. SEMPRE usare termini italiani.

SE l'utente fornisce un CIG specifico:
   → get_contract_by_cig(cig="XXXXXXXXXX")

SE l'utente chiede il profilo acquisti di un ente specifico:
   → get_authority_procurement_profile(authority_name=...)

SE l'utente vuole trovare contratti comparabili per evidenza:
   → find_similar_contracts(procurement_description=..., importo_riferimento=...)

== FORMATO RISPOSTE PER ANALISI DI MERCATO ==
Quando usi benchmark_market_prices, presenta la risposta seguendo le 7 sezioni
restituite dallo strumento. NON riscrivere o semplificare il contenuto.
Includi SEMPRE:
- Le statistiche (mediana, P25–P75, range)
- Gli esempi concreti con CIG
- La valutazione di congruità con posizionamento vs quartili
- I rischi e cautele quando presenti
- Le conclusioni operative

Se qualita_campione è "bassa" o "insufficiente", EVIDENZIALO chiaramente.
Se ci sono rischi, NON ometterli per sembrare più positivo.

== PARAMETRI CHIAVE ==
cpv_prefix (2+ cifre): '45'=Costruzione, '48'=Software, '72'=Servizi IT,
  '79'=Servizi aziendali, '85'=Sanità, '90'=Ambiente/Pulizia
region: nome regione italiana (es. 'Campania', 'Lombardia', 'Lazio')
tipo_procedura: 'AFFIDAMENTO DIRETTO', 'PROCEDURA APERTA', 'PROCEDURA NEGOZIATA'
tipo_ente: 'Comune', 'ASL', 'Provincia', 'Regione', 'Universita', 'Ministero',
  'Azienda ospedaliera'

== LINEE GUIDA GENERALI ==
- Includere sempre copertura temporale e citazione ANAC
- Per dati storici oltre 12 mesi: https://dati.anticorruzione.it/opendata/dataset/cig""",
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
