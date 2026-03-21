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
    instructions="""Access Italy's national public procurement database (BDNCP) managed by ANAC.
Data license: CC-BY 4.0. No credentials required.

== CRITICAL: DATA LIMITATIONS ==
The ANAC OCDS API is extremely slow (~17 seconds per record). This server
caches ~3 of the most recent contracts and serves them instantly. All tools
operate on this small cached sample. Results are INDICATIVE, not exhaustive.
For comprehensive research, direct users to ANAC CSV bulk downloads:
https://dati.anticorruzione.it/opendata/dataset/bandecig

== TOOL ROUTING DECISION TREE ==
Use this to pick the right tool for each user request:

1. "Mostrami i contratti recenti" / "Cerca contratti" / general browsing
   → search_contracts()
   Returns ~3 most recent contracts. Filters are client-side.
   Instant response (cached).

2. "Cercami il CIG XXXXXXXXXX" / specific contract lookup
   → get_contract_by_cig(cig="...")
   Checks cache first (instant). If not cached, tries 2 API calls (~30s each).
   Most CIG codes will NOT be found — response includes ANAC portal link.

3. "Analisi di mercato per..." / "Quanto costa..." / price benchmarks
   → benchmark_market_prices(procurement_description="...", cpv_prefix="...")
   Use Italian procurement descriptions. Returns stats + paste-ready paragraph.
   Based on ~3 cached contracts — always flag as "indicative" to the user.

4. "Contratti di [ente]" / authority-specific queries
   → get_authority_procurement_profile(authority_name="...")
   Filters cache by buyer name. Will often return 0 results (expected).
   Suggest ANAC portal as fallback.

5. "Confronta CIG X con contratti simili"
   → find_similar_contracts(reference_cig="...")
   Compares a CIG against other cached contracts. Very small comparison pool.

== COMMON PARAMETERS ==
- cpv_prefix: 2-digit category code. '72'=IT, '45'=Construction, '48'=Software,
  '79'=Business services, '85'=Healthcare, '90'=Environmental
- keyword: Always use ITALIAN terms (e.g. 'servizi informatici' not 'IT services')

== RESPONSE GUIDELINES ==
- Always mention the sample size limitation to the user
- Always include the ANAC citation from tool responses
- If a tool returns 0 results, suggest CSV bulk downloads as alternative
- If status="warming_up", tell user to wait 60-90 seconds and retry""",
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
