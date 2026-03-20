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
    instructions="""Provides AI-powered access to Italy's national public procurement database (BDNCP) managed by ANAC (Autorità Nazionale Anticorruzione).

WHAT THIS SERVER ENABLES:
- Market price benchmarking for any procurement category (analisi di mercato)
- CIG code lookup and full contract lifecycle details
- Contracting authority procurement profiling
- Similar contract discovery for price validation
- Pre-formatted analisi di mercato text ready for official documents

DATA SOURCE: ANAC Banca Dati Nazionale Contratti Pubblici
- Covers all Italian public contracts above €40,000
- Updated in real-time via OCDS API
- License: CC-BY 4.0 — all outputs are legally citable in official documents
- No credentials required

KEY USE CASE: When asked to prepare an 'analisi di mercato' for any procurement, use benchmark_market_prices() with a precise Italian description of what is being purchased. The output includes a ready-to-paste Italian paragraph for the official file.""",
    version="1.0.0",
    website_url="https://dati.anticorruzione.it",
)

# ─────────────────────────────────────────────
# Register tools
# ─────────────────────────────────────────────

mcp.tool(name="search_contracts")(search_contracts)
mcp.tool(name="get_contract_by_cig")(get_contract_by_cig)
mcp.tool(name="benchmark_market_prices")(benchmark_market_prices)
mcp.tool(name="get_authority_procurement_profile")(get_authority_procurement_profile)
mcp.tool(name="find_similar_contracts")(find_similar_contracts)

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
