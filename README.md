# ANAC Procurement Intelligence MCP Server

AI-powered access to Italy's national public procurement database — no API key, no credentials, legally citable outputs.

---

## What is ANAC?

**ANAC** (Autorità Nazionale Anticorruzione) is Italy's National Anti-Corruption Authority. It maintains the **BDNCP** (Banca Dati Nazionale dei Contratti Pubblici), which records every public contract issued by Italian public administrations above €40,000.

The data is published as open data under **CC-BY 4.0 license**, meaning all outputs from this server can be legally cited in official procurement documents, audit reports, and compliance filings.

---

## Why This Server Exists: The Analisi di Mercato Use Case

Italian procurement law (**D.Lgs. 36/2023, art. 14**) requires every contracting authority to conduct a **market analysis** before issuing a direct award. This is called the *analisi di mercato* and must be documented in the procurement file.

In practice, this means: before awarding a €80,000 software maintenance contract to a single supplier, you must demonstrate that €80,000 is a reasonable market price.

This server automates that step. Ask:

> "Fai un'analisi di mercato per servizi di manutenzione software gestionale, CPV 72, valore stimato €80.000"

The server queries ANAC's database of real contracts, computes the statistical benchmark, and returns a **ready-to-paste Italian paragraph** with all required citation elements.

---

## No API Key Needed

ANAC's data is sovereign Italian open data. No registration, no API keys, no OAuth.

Just run the server and connect.

---

## Installation

```bash
git clone <this-repo>
cd anac-mcp
pip install -r requirements.txt
cp .env.example .env
```

## Running the Server

```bash
uvicorn server:app --host 0.0.0.0 --port 8000
```

The server exposes:
- **MCP endpoint**: `http://your-server:8000/mcp`
- **Health check**: `http://your-server:8000/health`

---

## Connect to Intric

1. Open your Intric workspace settings
2. Add a new MCP server with URL: `http://your-server:8000/mcp`
3. No API key field needed
4. The server will appear with all 5 tools and 3 resources available

---

## 5 Example Prompts

### 1. Analisi di mercato automatica

> "Fai un'analisi di mercato per servizi di manutenzione software gestionale per enti locali, CPV 72, valore stimato €80.000"

The server returns a complete `analisi_di_mercato_text` ready to paste into the procurement file, including statistics, sample size, median reference price, and the full ANAC citation.

---

### 2. Ricerca contratti IT per regione

> "Cerca tutti i contratti IT aggiudicati dalla Regione Lombardia nel 2024"

Uses `search_contracts` with `contracting_authority="Regione Lombardia"`, `cpv_prefix="72"`, `year=2024`.

---

### 3. Dettaglio CIG specifico

> "Dammi i dettagli del CIG 918052266A"

Uses `get_contract_by_cig("918052266A")` to retrieve the full lifecycle: tender details, awarded value vs. base price, winning supplier, procedure type, and a direct link to the ANAC dashboard.

---

### 4. Profilo procurement di un ente

> "Qual è il profilo di procurement del Comune di Roma?"

Uses `get_authority_procurement_profile("Comune di Roma")` to return total spend, breakdown by CPV category, top suppliers, direct award ratio, and a one-paragraph Italian summary.

---

### 5. Validazione prezzo di un contratto

> "Trova contratti simili a questo CIG e dimmi se il prezzo era di mercato: 918052266A"

Uses `find_similar_contracts("918052266A")` to find comparable contracts by CPV and value range, compute the median, and return an Italian analysis paragraph with the verdict: `above_market`, `at_market`, or `below_market`.

---

## Tools

| Tool | Description |
|------|-------------|
| `search_contracts` | Search ANAC database by keyword, CPV, region, value, year, PNRR flag |
| `get_contract_by_cig` | Get full details of a specific contract by CIG code |
| `benchmark_market_prices` | Generate analisi di mercato with statistics and paste-ready text |
| `get_authority_procurement_profile` | Full procurement profile of any Italian PA entity |
| `find_similar_contracts` | Find comparable contracts and validate a price against market |

## Resources

| Resource URI | Description |
|--------------|-------------|
| `resource://anac/procurement_law_guide` | D.Lgs. 36/2023 guide, CIG explained, citation formats |
| `resource://anac/nuts_codes` | NUTS2 codes for all 20 Italian regions |
| `resource://anac/cpv_categories` | CPV category reference for Italian PA procurement |

---

## API Access Methods

### Method A: OCDS REST API (primary)
- URL: `https://dati.anticorruzione.it/opendata/ocds/api/`
- Format: JSON (Open Contracting Data Standard)
- Updated: continuously
- **Note**: Some IP ranges may receive 403 errors. This is an ANAC infrastructure issue, not an authentication problem.

### Method B: CSV Bulk Downloads (fallback reference)
- Updated: 2nd of each month
- BandiCIG (>€40k): `https://dati.anticorruzione.it/opendata/dataset/bandecig`
- SmartCIG (<€40k): `https://dati.anticorruzione.it/opendata/dataset/cig`
- Aggiudicazioni: `https://dati.anticorruzione.it/opendata/dataset/aggiudicazioni`
- Aggiudicatari: `https://dati.anticorruzione.it/opendata/dataset/aggiudicatari`

When the OCDS API returns a 403, tools return a structured error with the fallback URL pointing to the correct CSV dataset.

---

## Data License

All data from ANAC is published under **Creative Commons CC-BY 4.0**.

This means outputs from this server — including benchmark statistics and analisi di mercato paragraphs — can be cited in official Italian PA documents, audit reports, and procurement files without additional permission.

**Standard citation:**
```
Fonte: ANAC - Banca Dati Nazionale Contratti Pubblici (BDNCP).
Licenza: Creative Commons CC-BY 4.0.
URL: https://dati.anticorruzione.it/opendata
```

---

## Rate Limits

The server enforces a maximum of **20 requests per minute** to the ANAC API to avoid overloading the public infrastructure. This is handled automatically with a thread-safe rate limiter. No action required on your part.
