"""
ANAC Procurement Intelligence — Static resources.

These resources are hardcoded reference data (no API calls).
They provide procurement law guidance, NUTS codes, and CPV reference tables
for Italian public sector users.
"""


def get_procurement_law_guide() -> str:
    """
    Guide to Italian public procurement law and how ANAC data supports compliance.
    Covers the analisi di mercato requirement, CIG codes, CPV classification,
    and how to cite ANAC data in official procurement documents.
    """
    return """
# Guida al Codice dei Contratti Pubblici e ai Dati ANAC

## Cos'è il CIG

Il **CIG (Codice Identificativo Gara)** è un codice alfanumerico di 10 caratteri
(es. `918052266A`) assegnato dall'ANAC a ogni contratto pubblico italiano.
È obbligatorio per tutti gli acquisti della PA e viene utilizzato per:

- Tracciare il ciclo di vita completo dell'appalto (bando → aggiudicazione → contratto)
- Riferimento univoco in tutti i sistemi gestionali della PA
- Reportistica obbligatoria verso ANAC ai sensi dell'art. 222 del D.Lgs. 36/2023
- Verifica antimafia e controlli di legittimità

**Quando è obbligatorio il CIG?**
- Tutti i contratti superiori a €5.000 (importo al netto di IVA)
- Sono esclusi solo acquisti di modico valore con cassa economale (< €5.000)

---

## BandiCIG vs SmartCIG

| Tipo         | Soglia          | Dataset ANAC          | Note                          |
|--------------|-----------------|----------------------|-------------------------------|
| **BandiCIG** | > €40.000       | `bandecig`           | Procedura completa ANAC       |
| **SmartCIG** | ≤ €40.000       | `cig`                | Procedura semplificata online |

Il dataset **BandiCIG** è il più completo e include: oggetto, importo a base di gara,
stazione appaltante, CPV, NUTS, procedura, PNRR flag.

---

## D.Lgs. 36/2023 — Analisi di Mercato (art. 14)

L'**art. 14 del D.Lgs. 36/2023** (Codice dei Contratti Pubblici) impone alle stazioni
appaltanti di effettuare un'analisi di mercato prima di procedere all'affidamento diretto.

### Requisiti dell'analisi di mercato:
1. Individuare operatori economici presenti sul mercato
2. Verificare la congruità del prezzo rispetto ai valori di mercato
3. Documentare l'analisi nel fascicolo di gara

### Come usare i dati ANAC per l'analisi di mercato:
1. Usa `benchmark_market_prices()` con una descrizione precisa in italiano
2. Il tool restituisce automaticamente il paragrafo pronto da incollare nel fascicolo
3. Il prezzo di riferimento consigliato è il **valore mediano** dei contratti analoghi

---

## Come citare i dati ANAC in documenti ufficiali

**Formato di citazione standard:**

```
Fonte: ANAC - Banca Dati Nazionale Contratti Pubblici (BDNCP).
Licenza: Creative Commons CC-BY 4.0.
Dati aggiornati al [DATA]. URL: https://dati.anticorruzione.it/opendata
```

**Note sulla citabilità:**
- I dati ANAC sono rilasciati con licenza **CC-BY 4.0**, che consente l'uso in
  documenti ufficiali della PA senza restrizioni
- È sufficiente citare la fonte come indicato sopra
- I valori estratti dal database sono legalmente opponibili in sede di controllo

---

## CPV — Codice del Vocabolario Comune degli Appalti

I **CPV (Common Procurement Vocabulary)** sono codici numerici a 8 cifre che
classificano l'oggetto degli appalti pubblici a livello europeo.

### Struttura del codice CPV:
- **2 cifre**: Divisione (categoria principale) — es. `72` = Servizi informatici
- **3 cifre**: Gruppo — es. `722` = Sviluppo software
- **4 cifre**: Classe — es. `7222` = Servizi di programmazione
- **8 cifre**: Codice completo — es. `72224000-1` = Servizi di consulenza per pianificazione sistemi

### Le 10 categorie CPV più usate nella PA italiana (IT):

| Divisione | Descrizione                        | Esempi di acquisto PA              |
|-----------|------------------------------------|------------------------------------|
| **30**    | Macchine da ufficio e informatica  | PC, stampanti, scanner             |
| **45**    | Lavori di costruzione              | Ristrutturazioni, manutenzioni edili |
| **48**    | Pacchetti software                 | ERP, CRM, sistemi gestionali       |
| **50**    | Servizi di riparazione             | Manutenzione hardware              |
| **71**    | Servizi di architettura e ingegneria | Progettazione, perizie            |
| **72**    | Servizi informatici                | Sviluppo web, consulenza IT, cloud |
| **73**    | Ricerca e sviluppo                 | R&D, studi e ricerche              |
| **79**    | Servizi alle imprese               | Consulenza gestionale, legale      |
| **80**    | Istruzione e formazione            | Corsi di formazione, e-learning    |
| **85**    | Servizi sanitari e sociali         | Assistenza domiciliare, servizi medici |

---

## Tipi di procedura di affidamento (cod_tipo_scelta_contraente)

| Codice / Descrizione                     | Soglia massima (2024)  |
|------------------------------------------|------------------------|
| **Affidamento diretto** (art. 50 c.1)    | < €150.000 (servizi)   |
| **Affidamento diretto** (art. 50 c.1 b) | < €150.000 (forniture) |
| **Procedura negoziata senza bando**      | < soglie UE            |
| **Procedura aperta**                     | Senza limiti           |
| **Procedura ristretta**                  | Senza limiti           |
| **Accordo quadro**                       | Qualsiasi importo      |

**Nota:** Le soglie UE per il 2024-2025 sono:
- Forniture e servizi PA centrale: €143.000
- Forniture e servizi altre PA: €221.000
- Lavori: €5.538.000

---

## PNRR e PNC nei dati ANAC

Il campo `flag_pnrr_pnc` nel dataset BandiCIG identifica i contratti finanziati
con fondi del **Piano Nazionale di Ripresa e Resilienza (PNRR)** o del
**Piano Nazionale Complementare (PNC)**.

- Disponibile nei dati dal **2023 in poi**
- Contratti PNRR soggetti a rendicontazione aggiuntiva verso la Corte dei Conti
- Usa il filtro `pnrr_only=True` in `search_contracts()` per isolare questi contratti

---

## API ANAC — Accesso ai Dati

### OCDS API (tempo reale):
- URL base: `https://dati.anticorruzione.it/opendata/ocds/api/`
- Formato: JSON standard OCDS (Open Contracting Data Standard)
- Aggiornamento: continuo
- Nessuna chiave API richiesta

### CSV Bulk Download (mensile):
- Aggiornamento: il 2° giorno di ogni mese
- BandiCIG: `https://dati.anticorruzione.it/opendata/dataset/bandecig`
- SmartCIG: `https://dati.anticorruzione.it/opendata/dataset/cig`
- Aggiudicazioni: `https://dati.anticorruzione.it/opendata/dataset/aggiudicazioni`
- Aggiudicatari: `https://dati.anticorruzione.it/opendata/dataset/aggiudicatari`
"""


def get_nuts_codes() -> str:
    """NUTS2 territorial codes for all 20 Italian regions, used for geographic filtering."""
    return """
# Codici NUTS2 delle Regioni Italiane

Utilizzare questi codici nel parametro `region_nuts` degli strumenti di ricerca.

| Codice NUTS2 | Regione                       | Capoluogo     |
|--------------|-------------------------------|---------------|
| ITC1         | Piemonte                      | Torino        |
| ITC2         | Valle d'Aosta                 | Aosta         |
| ITC3         | Liguria                       | Genova        |
| ITC4         | Lombardia                     | Milano        |
| ITH1         | Provincia Autonoma di Bolzano | Bolzano       |
| ITH2         | Provincia Autonoma di Trento  | Trento        |
| ITH3         | Veneto                        | Venezia       |
| ITH4         | Friuli-Venezia Giulia         | Trieste       |
| ITH5         | Emilia-Romagna                | Bologna       |
| ITI1         | Toscana                       | Firenze       |
| ITI2         | Umbria                        | Perugia       |
| ITI3         | Marche                        | Ancona        |
| ITI4         | Lazio                         | Roma          |
| ITF1         | Abruzzo                       | L'Aquila      |
| ITF2         | Molise                        | Campobasso    |
| ITF3         | Campania                      | Napoli        |
| ITF4         | Puglia                        | Bari          |
| ITF5         | Basilicata                    | Potenza       |
| ITF6         | Calabria                      | Catanzaro     |
| ITG1         | Sicilia                       | Palermo       |
| ITG2         | Sardegna                      | Cagliari      |

## Note sull'uso

- I codici NUTS sono definiti da Eurostat e sono stabili nel tempo
- I dati ANAC usano principalmente il livello NUTS2 (regione)
- Per una ricerca nazionale (tutte le regioni), ometti il parametro `region_nuts`
- Il livello NUTS1 raggruppa le macroregioni:
  - **ITC**: Nord-Ovest (Piemonte, Valle d'Aosta, Liguria, Lombardia)
  - **ITH**: Nord-Est (Trentino, Veneto, Friuli, Emilia-Romagna)
  - **ITI**: Centro (Toscana, Umbria, Marche, Lazio)
  - **ITF**: Sud (Abruzzo, Molise, Campania, Puglia, Basilicata, Calabria)
  - **ITG**: Isole (Sicilia, Sardegna)

## Esempi di utilizzo

```
region_nuts="ITC4"   # Solo Lombardia
region_nuts="ITI4"   # Solo Lazio
region_nuts="ITF3"   # Solo Campania
region_nuts="ITG1"   # Solo Sicilia
```
"""


def get_cpv_categories() -> str:
    """
    CPV (Common Procurement Vocabulary) category reference for Italian PA procurement.
    Lists the most relevant divisions with codes, names, and examples relevant
    to Italian public sector purchasing patterns.
    """
    return """
# Categorie CPV per gli Appalti Pubblici Italiani

Usa il codice a 2 cifre nel parametro `cpv_prefix` per filtrare per categoria.

## Tecnologia dell'Informazione (IT)

| Divisione | Nome (IT)                           | Nome (EN)                        | Esempi tipici PA                                              |
|-----------|-------------------------------------|----------------------------------|---------------------------------------------------------------|
| **30**    | Macchine da ufficio e informatica   | Office machinery & computers     | PC desktop, laptop, stampanti, server, UPS                    |
| **32**    | Apparecchiature radio e telecomunic.| Radio/TV/telecom equipment       | Apparati di rete, switch, router, centralini VoIP             |
| **48**    | Pacchetti software e sistemi inform.| Software packages & info systems | ERP (SAP, Oracle), CRM, software gestionale, licenze OS       |
| **50**    | Servizi di riparazione              | Repair and maintenance services  | Manutenzione HW, assistenza tecnica apparecchiature           |
| **72**    | Servizi informatici                 | IT services                      | Sviluppo applicativi, cloud, cybersecurity, consulenza IT     |
| **73**    | Ricerca e sviluppo                  | Research & development services  | R&D tecnologico, studi di fattibilità informatica             |

### CPV 72 — Servizi IT (dettaglio):

| Codice   | Descrizione                                      |
|----------|--------------------------------------------------|
| 72200000 | Servizi di programmazione e consulenza software  |
| 72212000 | Servizi di programmazione di software applicativo|
| 72220000 | Servizi di consulenza su sistemi e tecnici       |
| 72250000 | Servizi di manutenzione di sistemi               |
| 72310000 | Servizi di elaborazione dati                     |
| 72315000 | Servizi di gestione reti dati                    |
| 72400000 | Servizi Internet                                 |
| 72500000 | Servizi informatici                              |
| 72600000 | Servizi di supporto e consulenza informatica     |
| 72700000 | Servizi di rete informatica                      |

---

## Lavori e Costruzioni

| Divisione | Nome (IT)                           | Esempi tipici PA                                              |
|-----------|-------------------------------------|---------------------------------------------------------------|
| **45**    | Lavori di costruzione               | Ristrutturazioni scuole, ospedali, uffici pubblici            |
| **71**    | Servizi di architettura e ingegneria| Progettazione, direzione lavori, perizie, collaudi            |

---

## Servizi Alle Imprese e Consulenza

| Divisione | Nome (IT)                           | Esempi tipici PA                                              |
|-----------|-------------------------------------|---------------------------------------------------------------|
| **79**    | Servizi alle imprese                | Consulenza gestionale, legale, comunicazione, sicurezza       |

### CPV 79 — Servizi alle imprese (dettaglio):

| Codice   | Descrizione                                      |
|----------|--------------------------------------------------|
| 79100000 | Servizi legali                                   |
| 79200000 | Servizi di contabilità, revisione, fiscali       |
| 79300000 | Ricerche di mercato e sondaggi                   |
| 79400000 | Consulenza aziendale e gestionale                |
| 79500000 | Servizi di segreteria e supporto                 |
| 79700000 | Servizi investigativi e di sicurezza             |
| 79800000 | Stampa e servizi correlati                       |

---

## Servizi Essenziali e Facility Management

| Divisione | Nome (IT)                           | Esempi tipici PA                                              |
|-----------|-------------------------------------|---------------------------------------------------------------|
| **55**    | Servizi alberghieri e ristorazione  | Mense scolastiche, catering ospedali                          |
| **63**    | Servizi di trasporto                | Trasporto scolastico, ambulanze, logistica                    |
| **77**    | Servizi agricoli e forestali        | Manutenzione verde pubblico, parchi                           |
| **90**    | Servizi di fognatura e gestione rif.| Pulizie, raccolta rifiuti, igiene ambientale                  |

### CPV 90 — Servizi ambientali (dettaglio):

| Codice   | Descrizione                                      |
|----------|--------------------------------------------------|
| 90910000 | Servizi di pulizia                               |
| 90911000 | Servizi di pulizia alloggi, edifici, finestre    |
| 90920000 | Servizi di igiene ambientale                     |
| 90600000 | Servizi di pulizia e disinfestazione             |

---

## Salute e Sociale

| Divisione | Nome (IT)                           | Esempi tipici PA                                              |
|-----------|-------------------------------------|---------------------------------------------------------------|
| **33**    | Forniture mediche                   | Dispositivi medici, farmaci, attrezzature sanitarie           |
| **85**    | Servizi sanitari e sociali          | Assistenza domiciliare, RSA, servizi disabilità               |

---

## Formazione

| Divisione | Nome (IT)                           | Esempi tipici PA                                              |
|-----------|-------------------------------------|---------------------------------------------------------------|
| **80**    | Istruzione e formazione             | Corsi formazione personale PA, e-learning, convegni           |

---

## Forniture Generali

| Divisione | Nome (IT)                           | Esempi tipici PA                                              |
|-----------|-------------------------------------|---------------------------------------------------------------|
| **14**    | Prodotti delle miniere e cava       | Materiali edili                                               |
| **22**    | Prodotti editoriali e stampati      | Libri, riviste, materiali didattici                           |
| **34**    | Materiale di trasporto              | Veicoli comunali, automezzi, flotte                           |
| **35**    | Attrezzature di sicurezza           | Dispositivi protezione individuale, antincendio               |
| **39**    | Mobili, arredi                      | Arredamento uffici, scuole, ospedali                          |
| **44**    | Costruzioni e materiali edili       | Materiali per lavori pubblici                                 |

---

## Come usare i codici CPV nelle ricerche

```python
# Cerca solo contratti IT services
search_contracts(keyword="cloud", cpv_prefix="72")

# Cerca software packages
search_contracts(keyword="licenze", cpv_prefix="48")

# Benchmark per categoria pulizie
benchmark_market_prices("servizio pulizie uffici", cpv_prefix="90")

# Profilo IT spending di un ente
get_authority_procurement_profile("Comune di Milano")
# Poi filtra spending_by_cpv per "72" e "48"
```
"""
