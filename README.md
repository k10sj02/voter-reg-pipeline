# Voter Registration Pipeline

**Stann-Omar Jones · Analytics Engineering · Portfolio**

An interactive Streamlit app demonstrating a production-style partner data ingestion pipeline for voter registration reporting — from raw extract through deduplication, validation, enrichment, and final union with an existing reporting table. All data is synthetic and illustrative.

## What it demonstrates

- Layered pipeline architecture (raw → staging → mart)
- Window-function deduplication with an explicit tie-breaking strategy
- Contact validation (email regex, ZIP format, NANP phone normalization)
- Demographic & temporal validation (age bounds, recency constraints)
- County enrichment via ZIP lookup (LEFT JOIN, not INNER)
- UUID surrogate key generation and org tagging
- Schema-aligned UNION ALL integration with NULL placeholders
- Post-integration row-count reconciliation
- Annotated SQL for every step

## Quick start

```bash
uv sync
uv run streamlit run app.py
```

Or with pip:

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Project structure

```
voter-reg-pipeline/
├── app.py                      # Main Streamlit application
├── pyproject.toml              # Dependencies (uv)
├── requirements.txt            # Fallback pip dependencies
├── render.yaml                 # Render deployment config
├── data/
│   ├── partner_data.csv        # Synthetic partner extract (50 rows)
│   ├── zip_county_lookup.csv   # ZIP → county reference table
│   └── all_records.csv         # Synthetic baseline reporting table (10 rows)
└── README.md
```

## Key design decisions

| Decision | Rationale |
|---|---|
| NULL invalid contacts, don't infer | Fabricating corrections introduces data we don't have |
| Exclude middle_name from dedup key | Inconsistent data quality causes false non-matches |
| LEFT JOIN for county enrichment | Preserves all rows; missing county visible as NULL |
| UNION ALL, not UNION | Prevents silent cross-source deduplication |
| UUID surrogate keys | Partner records lack globally unique IDs |
| Recency constraint on reg_date | Enforces reporting model's current-cycle assumption |

## Deployment

Deployed on [Render](https://render.com) via `render.yaml`. To deploy your own instance:

1. Push to a public GitHub repo
2. Connect the repo in the Render dashboard
3. Render will detect `render.yaml` and configure automatically
