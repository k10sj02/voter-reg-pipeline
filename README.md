# Voter Registration Pipeline · Portfolio Demo

**Stann-Omar Jones — Analytics Engineering Assessment (EVC)**

An interactive Streamlit app that walks through a complete partner data ingestion pipeline —
from raw extract through deduplication, validation, enrichment, and final union with an
existing reporting table. All data is synthetic.

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
pip install -r requirements.txt
streamlit run app.py
```

## Project structure

```
evc_portfolio/
├── app.py                  # Main Streamlit application
├── requirements.txt
├── data/
│   ├── partner_data.csv    # Synthetic partner extract (50 rows, intentional issues)
│   ├── zip_county_lookup.csv
│   └── all_records.csv     # Synthetic baseline reporting table (10 rows)
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

Deploy to [Streamlit Community Cloud](https://streamlit.io/cloud) for free:
1. Push to a public GitHub repo
2. Connect at share.streamlit.io
3. Set main file to `app.py`
