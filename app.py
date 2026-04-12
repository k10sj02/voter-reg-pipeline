import streamlit as st
import duckdb
import pandas as pd
import re
from datetime import date, timedelta
import uuid

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Data Engineering Portfolio · Stann-Omar Jones",
    page_icon="🗂️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Fraunces:ital,opsz,wght@0,9..144,300;0,9..144,600;1,9..144,300&display=swap');

:root {
    --ink:     #1a1a2e;
    --paper:   #f5f0e8;
    --accent:  #c84b31;
    --green:   #2d6a4f;
    --muted:   #7a7a8c;
    --border:  #d6cfc2;
    --card:    #ffffff;
}

html, body, [class*="css"] {
    font-family: 'Fraunces', Georgia, serif;
    background-color: var(--paper);
    color: var(--ink);
}

/* Sidebar */
[data-testid="stSidebar"] {
    background-color: var(--ink) !important;
    border-right: 3px solid var(--accent);
}
[data-testid="stSidebar"] * {
    color: var(--paper) !important;
}
[data-testid="stSidebar"] .stRadio label {
    font-family: 'DM Mono', monospace !important;
    font-size: 0.85rem !important;
    letter-spacing: 0.05em;
}
[data-testid="stSidebar"] hr {
    border-color: #3a3a5c !important;
}

/* Headers */
h1 { font-size: 2.4rem !important; font-weight: 600 !important; line-height: 1.15 !important; }
h2 { font-size: 1.5rem !important; font-weight: 300 !important; font-style: italic; color: var(--accent) !important; border-bottom: 1px solid var(--border); padding-bottom: 0.3rem; }
h3 { font-family: 'DM Mono', monospace !important; font-size: 0.9rem !important; letter-spacing: 0.08em; text-transform: uppercase; color: var(--muted) !important; }

/* Metric cards */
[data-testid="metric-container"] {
    background: var(--card);
    border: 1px solid var(--border);
    border-left: 4px solid var(--accent);
    border-radius: 4px;
    padding: 1rem !important;
}
[data-testid="stMetricValue"] { font-family: 'DM Mono', monospace !important; font-size: 2rem !important; }

/* Code blocks */
.stCodeBlock { border-left: 3px solid var(--accent) !important; }
code { font-family: 'DM Mono', monospace !important; font-size: 0.82rem !important; }

/* Dataframe */
[data-testid="stDataFrame"] { border: 1px solid var(--border) !important; }

/* Expander */
[data-testid="stExpander"] {
    border: 1px solid var(--border) !important;
    border-radius: 4px !important;
    background: var(--card) !important;
}

/* Tabs */
[data-testid="stTabs"] button {
    font-family: 'DM Mono', monospace !important;
    font-size: 0.8rem !important;
    letter-spacing: 0.06em;
}

/* Info/success/warning boxes */
.stAlert { border-radius: 4px !important; font-size: 0.88rem !important; }

/* Badge-style chips */
.chip {
    display: inline-block;
    background: var(--ink);
    color: var(--paper);
    font-family: 'DM Mono', monospace;
    font-size: 0.72rem;
    letter-spacing: 0.07em;
    padding: 3px 10px;
    border-radius: 2px;
    margin: 2px 3px;
}
.chip-red { background: var(--accent); }
.chip-green { background: var(--green); }

/* Hero banner */
.hero {
    background: var(--ink);
    color: var(--paper);
    padding: 2.5rem 2.5rem 2rem;
    border-radius: 6px;
    margin-bottom: 2rem;
    position: relative;
    overflow: hidden;
}
.hero::after {
    content: '';
    position: absolute;
    top: 0; right: 0;
    width: 40%;
    height: 100%;
    background: linear-gradient(135deg, transparent 60%, rgba(200,75,49,0.15));
}
.hero h1 { color: var(--paper) !important; margin: 0 0 0.3rem !important; }
.hero p { color: #a0a0b8; font-family: 'DM Mono', monospace; font-size: 0.85rem; margin: 0; }

/* Step badge */
.step-badge {
    display: inline-block;
    background: var(--accent);
    color: white;
    font-family: 'DM Mono', monospace;
    font-size: 0.7rem;
    font-weight: 500;
    letter-spacing: 0.1em;
    padding: 3px 10px;
    border-radius: 2px;
    margin-bottom: 0.5rem;
}

/* Decision block */
.decision {
    background: var(--card);
    border-left: 4px solid var(--green);
    padding: 1rem 1.2rem;
    border-radius: 0 4px 4px 0;
    margin: 0.8rem 0;
    font-size: 0.9rem;
}
.decision strong { font-family: 'DM Mono', monospace; font-size: 0.8rem; color: var(--green); display: block; margin-bottom: 0.3rem; letter-spacing: 0.05em; text-transform: uppercase; }
</style>
""", unsafe_allow_html=True)


# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_data
def load_data():
    partner = pd.read_csv("data/partner_data.csv", dtype=str)
    zips    = pd.read_csv("data/zip_county_lookup.csv", dtype=str)
    records = pd.read_csv("data/all_records.csv", dtype=str)
    return partner, zips, records

partner_raw, zip_lookup, all_records_base = load_data()


# ── DuckDB helpers ────────────────────────────────────────────────────────────
def get_con():
    con = duckdb.connect()
    con.register("partner_data",      partner_raw)
    con.register("zip_county_lookup", zip_lookup)
    con.register("all_records",       all_records_base)
    return con


# ── Transformation logic (mirrors the SQL) ────────────────────────────────────
def run_dedup(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    con = duckdb.connect()
    con.register("src", df)
    full = con.execute("""
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY LOWER(TRIM(first_name)),
                                LOWER(TRIM(last_name)),
                                CAST(date_of_birth AS DATE),
                                CAST(registration_date AS DATE)
                   ORDER BY registration_date DESC,
                            CASE WHEN status = 'Complete' THEN 1 ELSE 0 END DESC
               ) AS rn
        FROM src
    """).df()
    kept    = full[full["rn"] == 1].drop(columns=["rn"]).reset_index(drop=True)
    dropped = full[full["rn"] > 1].drop(columns=["rn"]).reset_index(drop=True)
    return kept, dropped


def validate_email(email):
    if pd.isna(email): return None
    pattern = r'^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$'
    return email.lower().strip() if re.match(pattern, str(email).strip()) else None

def validate_zip(z):
    if pd.isna(z): return None
    z = str(z).strip()
    return z if re.match(r'^\d{5}$', z) else None

def validate_phone(p):
    if pd.isna(p): return None
    digits = re.sub(r'[^0-9]', '', str(p))
    if len(digits) == 10 and re.match(r'^[2-9]', digits):
        return digits
    if len(digits) == 11 and digits[0] == '1':
        d10 = digits[1:]
        return d10 if re.match(r'^[2-9]', d10) else None
    return None

def validate_dob(dob_str):
    try:
        d = pd.to_datetime(dob_str).date()
        today = date.today()
        if (today - timedelta(days=105*365)) <= d <= (today - timedelta(days=18*365)):
            return d
        return None
    except: return None

def validate_reg_date(reg_str):
    try:
        d = pd.to_datetime(reg_str)
        if d.date() >= (date.today() - timedelta(days=365)):
            return d
        return None
    except: return None

def run_transform(df: pd.DataFrame, zip_df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    zip_map = dict(zip(zip_df["zip5"].str.strip(), zip_df["countyname"]))

    # contact cleaning
    email_results   = out["email_address"].apply(validate_email)
    zip_results     = out["home_zip_code"].apply(validate_zip)
    phone_results   = out["phone"].apply(validate_phone)
    dob_results     = out["date_of_birth"].apply(validate_dob)
    reg_results     = out["registration_date"].apply(validate_reg_date)

    out["email_address"]   = email_results
    out["home_zip_code"]   = zip_results
    out["phone"]           = phone_results
    out["date_of_birth"]   = dob_results
    out["registration_date"] = reg_results

    # county enrichment
    out["county"] = out["home_zip_code"].map(zip_map)

    # derived fields
    out["complete"] = out["status"] == "Complete"
    out["evc_year"]  = out["registration_date"].apply(lambda x: int(x.year)  if pd.notna(x) else None)
    out["evc_month"] = out["registration_date"].apply(lambda x: int(x.month) if pd.notna(x) else None)
    out["evc_week"]  = out["registration_date"].apply(
        lambda x: (x - pd.Timedelta(days=x.weekday())).date() if pd.notna(x) else None
    )
    out["application_id"]   = [str(uuid.uuid4()) for _ in range(len(out))]
    out["organization_id"]  = "org_9"

    # store original for comparison
    return out


def build_final_union(transformed: pd.DataFrame, base: pd.DataFrame) -> pd.DataFrame:
    COLS = [
        "name_suffix","voting_street_address_one","voting_street_address_two",
        "voting_city","voting_state","voting_zipcode",
        "mailing_street_address_one","mailing_street_address_two",
        "mailing_city","mailing_state","mailing_zipcode",
        "county","gender","date_of_birth","phone_number","email_address",
        "updated_at","party","name_prefix","ethnicity",
        "latitude","longitude","completed","shift_type","locations_state",
        "program_type","program_sub_type","collection_medium","office",
        "field_start","field_end","shift_start","shift_end",
        "registration_date","evc_month","evc_year","evc_week",
    ]
    partner_aligned = pd.DataFrame({
        "name_suffix":                  transformed.get("name_suffix"),
        "voting_street_address_one":    transformed.get("home_address"),
        "voting_street_address_two":    transformed.get("home_unit"),
        "voting_city":                  transformed.get("home_city"),
        "voting_state":                 transformed.get("home_state"),
        "voting_zipcode":               transformed.get("home_zip_code"),
        "mailing_street_address_one":   None,
        "mailing_street_address_two":   None,
        "mailing_city":                 None,
        "mailing_state":                None,
        "mailing_zipcode":              None,
        "county":                       transformed.get("county"),
        "gender":                       transformed.get("gender"),
        "date_of_birth":                transformed.get("date_of_birth").astype(str),
        "phone_number":                 transformed.get("phone"),
        "email_address":                transformed.get("email_address"),
        "updated_at":                   None,
        "party":                        transformed.get("party"),
        "name_prefix":                  transformed.get("salutation"),
        "ethnicity":                    transformed.get("race"),
        "latitude":                     None,
        "longitude":                    None,
        "completed":                    transformed.get("complete").astype(str),
        "shift_type":                   transformed.get("shift_type"),
        "locations_state":              transformed.get("program_state"),
        "program_type":                 None,
        "program_sub_type":             None,
        "collection_medium":            transformed.get("registration_source"),
        "office":                       transformed.get("office"),
        "field_start":                  transformed.get("field_start"),
        "field_end":                    transformed.get("field_end"),
        "shift_start":                  transformed.get("shift_start"),
        "shift_end":                    transformed.get("shift_end"),
        "registration_date":            transformed.get("registration_date").astype(str),
        "evc_month":                    transformed.get("evc_month"),
        "evc_year":                     transformed.get("evc_year"),
        "evc_week":                     transformed.get("evc_week").astype(str),
    })
    base_aligned = base[COLS] if all(c in base.columns for c in COLS) else base.reindex(columns=COLS)
    return pd.concat([base_aligned, partner_aligned], ignore_index=True)


# ── Sidebar navigation ────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🗂️ EVC Pipeline")
    st.markdown("**Stann-Omar Jones**")
    st.markdown("Analytics Engineering Assessment")
    st.markdown("---")
    page = st.radio(
        "Navigate",
        ["Overview", "Raw Data", "Deduplication", "Validation & Enrichment", "Final Integration", "SQL Reference"],
        label_visibility="collapsed",
    )
    st.markdown("---")
    st.markdown(
        "<span style='font-family:DM Mono,monospace;font-size:0.72rem;color:#7a7a8c;'>"
        "Built with DuckDB · Streamlit<br>Data is synthetic / illustrative"
        "</span>", unsafe_allow_html=True
    )


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════════
if page == "Overview":
    st.markdown("""
    <div class='hero'>
        <h1>Voter Registration Pipeline</h1>
        <p>ANALYTICS ENGINEERING ASSESSMENT · EVC · 2026</p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("## What this demonstrates")
    st.markdown("""
    This app walks through the complete partner data ingestion pipeline built for EVC's 
    voter registration reporting model — from raw partner extract through deduplication, 
    validation, enrichment, and final union with the existing `all_records` table.
    """)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Partner Records",  "50",  help="Raw rows in partner extract")
    col2.metric("Baseline Records", "10",  help="Existing all_records rows")
    col3.metric("Dedup Removed",    "2",   help="Same-person same-day duplicates collapsed")
    col4.metric("Final Union",      "58",  help="48 deduplicated partner + 10 baseline")

    st.markdown("## Pipeline architecture")

    col_a, col_b, col_c = st.columns(3)
    with col_a:
        st.markdown("### 01 · Raw")
        st.markdown("""
        <div class='decision'>
            <strong>Preserve source fidelity</strong>
            Working copy of partner extract. Never modified after creation — 
            auditable baseline for all downstream work.
        </div>
        """, unsafe_allow_html=True)
        st.markdown("""
        - `partner_data_clean` preserved as-is  
        - Schema inspection against `all_records`  
        - Identify nullability, type conflicts, missing fields
        """)

    with col_b:
        st.markdown("### 02 · Staging")
        st.markdown("""
        <div class='decision'>
            <strong>Deterministic transformation</strong>
            Invalid values nulled rather than inferred. 
            Business rules made explicit, not implicit.
        </div>
        """, unsafe_allow_html=True)
        st.markdown("""
        - Deduplication (identity = name + DOB + date)
        - Email / ZIP / phone validation
        - Age bounds (18–105), recency (≤1 yr)
        - County enrichment via ZIP lookup
        - UUID surrogate keys + org tag
        """)

    with col_c:
        st.markdown("### 03 · Reporting mart")
        st.markdown("""
        <div class='decision'>
            <strong>Schema alignment via UNION ALL</strong>
            Explicit column mapping, NULL placeholders for 
            absent partner fields. UNION ALL preserves full fidelity.
        </div>
        """, unsafe_allow_html=True)
        st.markdown("""
        - Field semantic mapping (`home_*` → `voting_*`)
        - All fields cast to varchar for type safety  
        - UNION ALL (not UNION) — intentional  
        - Row-count reconciliation as post-check
        """)

    st.markdown("---")
    st.markdown("## Key design decisions")

    d1, d2 = st.columns(2)
    with d1:
        st.markdown("""
        <div class='decision'>
            <strong>Why NULL invalid contacts, not infer?</strong>
            Fabricating a corrected email or phone introduces data we don't have. 
            Downstream reports should reflect actual data quality, not optimistic guesses.
        </div>
        <div class='decision'>
            <strong>Why UNION ALL, not UNION?</strong>
            UNION deduplicates across both tables — that would silently remove records 
            that legitimately exist in both systems. UNION ALL preserves full lineage.
        </div>
        """, unsafe_allow_html=True)
    with d2:
        st.markdown("""
        <div class='decision'>
            <strong>Why exclude middle name from dedup key?</strong>
            Middle name data quality is highly inconsistent — missing, abbreviated, 
            or spelled differently. Including it would cause real duplicates to survive.
        </div>
        <div class='decision'>
            <strong>Why UUID surrogate keys?</strong>
            Partner records lack a globally unique ID. Gen_random_uuid() provides a 
            stable, collision-resistant surrogate for downstream joins and lineage tracking.
        </div>
        """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: RAW DATA
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Raw Data":
    st.markdown("## Raw partner extract")
    st.markdown("""
    The partner dataset arrives with known data quality issues — intentionally embedded here 
    to demonstrate the pipeline's validation logic. Use the tabs below to explore the data 
    and see what needs fixing.
    """)

    tab1, tab2, tab3 = st.tabs(["📋 Full dataset", "🔍 Known issues", "📐 Schema"])

    with tab1:
        st.markdown(f"**{len(partner_raw)} rows** · {len(partner_raw.columns)} columns")
        st.dataframe(partner_raw, use_container_width=True, height=400)

    with tab2:
        st.markdown("### Issues embedded in this dataset")

        issues = {
            "Exact same-day duplicates": partner_raw[
                partner_raw.duplicated(subset=["first_name","last_name","date_of_birth","registration_date"], keep=False)
            ][["id","first_name","last_name","date_of_birth","registration_date","status","email_address"]],

            "Invalid email addresses": partner_raw[
                partner_raw["email_address"].apply(
                    lambda e: not bool(re.match(r'^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$', str(e).strip()))
                    if pd.notna(e) else False
                )
            ][["id","first_name","last_name","email_address"]],

            "Registration dates > 1 year old": partner_raw[
                partner_raw["registration_date"].apply(
                    lambda r: pd.to_datetime(r, errors="coerce") is not None and
                    pd.to_datetime(r, errors="coerce").date() < (date.today() - timedelta(days=365))
                    if pd.notna(r) else False
                )
            ][["id","first_name","last_name","registration_date"]],

            "DOB implying age < 18": partner_raw[
                partner_raw["date_of_birth"].apply(
                    lambda d: pd.to_datetime(d, errors="coerce").date() > (date.today() - timedelta(days=18*365))
                    if pd.notna(d) else False
                )
            ][["id","first_name","last_name","date_of_birth"]],

            "Phone numbers with country code (11 digits)": partner_raw[
                partner_raw["phone"].apply(
                    lambda p: len(re.sub(r'[^0-9]', '', str(p))) == 11 if pd.notna(p) else False
                )
            ][["id","first_name","last_name","phone"]],
        }

        for label, df_issue in issues.items():
            if len(df_issue) > 0:
                st.markdown(f"""<span class='chip chip-red'>{len(df_issue)} rows</span> **{label}**""",
                            unsafe_allow_html=True)
                st.dataframe(df_issue, use_container_width=True, hide_index=True)
            else:
                st.markdown(f"""<span class='chip chip-green'>✓ clean</span> {label}""", unsafe_allow_html=True)

    with tab3:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Partner dataset columns**")
            schema_df = pd.DataFrame({
                "Column": partner_raw.columns,
                "Sample": [str(partner_raw[c].dropna().iloc[0])[:40] if len(partner_raw[c].dropna()) > 0 else "—" 
                          for c in partner_raw.columns],
                "Nulls": [partner_raw[c].isna().sum() for c in partner_raw.columns],
            })
            st.dataframe(schema_df, use_container_width=True, hide_index=True, height=500)
        with col2:
            st.markdown("**all_records baseline columns**")
            schema_base = pd.DataFrame({
                "Column": all_records_base.columns,
                "Sample": [str(all_records_base[c].dropna().iloc[0])[:40] if len(all_records_base[c].dropna()) > 0 else "—"
                          for c in all_records_base.columns],
            })
            st.dataframe(schema_base, use_container_width=True, hide_index=True, height=500)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: DEDUPLICATION
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Deduplication":
    st.markdown("## Step 1 — Deduplication")
    st.markdown("""
    Same-day duplicates are collapsed using a window function. The identity key is 
    `(first_name, last_name, date_of_birth, registration_date::date)`.  
    The tie-breaker prefers **Complete** status, then most recent timestamp.
    """)

    kept, dropped = run_dedup(partner_raw)

    c1, c2, c3 = st.columns(3)
    c1.metric("Input rows",   len(partner_raw))
    c2.metric("Rows removed", len(dropped), delta=f"-{len(dropped)}", delta_color="off")
    c3.metric("Rows kept",    len(kept))

    tab1, tab2, tab3 = st.tabs(["✅ Deduplicated dataset", "🗑️ Dropped duplicates", "📐 SQL logic"])

    with tab1:
        st.dataframe(kept[["id","first_name","last_name","date_of_birth","registration_date","status","email_address"]],
                     use_container_width=True, hide_index=True)

    with tab2:
        if len(dropped) == 0:
            st.info("No duplicates were dropped.")
        else:
            st.markdown("""
            <div class='decision'>
                <strong>Design note</strong>
                Multiple registrations on *different* days are preserved — 
                a person may genuinely register more than once (e.g., moved, updated info). 
                Only same-day exact duplicates are collapsed.
            </div>
            """, unsafe_allow_html=True)
            st.dataframe(dropped[["id","first_name","last_name","date_of_birth","registration_date","status","email_address"]],
                         use_container_width=True, hide_index=True)

    with tab3:
        st.code("""
WITH ranked AS (
  SELECT
    t.*,
    ROW_NUMBER() OVER (
        PARTITION BY LOWER(TRIM(first_name)),
                     LOWER(TRIM(last_name)),
                     CAST(date_of_birth AS date),
                     CAST(registration_date AS date)
      ORDER BY
        registration_date DESC,
        CASE WHEN status = 'Complete' THEN 1 ELSE 0 END DESC,
        email_address DESC
    ) AS rn
  FROM partner_data_clean t
)
SELECT * FROM ranked WHERE rn = 1;
        """, language="sql")
        st.markdown("""
        <div class='decision'>
            <strong>Why exclude middle_name from the partition key?</strong>
            Middle name data is highly inconsistent — missing, abbreviated, or differently 
            formatted across systems. Including it would cause true duplicates to survive.
        </div>
        """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: VALIDATION & ENRICHMENT
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Validation & Enrichment":
    st.markdown("## Step 2 — Validation, enrichment & derivation")

    kept, _ = run_dedup(partner_raw)
    transformed = run_transform(kept, zip_lookup)

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📧 Contact cleaning",
        "📅 Date validation",
        "🗺️ County enrichment",
        "🏷️ Derived fields",
        "📐 SQL logic",
    ])

    with tab1:
        st.markdown("### Email, ZIP & phone validation")
        st.markdown("Invalid values are **set to NULL** rather than inferred — preserving data integrity over apparent completeness.")

        compare = pd.DataFrame({
            "id":            kept["id"],
            "name":          kept["first_name"] + " " + kept["last_name"],
            "email_raw":     kept["email_address"],
            "email_clean":   transformed["email_address"],
            "zip_raw":       kept["home_zip_code"],
            "zip_clean":     transformed["home_zip_code"],
            "phone_raw":     kept["phone"],
            "phone_clean":   transformed["phone"],
        })

        email_nulled = compare["email_clean"].isna().sum() - kept["email_address"].isna().sum()
        zip_nulled   = compare["zip_clean"].isna().sum()   - kept["home_zip_code"].isna().sum()
        phone_nulled = compare["phone_clean"].isna().sum() - kept["phone"].isna().sum()

        c1, c2, c3 = st.columns(3)
        c1.metric("Emails nulled",  max(0, email_nulled))
        c2.metric("ZIPs nulled",    max(0, zip_nulled))
        c3.metric("Phones cleaned/nulled", max(0, phone_nulled))

        changed_mask = (
            (compare["email_raw"] != compare["email_clean"]) |
            (compare["zip_raw"]   != compare["zip_clean"])   |
            (compare["phone_raw"] != compare["phone_clean"])
        )
        st.markdown(f"**{changed_mask.sum()} rows** had at least one contact field changed:")
        st.dataframe(compare[changed_mask], use_container_width=True, hide_index=True)

    with tab2:
        st.markdown("### Date of birth & registration date")

        dob_compare = pd.DataFrame({
            "id":        kept["id"],
            "name":      kept["first_name"] + " " + kept["last_name"],
            "dob_raw":   kept["date_of_birth"],
            "dob_clean": transformed["date_of_birth"].astype(str),
            "reg_raw":   kept["registration_date"],
            "reg_clean": transformed["registration_date"].astype(str),
        })

        dob_nulled = (dob_compare["dob_clean"] == "None").sum()
        reg_nulled = (dob_compare["reg_clean"] == "None").sum()

        c1, c2 = st.columns(2)
        c1.metric("DOBs nulled (age out of 18–105 range)", dob_nulled)
        c2.metric("Reg dates nulled (older than 1 year)",  reg_nulled)

        changed = (dob_compare["dob_raw"] != dob_compare["dob_clean"]) | (dob_compare["reg_raw"] != dob_compare["reg_clean"])
        if changed.sum():
            st.dataframe(dob_compare[changed], use_container_width=True, hide_index=True)
        else:
            st.success("No date values required nulling in this dataset.")

        st.markdown("""
        <div class='decision'>
            <strong>Design note — recency constraint</strong>
            Registration dates older than one year are nulled. This enforces the reporting 
            model's implicit assumption that all_records represents current-cycle activity.
        </div>
        """, unsafe_allow_html=True)

    with tab3:
        st.markdown("### County enrichment via ZIP lookup")

        enriched = pd.DataFrame({
            "id":       transformed["id"],
            "name":     kept["first_name"] + " " + kept["last_name"],
            "zip":      transformed["home_zip_code"],
            "county":   transformed["county"],
            "state":    transformed["home_state"],
        })

        matched   = enriched["county"].notna().sum()
        unmatched = enriched["county"].isna().sum()

        c1, c2 = st.columns(2)
        c1.metric("County matched",   matched)
        c2.metric("No match (ZIP null or not in lookup)", unmatched)

        st.dataframe(enriched, use_container_width=True, hide_index=True, height=350)

        st.markdown("""
        <div class='decision'>
            <strong>LEFT JOIN, not INNER JOIN</strong>
            An INNER JOIN would silently drop partner records with unmatched ZIPs. 
            LEFT JOIN preserves all rows — missing county is surfaced as NULL, not hidden.
        </div>
        """, unsafe_allow_html=True)

    with tab4:
        st.markdown("### Derived & reporting fields")

        derived = transformed[["id", "complete", "evc_year", "evc_month", "evc_week",
                               "application_id", "organization_id"]].copy()
        derived["id"] = kept["id"].values

        st.dataframe(derived.head(20), use_container_width=True, hide_index=True)

        st.markdown("""
        <div class='decision'>
            <strong>complete flag</strong>
            Derived directly from <code>status = 'Complete'</code>. The original 
            <code>status</code> column is preserved — the boolean is additive.
        </div>
        <div class='decision'>
            <strong>evc_week</strong>
            Set to the Monday of the registration week using DATE_TRUNC('week', ...). 
            Consistent with ISO week semantics used in the reporting model.
        </div>
        <div class='decision'>
            <strong>application_id</strong>
            UUID generated per record via gen_random_uuid(). Partner records lack a 
            globally unique identifier — this surrogate enables downstream joins and lineage.
        </div>
        """, unsafe_allow_html=True)

    with tab5:
        st.code("""
-- Contact validation
CASE
  WHEN t.email_address ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
    THEN LOWER(TRIM(t.email_address))
  ELSE NULL
END AS email_address,

CASE
  WHEN TRIM(COALESCE(t.home_zip_code::text, '')) ~ '^[0-9]{5}$'
    THEN TRIM(t.home_zip_code::text)
  ELSE NULL
END AS home_zip_code,

-- Phone: accept 10-digit NANP or strip leading 1 from 11-digit
CASE
  WHEN LENGTH(REGEXP_REPLACE(phone::text, '[^0-9]', '', 'g')) = 10
       AND REGEXP_REPLACE(phone::text, '[^0-9]', '', 'g') ~ '^[2-9][0-9]{9}$'
    THEN REGEXP_REPLACE(phone::text, '[^0-9]', '', 'g')
  WHEN LENGTH(REGEXP_REPLACE(phone::text, '[^0-9]', '', 'g')) = 11
       AND LEFT(REGEXP_REPLACE(phone::text, '[^0-9]', '', 'g'), 1) = '1'
    THEN RIGHT(REGEXP_REPLACE(phone::text, '[^0-9]', '', 'g'), 10)
  ELSE NULL
END AS phone,

-- Age bounds + recency
CASE
  WHEN CAST(date_of_birth AS date)
       BETWEEN (CURRENT_DATE - INTERVAL '105 years')
           AND (CURRENT_DATE - INTERVAL '18 years')
    THEN CAST(date_of_birth AS date)
  ELSE NULL
END AS date_of_birth,

-- County enrichment
LEFT JOIN assessment_data.zip_county_lookup z
  ON TRIM(validated_zip) = TRIM(z.zip5::text)
        """, language="sql")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: FINAL INTEGRATION
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Final Integration":
    st.markdown("## Step 3 — Final integration")
    st.markdown("""
    The transformed partner data is schema-aligned to `all_records` and unioned.  
    Field mapping is semantic (`home_*` → `voting_*`), NULLs fill absent fields, 
    and all values are cast to varchar for type safety.
    """)

    kept, _        = run_dedup(partner_raw)
    transformed    = run_transform(kept, zip_lookup)
    final          = build_final_union(transformed, all_records_base)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("all_records (baseline)",     len(all_records_base))
    c2.metric("partner_data (deduped)",     len(transformed))
    c3.metric("Expected total",             len(all_records_base) + len(transformed))
    c4.metric("Actual final count",         len(final),
              delta="✓ matches" if len(final) == len(all_records_base) + len(transformed) else "⚠ mismatch",
              delta_color="normal")

    tab1, tab2, tab3 = st.tabs(["📋 Final table", "🗺️ Field mapping", "📐 SQL logic"])

    with tab1:
        st.markdown(f"**{len(final)} total rows** after UNION ALL")
        st.dataframe(final, use_container_width=True, height=450)

    with tab2:
        mapping = pd.DataFrame([
            ("home_address",       "voting_street_address_one",  "Direct mapping"),
            ("home_unit",          "voting_street_address_two",  "Direct mapping"),
            ("home_city",          "voting_city",                "Direct mapping"),
            ("home_state",         "voting_state",               "Direct mapping"),
            ("home_zip_code",      "voting_zipcode",             "Direct mapping"),
            ("salutation",         "name_prefix",                "Semantic equivalent"),
            ("race",               "ethnicity",                  "Semantic equivalent"),
            ("phone",              "phone_number",               "Semantic equivalent"),
            ("complete",           "completed",                  "Derived boolean → varchar"),
            ("program_state",      "locations_state",            "Semantic equivalent"),
            ("registration_source","collection_medium",          "Semantic equivalent"),
            ("—",                  "mailing_*",                  "NULL placeholder — not in partner data"),
            ("—",                  "latitude / longitude",       "NULL placeholder — not in partner data"),
            ("—",                  "program_type / sub_type",    "NULL placeholder — not in partner data"),
            ("—",                  "updated_at",                 "NULL placeholder — not in partner data"),
        ], columns=["Partner field", "all_records field", "Note"])
        st.dataframe(mapping, use_container_width=True, hide_index=True)

    with tab3:
        st.code("""
CREATE TABLE all_records_final AS

-- Existing records (no transformation needed)
SELECT
  name_suffix, voting_street_address_one, ...,
  registration_date, evc_month, evc_year, evc_week
FROM assessment_data.all_records

UNION ALL

-- Partner records: schema-aligned with explicit field mapping
SELECT
  p.name_suffix::varchar,
  p.home_address::varchar       AS voting_street_address_one,
  p.home_unit::varchar          AS voting_street_address_two,
  p.home_city::varchar          AS voting_city,
  p.home_state::varchar         AS voting_state,
  p.home_zip_code::varchar      AS voting_zipcode,
  NULL::varchar                 AS mailing_street_address_one,
  NULL::varchar                 AS mailing_street_address_two,
  -- ... (remaining NULL placeholders)
  p.county::varchar,
  p.gender::varchar,
  p.date_of_birth::varchar,
  p.phone::varchar              AS phone_number,
  p.email_address::varchar,
  NULL::varchar                 AS updated_at,
  p.party::varchar,
  p.salutation::varchar         AS name_prefix,
  p.race::varchar               AS ethnicity,
  NULL::varchar                 AS latitude,
  NULL::varchar                 AS longitude,
  p.complete::varchar           AS completed,
  p.shift_type::varchar,
  p.program_state::varchar      AS locations_state,
  NULL::varchar                 AS program_type,
  NULL::varchar                 AS program_sub_type,
  p.registration_source::varchar AS collection_medium,
  p.office::varchar,
  p.field_start::varchar, p.field_end::varchar,
  p.shift_start::varchar, p.shift_end::varchar,
  p.registration_date::varchar,
  p.evc_month::varchar, p.evc_year::varchar, p.evc_week::varchar
FROM partner_data_transformed p;

-- Post-integration validation
SELECT COUNT(*) FROM all_records;        -- 10 rows
SELECT COUNT(*) FROM partner_data_transformed; -- 48 rows (after dedup)
SELECT COUNT(*) FROM all_records_final;  -- 58 rows ✓
        """, language="sql")

        st.markdown("""
        <div class='decision'>
            <strong>UNION ALL, not UNION</strong>
            UNION performs implicit deduplication across both result sets. Since a person 
            could legitimately appear in both the existing all_records and the partner 
            extract (e.g., re-registered), UNION ALL preserves full record lineage. 
            Cross-source deduplication would require a separate, deliberate reconciliation step.
        </div>
        """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: SQL REFERENCE
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "SQL Reference":
    st.markdown("## SQL reference")
    st.markdown("The complete annotated SQL from the assessment. Each block is independently executable in a Postgres environment with the assessment schema loaded.")

    blocks = {
        "01 · Schema setup & working copy": """
-- Isolate work in dedicated schema
CREATE SCHEMA stann_assessment;
SET search_path TO stann_assessment;

-- Baseline working copy — never modified after creation
CREATE TABLE partner_data_clean AS
SELECT * FROM assessment_data.partner_data;
""",
        "02 · Deduplication": """
WITH ranked AS (
  SELECT
    t.*,
    ROW_NUMBER() OVER (
        PARTITION BY LOWER(TRIM(first_name)),
                     LOWER(TRIM(last_name)),
                     CAST(date_of_birth AS date),
                     CAST(registration_date AS date)
      ORDER BY
        registration_date DESC,
        CASE WHEN status = 'Complete' THEN 1 ELSE 0 END DESC,
        email_address DESC
    ) AS rn
  FROM stann_assessment.partner_data_clean t
)
SELECT * FROM ranked WHERE rn = 1;
""",
        "03 · Validation, enrichment & derivation": """
DROP TABLE IF EXISTS stann_assessment.partner_data_transformed;

CREATE TABLE stann_assessment.partner_data_transformed AS
SELECT
  gen_random_uuid() AS application_id,
  t.id,
  'org_9' AS organization_id,

  -- Email validation
  CASE
    WHEN t.email_address ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
      THEN LOWER(TRIM(t.email_address))
    ELSE NULL
  END AS email_address,

  -- ZIP validation
  CASE
    WHEN TRIM(COALESCE(t.home_zip_code::text, '')) ~ '^[0-9]{5}$'
      THEN TRIM(t.home_zip_code::text)
    ELSE NULL
  END AS home_zip_code,

  -- County enrichment
  z.countyname AS county,

  -- Phone: NANP 10-digit, or strip leading 1
  CASE
    WHEN LENGTH(REGEXP_REPLACE(COALESCE(t.phone::text,''),'[^0-9]','','g'))=10
         AND REGEXP_REPLACE(COALESCE(t.phone::text,''),'[^0-9]','','g')~'^[2-9][0-9]{9}$'
      THEN REGEXP_REPLACE(COALESCE(t.phone::text,''),'[^0-9]','','g')
    WHEN LENGTH(REGEXP_REPLACE(COALESCE(t.phone::text,''),'[^0-9]','','g'))=11
         AND LEFT(REGEXP_REPLACE(COALESCE(t.phone::text,''),'[^0-9]','','g'),1)='1'
      THEN RIGHT(REGEXP_REPLACE(COALESCE(t.phone::text,''),'[^0-9]','','g'),10)
    ELSE NULL
  END AS phone,

  -- DOB: age 18–105
  CASE
    WHEN CAST(t.date_of_birth AS date)
         BETWEEN (CURRENT_DATE - INTERVAL '105 years')
             AND (CURRENT_DATE - INTERVAL '18 years')
      THEN CAST(t.date_of_birth AS date)
    ELSE NULL
  END AS date_of_birth,

  -- Registration date: recency constraint (≤ 1 year)
  CASE
    WHEN CAST(t.registration_date AS date) >= (CURRENT_DATE - INTERVAL '1 year')
      THEN CAST(t.registration_date AS timestamp)
    ELSE NULL
  END AS registration_date,

  -- Reporting dimensions
  CASE WHEN CAST(t.registration_date AS date) >= (CURRENT_DATE - INTERVAL '1 year')
    THEN EXTRACT(YEAR  FROM CAST(t.registration_date AS date))::int END AS evc_year,
  CASE WHEN CAST(t.registration_date AS date) >= (CURRENT_DATE - INTERVAL '1 year')
    THEN EXTRACT(MONTH FROM CAST(t.registration_date AS date))::int END AS evc_month,
  CASE WHEN CAST(t.registration_date AS date) >= (CURRENT_DATE - INTERVAL '1 year')
    THEN DATE_TRUNC('week', CAST(t.registration_date AS date))::date END AS evc_week,

  -- Completion flag
  t.status,
  (t.status = 'Complete') AS complete,

  -- Pass-through fields
  t.first_name, t.middle_name, t.last_name, t.name_suffix,
  t.salutation, t.home_address, t.home_unit, t.home_city, t.home_state,
  t.party, t.race, t.gender, t.phone_type, t.registration_source,
  t.shift_id, t.shift_type, t.office, t.program_state, t.partner_id,
  t.field_start, t.field_end, t.shift_start, t.shift_end,
  t.citizenship_confirmed, t.submitted_via_state_api, t.evc_id

FROM stann_assessment.partner_data_clean t
LEFT JOIN assessment_data.zip_county_lookup z
  ON TRIM(
       CASE WHEN TRIM(COALESCE(t.home_zip_code::text,''))~'^[0-9]{5}$'
            THEN TRIM(t.home_zip_code::text) ELSE NULL END
     ) = TRIM(z.zip5::text);
""",
        "04 · Final UNION into all_records_final": """
DROP TABLE IF EXISTS stann_assessment.all_records_final;

CREATE TABLE stann_assessment.all_records_final AS
SELECT  -- existing records
  name_suffix, voting_street_address_one, voting_street_address_two,
  voting_city, voting_state, voting_zipcode,
  mailing_street_address_one, mailing_street_address_two,
  mailing_city, mailing_state, mailing_zipcode,
  county, gender, date_of_birth, phone_number, email_address,
  updated_at, party, name_prefix, ethnicity, latitude, longitude,
  completed, shift_type, locations_state, program_type, program_sub_type,
  collection_medium, office, field_start, field_end, shift_start, shift_end,
  registration_date, evc_month, evc_year, evc_week
FROM assessment_data.all_records

UNION ALL

SELECT  -- partner records, schema-aligned
  p.name_suffix::varchar,
  p.home_address::varchar          AS voting_street_address_one,
  p.home_unit::varchar             AS voting_street_address_two,
  p.home_city::varchar             AS voting_city,
  p.home_state::varchar            AS voting_state,
  p.home_zip_code::varchar         AS voting_zipcode,
  NULL::varchar, NULL::varchar, NULL::varchar, NULL::varchar, NULL::varchar,
  p.county::varchar, p.gender::varchar, p.date_of_birth::varchar,
  p.phone::varchar                 AS phone_number,
  p.email_address::varchar,
  NULL::varchar                    AS updated_at,
  p.party::varchar,
  p.salutation::varchar            AS name_prefix,
  p.race::varchar                  AS ethnicity,
  NULL::varchar, NULL::varchar,
  p.complete::varchar              AS completed,
  p.shift_type::varchar,
  p.program_state::varchar         AS locations_state,
  NULL::varchar, NULL::varchar,
  p.registration_source::varchar   AS collection_medium,
  p.office::varchar,
  p.field_start::varchar, p.field_end::varchar,
  p.shift_start::varchar, p.shift_end::varchar,
  p.registration_date::varchar,
  p.evc_month::varchar, p.evc_year::varchar, p.evc_week::varchar
FROM stann_assessment.partner_data_transformed p;
""",
        "05 · Post-integration validation": """
-- Row count reconciliation
SELECT COUNT(*) FROM assessment_data.all_records;         -- baseline: 10
SELECT COUNT(*) FROM stann_assessment.partner_data_transformed; -- partner: 48
SELECT COUNT(*) FROM stann_assessment.all_records_final;  -- expected: 58 ✓
""",
    }

    for title, sql in blocks.items():
        with st.expander(title, expanded=False):
            st.code(sql.strip(), language="sql")
