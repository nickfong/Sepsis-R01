#!/usr/bin/env python3
"""
Build per-encounter CCSR (Clinical Classifications Software Refined) features.

Uses AHRQ CCSR for ICD-10-CM Diagnoses v2026.1 — parsed directly from the
official CSV mapping file.

Primary source:
  - ccsr_v2026_1/DXCCSR_v2026-1.csv: ICD-10-CM → CCSR category mapping
    Downloaded from https://hcup-us.ahrq.gov/toolssoftware/ccsr/dxccsr.jsp

CCSR maps ALL ~75,000 ICD-10-CM codes into 553 clinically meaningful categories
across 22 body systems (3-letter prefix + 3-digit number, e.g., CIR008 =
Hypertension with complications). A single ICD code can map to up to 6
categories.

Produces two variants:
  - ccsr_all_*: All ICD-10-CM codes on encounter
  - ccsr_poa_*: Only codes with presentonadmission='Yes' or
    type IN ('Medical History', 'Problem List')

No weighted scores — CCSR is a clinical grouper, not a comorbidity index.

Output: Filtered_Combined_2026-02-20/ccsr_features.parquet (+ .csv)
"""

import csv
import os
import time

import duckdb
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CCSR_CSV = os.path.join(SCRIPT_DIR, "ccsr_v2026_1", "DXCCSR_v2026-1.csv")
COHORT_FILE = os.path.join(SCRIPT_DIR, "Filtered_Combined_2026-02-20", "filtered_cohort_combined.parquet")
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "Filtered_Combined_2026-02-20")

UCSF_DX = "/media/ubuntu/HDD Storage/parquet/deid_cdw/diagnosiseventfact/*.parquet"
UCSF_DIM = "/media/ubuntu/HDD Storage/parquet/deid_cdw/diagnosisterminologydim/*.parquet"
SFDPH_DX = "/media/ubuntu/HDD Storage/parquet/deid_cdw_sfdph/diagnosiseventfact/*.parquet"
SFDPH_DIM = "/media/ubuntu/HDD Storage/parquet/deid_cdw_sfdph/diagnosisterminologydim/*.parquet"

# 22 CCSR body systems
BODY_SYSTEMS = {
    "BLD": "Blood", "CIR": "Circulatory", "DEN": "Dental", "DIG": "Digestive",
    "EAR": "Ear", "END": "Endocrine", "EXT": "External Causes",
    "EYE": "Eye", "FAC": "Factors Influencing Health",
    "GEN": "Genitourinary", "INF": "Infectious", "INJ": "Injury",
    "MAL": "Malformations", "MBD": "Mental/Behavioral",
    "MUS": "Musculoskeletal", "NEO": "Neoplasms", "NVS": "Nervous System",
    "PNL": "Perinatal", "PRG": "Pregnancy", "RSP": "Respiratory",
    "SKN": "Skin", "SYM": "Symptoms/Signs",
}


def load_ccsr_mapping(csv_path):
    """Load AHRQ CCSR ICD-10-CM → category mapping from CSV.

    The CSV uses mixed quoting (single quotes for codes, double quotes for
    descriptions containing commas). Python's csv.reader handles this.

    Returns:
        mapping_df: DataFrame[icd_code, ccsr_category, ccsr_description]
            Long format — one row per (ICD code, CCSR category) pair.
        labels: dict[str, str] — CCSR category → description
    """
    rows = []
    with open(csv_path, "r") as f:
        reader = csv.reader(f)
        header = [h.strip("' ") for h in next(reader)]
        for row in reader:
            rows.append(row)

    raw_df = pd.DataFrame(rows, columns=header)

    # Strip single quotes from code columns
    code_cols = ["ICD-10-CM CODE", "Default CCSR CATEGORY IP", "Default CCSR CATEGORY OP"]
    for i in range(1, 7):
        code_cols.append(f"CCSR CATEGORY {i}")
    for col in code_cols:
        raw_df[col] = raw_df[col].str.strip("' ")

    # Melt category columns 1-6 into long format (vectorized)
    pieces = []
    for i in range(1, 7):
        cat_col = f"CCSR CATEGORY {i}"
        desc_col = f"CCSR CATEGORY {i} DESCRIPTION"
        mask = raw_df[cat_col] != ""
        subset = raw_df.loc[mask, ["ICD-10-CM CODE", cat_col, desc_col]].rename(
            columns={"ICD-10-CM CODE": "icd_code", cat_col: "ccsr_category", desc_col: "ccsr_description"}
        )
        pieces.append(subset)

    long_df = pd.concat(pieces, ignore_index=True)

    # Build labels dict from deduplicated categories
    label_df = long_df[["ccsr_category", "ccsr_description"]].drop_duplicates(subset="ccsr_category")
    labels = dict(zip(label_df["ccsr_category"], label_df["ccsr_description"]))

    # Mapping only needs icd_code + ccsr_category
    mapping_df = long_df[["icd_code", "ccsr_category"]].drop_duplicates()

    return mapping_df, labels


def main():
    t0 = time.time()
    print("=" * 70, flush=True)
    print("Building CCSR (Clinical Classifications Software Refined) features", flush=True)
    print("  AHRQ CCSR for ICD-10-CM v2026.1 (553 categories, 22 body systems)", flush=True)
    print("  Two variants: all-dx + POA-aware", flush=True)
    print("=" * 70, flush=True)

    # --- Load CCSR mapping from CSV ---
    print("\nLoading CCSR mapping from CSV...", flush=True)
    ccsr_map_df, labels = load_ccsr_mapping(CCSR_CSV)
    n_icd = ccsr_map_df["icd_code"].nunique()
    n_cat = ccsr_map_df["ccsr_category"].nunique()
    print(f"  {n_icd:,} ICD-10-CM codes → {n_cat} CCSR categories", flush=True)
    print(f"  {len(ccsr_map_df):,} total ICD-to-category mappings", flush=True)
    print(f"  Body systems: {len(set(c[:3] for c in ccsr_map_df['ccsr_category'].unique()))}", flush=True)

    # --- Connect DuckDB ---
    con = duckdb.connect()
    con.execute('SET memory_limit="200GB"')
    con.execute("SET threads=24")

    # Load cohort
    con.execute(f"CREATE VIEW cohort AS SELECT DISTINCT encounterkey FROM read_parquet('{COHORT_FILE}')")
    cohort_n = con.execute("SELECT COUNT(*) FROM cohort").fetchone()[0]
    print(f"\nCohort encounters: {cohort_n:,}", flush=True)

    # --- Query ALL diagnosis records for cohort encounters ---
    print("\nQuerying diagnosis data (both sites)...", flush=True)
    t1 = time.time()

    query = """
    SELECT DISTINCT
        def.encounterkey,
        REPLACE(REPLACE(dtd.value, '.', ''), ' ', '') AS icd_code,
        def.presentonadmission,
        def.type
    FROM (
        SELECT encounterkey, diagnosiskey, presentonadmission, type
        FROM read_parquet($ucsf_dx)
        UNION ALL
        SELECT encounterkey, diagnosiskey, presentonadmission, type
        FROM read_parquet($sfdph_dx)
    ) def
    INNER JOIN cohort c ON def.encounterkey = c.encounterkey
    INNER JOIN (
        SELECT diagnosiskey, value, type AS dim_type
        FROM read_parquet($ucsf_dim)
        WHERE type = 'ICD-10-CM'
        UNION ALL
        SELECT diagnosiskey, value, type AS dim_type
        FROM read_parquet($sfdph_dim)
        WHERE type = 'ICD-10-CM'
    ) dtd ON def.diagnosiskey = dtd.diagnosiskey
    """

    dx_df = con.execute(query, {
        "ucsf_dx": UCSF_DX,
        "sfdph_dx": SFDPH_DX,
        "ucsf_dim": UCSF_DIM,
        "sfdph_dim": SFDPH_DIM,
    }).fetchdf()

    print(f"Query complete in {time.time() - t1:.1f}s", flush=True)
    print(f"Total diagnosis records: {len(dx_df):,}", flush=True)
    print(f"Unique encounters: {dx_df['encounterkey'].nunique():,}", flush=True)
    print(f"Unique ICD-10-CM codes: {dx_df['icd_code'].nunique():,}", flush=True)

    # Normalize ICD codes
    dx_df["icd_code"] = dx_df["icd_code"].str.upper().str.strip()

    # Check CCSR coverage
    unique_codes = set(dx_df["icd_code"].unique())
    mapped_codes = unique_codes & set(ccsr_map_df["icd_code"].unique())
    print(f"ICD codes with CCSR mapping: {len(mapped_codes):,}/{len(unique_codes):,}"
          f" ({len(mapped_codes)/len(unique_codes):.1%})", flush=True)

    # --- Determine POA eligibility per record (vectorized) ---
    dx_df["poa_qualified"] = (
        (dx_df["presentonadmission"].str.lower() == "yes")
        | (dx_df["type"].isin(["Medical History", "Problem List"]))
    )

    # =====================================================================
    # CCSR MAPPING — ALL-DX + POA variants
    # =====================================================================

    # Merge diagnosis records with CCSR mapping on ICD code
    print("\n--- CCSR Mapping ---", flush=True)
    t2 = time.time()

    matched = dx_df.merge(ccsr_map_df, on="icd_code", how="inner")
    print(f"  Matched records: {len(matched):,}", flush=True)
    print(f"  Encounters with >=1 CCSR match: {matched['encounterkey'].nunique():,}", flush=True)

    # --- ALL-DX variant (no POA filter) ---
    print("\n  ALL-DX variant:", flush=True)
    all_long = matched[["encounterkey", "ccsr_category"]].drop_duplicates()
    print(f"    Encounter-category pairs: {len(all_long):,}", flush=True)
    print(f"    Active categories: {all_long['ccsr_category'].nunique()}", flush=True)

    all_long = all_long.copy()
    all_long["value"] = 1
    all_wide = all_long.pivot_table(
        index="encounterkey", columns="ccsr_category", values="value",
        aggfunc="max", fill_value=0,
    )
    all_cats = sorted(all_wide.columns)
    all_wide.columns = [f"ccsr_all_{c.lower()}" for c in all_wide.columns]
    all_wide = all_wide.reset_index()

    # --- POA variant (strict: all categories require POA evidence) ---
    print("\n  POA variant:", flush=True)
    poa_matched = matched[matched["poa_qualified"]]
    poa_long = poa_matched[["encounterkey", "ccsr_category"]].drop_duplicates()
    print(f"    Encounter-category pairs: {len(poa_long):,}", flush=True)
    print(f"    Active categories: {poa_long['ccsr_category'].nunique()}", flush=True)

    poa_long = poa_long.copy()
    poa_long["value"] = 1
    poa_wide = poa_long.pivot_table(
        index="encounterkey", columns="ccsr_category", values="value",
        aggfunc="max", fill_value=0,
    )
    poa_wide.columns = [f"ccsr_poa_{c.lower()}" for c in poa_wide.columns]
    poa_wide = poa_wide.reset_index()

    print(f"\n  Mapping time: {time.time() - t2:.1f}s", flush=True)

    # =====================================================================
    # MERGE AND COMPUTE SUMMARY SCORES
    # =====================================================================
    print("\n--- Merging and computing summary scores ---", flush=True)

    all_encounters = con.execute("SELECT encounterkey FROM cohort").fetchdf()

    result = all_encounters
    result = result.merge(all_wide, on="encounterkey", how="left")
    result = result.merge(poa_wide, on="encounterkey", how="left")

    # Fill NaN with 0 and cast to int8
    feat_cols = [c for c in result.columns if c.startswith("ccsr_")]
    result[feat_cols] = result[feat_cols].fillna(0).astype("int8")

    # Ensure all categories from ALL-DX exist in POA (may be absent if no POA encounters)
    for cat in all_cats:
        poa_col = f"ccsr_poa_{cat.lower()}"
        if poa_col not in result.columns:
            result[poa_col] = pd.array([0] * len(result), dtype="int8")

    # Category counts
    all_cat_cols = sorted([c for c in result.columns if c.startswith("ccsr_all_")])
    poa_cat_cols = sorted([c for c in result.columns if c.startswith("ccsr_poa_")])
    result["ccsr_all_count"] = result[all_cat_cols].sum(axis=1).astype("int16")
    result["ccsr_poa_count"] = result[poa_cat_cols].sum(axis=1).astype("int16")

    # Body-system counts (number of distinct 3-letter prefixes with >=1 category)
    all_systems = sorted(set(c[:3] for c in all_cats))
    for prefix_label, cat_cols_list, sys_col in [
        ("ccsr_all_", all_cat_cols, "ccsr_all_sys_count"),
        ("ccsr_poa_", poa_cat_cols, "ccsr_poa_sys_count"),
    ]:
        sys_counts = pd.Series(0, index=result.index, dtype="int8")
        for sys_prefix in all_systems:
            sys_specific = [c for c in cat_cols_list if c[len(prefix_label):][:3].upper() == sys_prefix]
            if sys_specific:
                sys_counts += (result[sys_specific].sum(axis=1) > 0).astype("int8")
        result[sys_col] = sys_counts

    # =====================================================================
    # SUMMARY
    # =====================================================================
    print(f"\n{'=' * 70}", flush=True)
    print("RESULTS", flush=True)
    print(f"{'=' * 70}", flush=True)
    print(f"Total encounters: {len(result):,}", flush=True)
    print(f"Output shape: {result.shape}", flush=True)

    for label, cat_cols_list, count_col, sys_col in [
        ("CCSR ALL-DX", all_cat_cols, "ccsr_all_count", "ccsr_all_sys_count"),
        ("CCSR POA", poa_cat_cols, "ccsr_poa_count", "ccsr_poa_sys_count"),
    ]:
        prev = result[cat_cols_list].sum().sort_values(ascending=False)
        n_active = (prev > 0).sum()
        n_any = (result[count_col] > 0).sum()
        print(f"\n--- {label} ({n_active} active categories) ---", flush=True)
        print(f"Encounters with >=1 category: {n_any:,} ({n_any / len(result):.1%})", flush=True)
        print(f"Mean category count: {result[count_col].mean():.1f}", flush=True)
        print(f"Mean body systems: {result[sys_col].mean():.1f}", flush=True)

        # Top 20 categories
        print(f"Top 20 categories:", flush=True)
        for cat_col in prev.head(20).index:
            n = int(prev[cat_col])
            # Extract category code from column name (e.g., "ccsr_all_cir008" → "CIR008")
            cat_code = cat_col.split("_", 2)[2].upper()
            desc = labels.get(cat_code, "")
            print(f"  {cat_code}: {n:,} ({n / len(result):.1%}) — {desc}", flush=True)

        # Body system distribution
        print(f"\nBody system distribution:", flush=True)
        prefix_label = cat_cols_list[0].rsplit("_", 1)[0] + "_" if cat_cols_list else ""
        sys_totals = {}
        for sys_prefix in all_systems:
            sys_specific = [c for c in cat_cols_list if c[len(prefix_label):][:3].upper() == sys_prefix]
            if sys_specific:
                n_enc = (result[sys_specific].sum(axis=1) > 0).sum()
                sys_totals[sys_prefix] = n_enc
        for sys_prefix, n_enc in sorted(sys_totals.items(), key=lambda x: -x[1]):
            sys_name = BODY_SYSTEMS.get(sys_prefix, sys_prefix)
            print(f"  {sys_prefix} ({sys_name}): {n_enc:,} ({n_enc / len(result):.1%})", flush=True)

    # =====================================================================
    # SAVE
    # =====================================================================
    out_parquet = os.path.join(OUTPUT_DIR, "ccsr_features.parquet")
    out_csv = os.path.join(OUTPUT_DIR, "ccsr_features.csv")
    result.to_parquet(out_parquet, index=False)
    result.to_csv(out_csv, index=False)
    print(f"\nWrote {out_parquet}", flush=True)
    print(f"Wrote {out_csv}", flush=True)
    print(f"Shape: {result.shape}", flush=True)
    print(f"Total time: {time.time() - t0:.1f}s", flush=True)

    con.close()


if __name__ == "__main__":
    main()
