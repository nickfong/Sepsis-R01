#!/usr/bin/env python3
"""
Build per-encounter Elixhauser comorbidity features using two validated mappings:

  Mapping A — AHRQ Refined for ICD-10-CM v2026.1 (38 categories, POA-aware)
    Source: HCUP Elixhauser Comorbidity Software Refined for ICD-10-CM
    Downloaded: CMR-Reference-File-v2026-1.xlsx from hcup-us.ahrq.gov
    Note: CBVD is split into CBVD_POA + CBVD_SQLA in the mapping file;
          we merge them into a single CBVD category in the output.

  Mapping B — Quan et al. 2005 (31 categories)
    Source: Quan H, et al. Med Care 2005;43(11):1130-9
    Code lists from comorbidipy package (MIT license)

POA Logic (per AHRQ methodology):
  - POA-exempt categories: ANY diagnosis code counts regardless of POA status
  - POA-dependent categories: only count if presentonadmission='Yes'
    OR type IN ('Medical History', 'Problem List')

Weighted scores:
  - Van Walraven et al. 2009 (Med Care 47(6):626-33) weights for both mappings

Output: Filtered_Combined_2026-02-20/elixhauser_features.parquet (+ .csv)
"""

import os
import time

import duckdb
import openpyxl
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
COHORT_FILE = os.path.join(SCRIPT_DIR, "Filtered_Combined_2026-02-20", "filtered_cohort_combined.parquet")
AHRQ_EXCEL = os.path.join(SCRIPT_DIR, "CMR-Reference-File-v2026-1.xlsx")
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "Filtered_Combined_2026-02-20")

UCSF_DX = "/media/ubuntu/HDD Storage/parquet/deid_cdw/diagnosiseventfact/*.parquet"
UCSF_DIM = "/media/ubuntu/HDD Storage/parquet/deid_cdw/diagnosisterminologydim/*.parquet"
SFDPH_DX = "/media/ubuntu/HDD Storage/parquet/deid_cdw_sfdph/diagnosiseventfact/*.parquet"
SFDPH_DIM = "/media/ubuntu/HDD Storage/parquet/deid_cdw_sfdph/diagnosisterminologydim/*.parquet"

# ---------------------------------------------------------------------------
# AHRQ POA classification (from Comorbidity_Measures sheet)
# POA="No" means the category is POA-exempt (always counts)
# POA="Yes" means the category is POA-dependent (needs POA evidence)
# CBVD_SQLA (sequelae) are chronic conditions — treat as exempt
# ---------------------------------------------------------------------------
AHRQ_POA_DEPENDENT = {
    "ANEMDEF", "BLDLOSS", "CBVD_POA", "COAG", "HF",
    "LIVER_MLD", "LIVER_SEV", "NEURO_MOVT", "NEURO_OTH", "NEURO_SEIZ",
    "PARALYSIS", "PSYCHOSES", "PULMCIRC", "RENLFL_MOD", "RENLFL_SEV",
    "ULCER_PEPTIC", "VALVE", "WGHTLOSS",
}

# ---------------------------------------------------------------------------
# Quan et al. 2005 ICD-10 Elixhauser mapping (prefix-based)
# Source: comorbidipy package (MIT license), mapping.py
# These are PREFIX matches: "I50" matches I50, I500, I501, I509, etc.
# ---------------------------------------------------------------------------
QUAN_ICD10_MAPPING = {
    "chf": ("I099", "I110", "I130", "I132", "I255", "I420", "I425", "I426",
            "I427", "I428", "I429", "I43", "I50", "P290"),
    "carit": ("I441", "I442", "I443", "I456", "I459", "I47", "I48", "I49",
              "R000", "R001", "R008", "T821", "Z450", "Z950"),
    "valv": ("A520", "I05", "I06", "I07", "I08", "I091", "I098", "I34",
             "I35", "I36", "I37", "I38", "I39", "Q230", "Q231", "Q232",
             "Q233", "Z952", "Z953", "Z954"),
    "pcd": ("I26", "I27", "I280", "I288", "I289"),
    "pvd": ("I70", "I71", "I731", "I738", "I739", "I771", "I790", "I792",
            "K551", "K558", "K559", "Z958", "Z959"),
    "hypunc": ("I10",),
    "hypc": ("I11", "I12", "I13", "I15"),
    "para": ("G041", "G114", "G801", "G802", "G81", "G82", "G830", "G831",
             "G832", "G833", "G834", "G839"),
    "ond": ("G10", "G11", "G12", "G13", "G20", "G21", "G22", "G254", "G255",
            "G312", "G318", "G319", "G32", "G35", "G36", "G37", "G40", "G41",
            "G931", "G934", "R470", "R56"),
    "cpd": ("I278", "I279", "J40", "J41", "J42", "J43", "J44", "J45", "J46",
            "J47", "J60", "J61", "J62", "J63", "J64", "J65", "J66", "J67",
            "J684", "J701", "J703"),
    "diabunc": ("E100", "E101", "E109", "E110", "E111", "E119", "E120",
                "E121", "E129", "E130", "E131", "E139", "E140", "E141", "E149"),
    "diabc": ("E102", "E103", "E104", "E105", "E106", "E107", "E108",
              "E112", "E113", "E114", "E115", "E116", "E117", "E118",
              "E122", "E123", "E124", "E125", "E126", "E127", "E128",
              "E132", "E133", "E134", "E135", "E136", "E137", "E138",
              "E142", "E143", "E144", "E145", "E146", "E147", "E148"),
    "hypothy": ("E00", "E01", "E02", "E03", "E890"),
    "rf": ("I120", "I131", "N18", "N19", "N250", "Z490", "Z491", "Z492",
           "Z940", "Z992"),
    "ld": ("B18", "I85", "I864", "I982", "K70", "K711", "K713", "K714",
           "K715", "K717", "K72", "K73", "K74", "K760", "K762", "K763",
           "K764", "K765", "K766", "K767", "K768", "K769", "Z944"),
    "pud": ("K257", "K259", "K267", "K269", "K277", "K279", "K287", "K289"),
    "aids": ("B20", "B21", "B22", "B24"),
    "lymph": ("C81", "C82", "C83", "C84", "C85", "C88", "C96", "C900", "C902"),
    "metacanc": ("C77", "C78", "C79", "C80"),
    "solidtum": ("C00", "C01", "C02", "C03", "C04", "C05", "C06", "C07",
                 "C08", "C09", "C10", "C11", "C12", "C13", "C14", "C15",
                 "C16", "C17", "C18", "C19", "C20", "C21", "C22", "C23",
                 "C24", "C25", "C26", "C30", "C31", "C32", "C33", "C34",
                 "C37", "C38", "C39", "C40", "C41", "C43", "C45", "C46",
                 "C47", "C48", "C49", "C50", "C51", "C52", "C53", "C54",
                 "C55", "C56", "C57", "C58", "C60", "C61", "C62", "C63",
                 "C64", "C65", "C66", "C67", "C68", "C69", "C70", "C71",
                 "C72", "C73", "C74", "C75", "C76", "C97"),
    "rheumd": ("L940", "L941", "L943", "M05", "M06", "M08", "M120", "M123",
               "M30", "M310", "M311", "M312", "M313", "M32", "M33", "M34",
               "M35", "M45", "M461", "M468", "M469"),
    "coag": ("D65", "D66", "D67", "D68", "D691", "D693", "D694", "D695", "D696"),
    "obes": ("E66",),
    "wloss": ("E40", "E41", "E42", "E43", "E44", "E45", "E46", "R634", "R64"),
    "fed": ("E222", "E86", "E87"),
    "blane": ("D500",),
    "dane": ("D508", "D509", "D51", "D52", "D53"),
    "alcohol": ("F10", "E52", "G621", "I426", "K292", "K700", "K703", "K709",
                "T51", "Z502", "Z714", "Z721"),
    "drug": ("F11", "F12", "F13", "F14", "F15", "F16", "F18", "F19",
             "Z715", "Z722"),
    "psycho": ("F20", "F22", "F23", "F24", "F25", "F28", "F29", "F302",
               "F312", "F315"),
    "depre": ("F204", "F313", "F314", "F315", "F32", "F33", "F341",
              "F412", "F432"),
}

# Quan categories that are POA-exempt (chronic conditions)
QUAN_POA_EXEMPT = {
    "aids", "alcohol", "diabunc", "diabc", "hypothy", "hypunc", "hypc",
    "cpd", "obes", "drug", "psycho", "depre", "lymph", "metacanc",
    "solidtum", "para", "pvd",
}

# Van Walraven weights for Quan categories
# Source: van Walraven et al. 2009, Medical Care 47(6):626-33
VW_WEIGHTS_QUAN = {
    "chf": 7, "carit": 5, "valv": -1, "pcd": 4, "pvd": 2,
    "hypunc": 0, "hypc": 0, "para": 7, "ond": 6, "cpd": 3,
    "diabunc": 0, "diabc": 0, "hypothy": 0, "rf": 5, "ld": 11,
    "pud": 0, "aids": 0, "lymph": 9, "metacanc": 12, "solidtum": 4,
    "rheumd": 0, "coag": 3, "obes": -4, "wloss": 6, "fed": 5,
    "blane": -2, "dane": -2, "alcohol": 0, "drug": -7, "psycho": 0,
    "depre": -3,
}

# Van Walraven weights mapped to AHRQ category names
# For AHRQ categories split from the originals, we use the parent weight.
VW_WEIGHTS_AHRQ = {
    "AIDS": 0, "ALCOHOL": 0, "ANEMDEF": -2, "AUTOIMMUNE": 0,
    "BLDLOSS": -2, "CANCER_LEUK": 9, "CANCER_LYMPH": 9,
    "CANCER_METS": 12, "CANCER_NSITU": 4, "CANCER_SOLID": 4,
    "CBVD": 0, "COAG": 3, "DEMENTIA": 0, "DEPRESS": -3,
    "DIAB_CX": 0, "DIAB_UNCX": 0, "DRUG_ABUSE": -7,
    "HF": 7, "HTN_CX": 0, "HTN_UNCX": 0,
    "LIVER_MLD": 11, "LIVER_SEV": 11,
    "LUNG_CHRONIC": 3, "NEURO_MOVT": 6, "NEURO_OTH": 6,
    "NEURO_SEIZ": 6, "OBESE": -4, "PARALYSIS": 7,
    "PERIVASC": 2, "PSYCHOSES": 0, "PULMCIRC": 4,
    "RENLFL_MOD": 5, "RENLFL_SEV": 5,
    "THYROID_HYPO": 0, "THYROID_OTH": 0,
    "ULCER_PEPTIC": 0, "VALVE": -1, "WGHTLOSS": 6,
}


def load_ahrq_mapping(excel_path):
    """Load AHRQ ICD-10-CM → Elixhauser category mapping from Excel file.

    Returns DataFrame with columns: icd_code, category
    """
    wb = openpyxl.load_workbook(excel_path, read_only=True)
    ws = wb["DX_to_Comorb_Mapping"]

    rows = list(ws.iter_rows(min_row=2, values_only=True))
    headers = list(rows[0])
    cat_names = headers[3:]

    records = []
    for row in rows[1:]:
        icd_code = row[0]
        if icd_code is None:
            continue
        for i, cat in enumerate(cat_names):
            if row[i + 3] == 1:
                records.append((icd_code, cat))

    wb.close()
    return pd.DataFrame(records, columns=["icd_code", "category"])


def build_quan_mapping_df(quan_mapping):
    """Expand Quan prefix mapping into a DataFrame of (prefix, category).

    Returns DataFrame with columns: prefix, quan_category
    """
    records = []
    for cat, prefixes in quan_mapping.items():
        for prefix in prefixes:
            records.append((prefix, cat))
    return pd.DataFrame(records, columns=["prefix", "quan_category"])


def main():
    t0 = time.time()
    print("=" * 70, flush=True)
    print("Building Elixhauser comorbidity features", flush=True)
    print("  AHRQ Refined v2026.1 (38 categories) + Quan et al. 2005 (31 categories)", flush=True)
    print("=" * 70, flush=True)

    # --- Load AHRQ mapping from Excel ---
    print("\nLoading AHRQ mapping from Excel...", flush=True)
    ahrq_map_df = load_ahrq_mapping(AHRQ_EXCEL)
    print(f"  {ahrq_map_df['category'].nunique()} categories, "
          f"{len(ahrq_map_df)} ICD-to-category mappings", flush=True)

    # Tag POA dependency
    ahrq_map_df["poa_dependent"] = ahrq_map_df["category"].isin(AHRQ_POA_DEPENDENT)

    # --- Build Quan prefix mapping ---
    print("Building Quan prefix mapping...", flush=True)
    quan_map_df = build_quan_mapping_df(QUAN_ICD10_MAPPING)
    quan_map_df["poa_dependent"] = ~quan_map_df["quan_category"].isin(QUAN_POA_EXEMPT)
    print(f"  {quan_map_df['quan_category'].nunique()} categories, "
          f"{len(quan_map_df)} prefix entries", flush=True)

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
        REPLACE(dtd.value, '.', '') AS icd_code,
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

    # --- Determine POA eligibility per record (vectorized) ---
    dx_df["poa_qualified"] = (
        (dx_df["presentonadmission"].str.lower() == "yes")
        | (dx_df["type"].isin(["Medical History", "Problem List"]))
    )

    # =====================================================================
    # AHRQ MAPPING (exact code match via merge)
    # =====================================================================
    print("\n--- AHRQ Mapping (exact match) ---", flush=True)
    t2 = time.time()

    # Merge diagnosis records with AHRQ mapping on ICD code
    ahrq_matched = dx_df.merge(ahrq_map_df, on="icd_code", how="inner")

    # Apply POA logic: keep if (POA-exempt) OR (POA-dependent AND poa_qualified)
    ahrq_matched = ahrq_matched[
        (~ahrq_matched["poa_dependent"]) | (ahrq_matched["poa_qualified"])
    ]

    # Merge CBVD_POA and CBVD_SQLA into single CBVD
    ahrq_matched["category"] = ahrq_matched["category"].replace(
        {"CBVD_POA": "CBVD", "CBVD_SQLA": "CBVD"}
    )

    # Deduplicate to encounter × category
    ahrq_long = ahrq_matched[["encounterkey", "category"]].drop_duplicates()
    print(f"  Encounter-category pairs: {len(ahrq_long):,}", flush=True)
    print(f"  Time: {time.time() - t2:.1f}s", flush=True)

    # Pivot to wide format
    ahrq_long["value"] = 1
    ahrq_wide = ahrq_long.pivot_table(
        index="encounterkey", columns="category", values="value",
        aggfunc="max", fill_value=0,
    )
    ahrq_wide.columns = [f"ahrq_{c.lower()}" for c in ahrq_wide.columns]
    ahrq_wide = ahrq_wide.reset_index()

    # =====================================================================
    # QUAN MAPPING (prefix match via DuckDB)
    # =====================================================================
    print("\n--- Quan Mapping (prefix match) ---", flush=True)
    t3 = time.time()

    # Register prefix table in DuckDB for efficient prefix matching
    con.register("quan_prefixes", quan_map_df)
    con.register("dx_records", dx_df[["encounterkey", "icd_code", "poa_qualified"]])

    quan_query = """
    WITH matched AS (
        SELECT DISTINCT
            d.encounterkey,
            q.quan_category,
            q.poa_dependent,
            d.poa_qualified
        FROM dx_records d
        INNER JOIN quan_prefixes q
            ON d.icd_code LIKE (q.prefix || '%')
    )
    SELECT encounterkey, quan_category
    FROM matched
    WHERE NOT poa_dependent OR poa_qualified
    """
    quan_long = con.execute(quan_query).fetchdf()
    quan_long = quan_long.drop_duplicates()
    print(f"  Encounter-category pairs: {len(quan_long):,}", flush=True)
    print(f"  Time: {time.time() - t3:.1f}s", flush=True)

    # Pivot to wide format
    quan_long["value"] = 1
    quan_wide = quan_long.pivot_table(
        index="encounterkey", columns="quan_category", values="value",
        aggfunc="max", fill_value=0,
    )
    quan_wide.columns = [f"quan_{c}" for c in quan_wide.columns]
    quan_wide = quan_wide.reset_index()

    # =====================================================================
    # MERGE AND COMPUTE SCORES
    # =====================================================================
    print("\n--- Merging and computing scores ---", flush=True)

    # Get full cohort
    all_encounters = con.execute("SELECT encounterkey FROM cohort").fetchdf()

    # Merge AHRQ
    result = all_encounters.merge(ahrq_wide, on="encounterkey", how="left")
    ahrq_cols = [c for c in result.columns if c.startswith("ahrq_")]
    result[ahrq_cols] = result[ahrq_cols].fillna(0).astype("int8")

    # Merge Quan
    result = result.merge(quan_wide, on="encounterkey", how="left")
    quan_cols = [c for c in result.columns if c.startswith("quan_")]
    result[quan_cols] = result[quan_cols].fillna(0).astype("int8")

    # Ensure all expected AHRQ columns exist
    for cat in VW_WEIGHTS_AHRQ:
        col = f"ahrq_{cat.lower()}"
        if col not in result.columns:
            result[col] = pd.array([0] * len(result), dtype="int8")

    # Ensure all expected Quan columns exist
    for cat in QUAN_ICD10_MAPPING:
        col = f"quan_{cat}"
        if col not in result.columns:
            result[col] = pd.array([0] * len(result), dtype="int8")

    # Compute van Walraven weighted score (AHRQ) — vectorized
    ahrq_score = pd.Series(0, index=result.index, dtype="int16")
    for cat, weight in VW_WEIGHTS_AHRQ.items():
        col = f"ahrq_{cat.lower()}"
        if weight != 0 and col in result.columns:
            ahrq_score += result[col].astype("int16") * weight
    result["ahrq_vw_score"] = ahrq_score

    # Compute van Walraven weighted score (Quan) — vectorized
    quan_score = pd.Series(0, index=result.index, dtype="int16")
    for cat, weight in VW_WEIGHTS_QUAN.items():
        col = f"quan_{cat}"
        if weight != 0 and col in result.columns:
            quan_score += result[col].astype("int16") * weight
    result["quan_vw_score"] = quan_score

    # Count of comorbidities
    ahrq_cols_final = sorted([c for c in result.columns if c.startswith("ahrq_") and c != "ahrq_vw_score"])
    quan_cols_final = sorted([c for c in result.columns if c.startswith("quan_") and c != "quan_vw_score"])
    result["ahrq_count"] = result[ahrq_cols_final].sum(axis=1).astype("int8")
    result["quan_count"] = result[quan_cols_final].sum(axis=1).astype("int8")

    # =====================================================================
    # SUMMARY
    # =====================================================================
    print(f"\n{'=' * 70}", flush=True)
    print("RESULTS", flush=True)
    print(f"{'=' * 70}", flush=True)
    print(f"Total encounters: {len(result):,}", flush=True)

    print(f"\n--- AHRQ Elixhauser ({len(ahrq_cols_final)} categories) ---", flush=True)
    ahrq_prev = result[ahrq_cols_final].sum().sort_values(ascending=False)
    n_with_any_ahrq = (result[ahrq_cols_final].sum(axis=1) > 0).sum()
    print(f"Encounters with >=1 AHRQ comorbidity: {n_with_any_ahrq:,} ({n_with_any_ahrq / len(result):.1%})", flush=True)
    print(f"Mean AHRQ comorbidities per encounter: {result['ahrq_count'].mean():.1f}", flush=True)
    print(f"Mean AHRQ VW score: {result['ahrq_vw_score'].mean():.1f}", flush=True)
    print(f"\nTop 10 AHRQ categories:", flush=True)
    for cat in ahrq_prev.head(10).index:
        n = int(ahrq_prev[cat])
        print(f"  {cat}: {n:,} ({n / len(result):.1%})", flush=True)

    print(f"\n--- Quan Elixhauser ({len(quan_cols_final)} categories) ---", flush=True)
    quan_prev = result[quan_cols_final].sum().sort_values(ascending=False)
    n_with_any_quan = (result[quan_cols_final].sum(axis=1) > 0).sum()
    print(f"Encounters with >=1 Quan comorbidity: {n_with_any_quan:,} ({n_with_any_quan / len(result):.1%})", flush=True)
    print(f"Mean Quan comorbidities per encounter: {result['quan_count'].mean():.1f}", flush=True)
    print(f"Mean Quan VW score: {result['quan_vw_score'].mean():.1f}", flush=True)
    print(f"\nTop 10 Quan categories:", flush=True)
    for cat in quan_prev.head(10).index:
        n = int(quan_prev[cat])
        print(f"  {cat}: {n:,} ({n / len(result):.1%})", flush=True)

    # =====================================================================
    # SAVE
    # =====================================================================
    out_parquet = os.path.join(OUTPUT_DIR, "elixhauser_features.parquet")
    out_csv = os.path.join(OUTPUT_DIR, "elixhauser_features.csv")
    result.to_parquet(out_parquet, index=False)
    result.to_csv(out_csv, index=False)
    print(f"\nWrote {out_parquet}", flush=True)
    print(f"Wrote {out_csv}", flush=True)
    print(f"Shape: {result.shape}", flush=True)
    print(f"Total time: {time.time() - t0:.1f}s", flush=True)

    con.close()


if __name__ == "__main__":
    main()
