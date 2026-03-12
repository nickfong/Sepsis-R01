#!/usr/bin/env python3
"""
Build per-encounter HCC (Hierarchical Condition Category) features.

Uses CMS-HCC Model V28, Payment Year 2026 — parsed directly from official
CMS source files (no third-party packages).

Primary source files (in cms_hcc_v28_2026/):
  - F2826T1N.TXT:  ICD-10-CM → HCC crosswalk (CMS 2026 Midyear/Final)
  - V28115H1.TXT:  Hierarchy SAS macro (which HCCs trump which)
  - C2824T2N.csv:  RAF coefficients (CNA segment used for scoring)
  - V28115L3.TXT:  HCC labels

Downloaded from: https://www.cms.gov/medicare/payment/medicare-advantage-rates-statistics/risk-adjustment/2026-model-software-icd-10-mappings

Produces two variants:
  - hcc_all_*: Standard CMS approach (all ICD-10-CM codes on encounter)
  - hcc_poa_*: POA-aware variant (only codes with presentonadmission='Yes'
    or type IN ('Medical History', 'Problem List'))

HCC key concepts:
  - 115 payment model categories organized into hierarchical families
  - Hierarchy enforcement: within a family, only the most severe counts
  - Heart patch: CC223 removed if no other heart CCs (221,222,224,225,226) present
  - RAF score: sum of CNA (Community NonDual Aged) HCC coefficients + interaction
    terms + diagnostic count bonuses. Demographics excluded (de-identified data).

Output: Filtered_Combined_2026-02-20/hcc_features.parquet (+ .csv)
"""

import os
import re
import time

import duckdb
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CMS_DIR = os.path.join(SCRIPT_DIR, "cms_hcc_v28_2026")
COHORT_FILE = os.path.join(SCRIPT_DIR, "Filtered_Combined_2026-02-20", "filtered_cohort_combined.parquet")
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "Filtered_Combined_2026-02-20")

UCSF_DX = "/media/ubuntu/HDD Storage/parquet/deid_cdw/diagnosiseventfact/*.parquet"
UCSF_DIM = "/media/ubuntu/HDD Storage/parquet/deid_cdw/diagnosisterminologydim/*.parquet"
SFDPH_DX = "/media/ubuntu/HDD Storage/parquet/deid_cdw_sfdph/diagnosiseventfact/*.parquet"
SFDPH_DIM = "/media/ubuntu/HDD Storage/parquet/deid_cdw_sfdph/diagnosisterminologydim/*.parquet"


# =============================================================================
# CMS FILE PARSERS
# =============================================================================

def load_cms_crosswalk(path):
    """Parse CMS ICD-10-CM → HCC crosswalk from F2826T1N.TXT.

    File format: tab-delimited, ICD_CODE<tab>HCC_NUMBER per line.
    One ICD code can map to multiple HCCs (separate lines).

    Returns:
        dict[str, list[int]]: ICD code (no dots) → list of HCC numbers
    """
    dx2hcc = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 2:
                continue
            icd = parts[0].strip().upper().replace(".", "")
            hcc_num = int(parts[1].strip())
            if icd not in dx2hcc:
                dx2hcc[icd] = []
            if hcc_num not in dx2hcc[icd]:
                dx2hcc[icd].append(hcc_num)
    return dx2hcc


def load_cms_hierarchy(path):
    """Parse CMS hierarchy SAS macro from V28115H1.TXT.

    Extracts %SET0(CC=parent, HIER=%STR(child1, child2, ...)) rules.
    When parent HCC is present, all children are suppressed.

    Returns:
        dict[int, list[int]]: parent HCC → list of child HCCs to suppress
    """
    hierarchy = {}
    text = open(path).read()
    # Remove line continuations: lines ending without ));  have content on next line
    text = text.replace("\n", " ")
    # Find all %SET0 calls
    pattern = r'%SET0\s*\(\s*CC\s*=\s*(\d+)\s*,\s*HIER\s*=\s*%STR\(([^)]+)\)\s*\)'
    for match in re.finditer(pattern, text):
        parent = int(match.group(1))
        children = [int(x.strip()) for x in match.group(2).split(",") if x.strip()]
        hierarchy[parent] = children
    return hierarchy


def load_cms_coefficients(path):
    """Parse CMS RAF coefficients from C2824T2N.csv.

    Extracts CNA (Community NonDual Aged) segment coefficients for:
      - HCC indicators (CNA_HCC*)
      - Interaction terms (CNA_DIABETES_HF_V28, etc.)
      - Diagnostic count bonuses (CNA_D1 through CNA_D10P)

    Returns:
        hcc_coeffs: dict[int, float] — HCC number → coefficient
        interaction_coeffs: dict[str, float] — interaction name → coefficient
        dcount_coeffs: dict[str, float] — D1..D10P → coefficient
    """
    hcc_coeffs = {}
    interaction_coeffs = {}
    dcount_coeffs = {}

    with open(path, encoding="latin-1") as f:
        header = f.readline()  # skip header
        for line in f:
            parts = [p.strip() for p in line.split(",", 2)]
            if len(parts) < 2:
                continue
            name = parts[0].strip()
            coeff = float(parts[1].strip())

            if not name.startswith("CNA_"):
                continue

            suffix = name[4:]  # strip "CNA_"
            hcc_match = re.match(r'^HCC(\d+)$', suffix)
            if hcc_match:
                hcc_coeffs[int(hcc_match.group(1))] = coeff
            elif suffix.startswith("D") and suffix[1:].replace("P", "").isdigit():
                dcount_coeffs[suffix] = coeff
            elif suffix in ("DIABETES_HF_V28", "HF_CHR_LUNG_V28", "HF_KIDNEY_V28",
                            "CHR_LUNG_CARD_RESP_FAIL_V28", "HF_HCC238_V28"):
                interaction_coeffs[suffix] = coeff

    return hcc_coeffs, interaction_coeffs, dcount_coeffs


def load_cms_labels(path):
    """Parse HCC labels from V28115L3.TXT.

    Returns:
        dict[int, str]: HCC number → description
    """
    labels = {}
    text = open(path).read()
    pattern = r'HCC(\d+)\s*=\s*"([^"]+)"'
    for match in re.finditer(pattern, text):
        labels[int(match.group(1))] = match.group(2).strip()
    return labels


# =============================================================================
# HCC ENGINE (replaces hccpy)
# =============================================================================

def apply_heart_patch(cc_set):
    """CMS heart interaction patch (from V2826T1M.TXT lines 449-450).

    If CC223 is present but none of CC221, CC222, CC224, CC225, CC226 are
    present, remove CC223. This is applied BEFORE hierarchy enforcement.
    """
    if 223 in cc_set:
        if not cc_set & {221, 222, 224, 225, 226}:
            cc_set.discard(223)
    return cc_set


def apply_hierarchy(hcc_set, hierarchy):
    """Apply CMS HCC hierarchy rules.

    For each parent HCC present, suppress all its children.

    Args:
        hcc_set: set of HCC numbers (modified in place)
        hierarchy: dict[int, list[int]] from load_cms_hierarchy
    """
    for parent, children in hierarchy.items():
        if parent in hcc_set:
            for child in children:
                hcc_set.discard(child)
    return hcc_set


def compute_interactions(hcc_set):
    """Compute CMS-HCC V28 interaction terms from final HCC set.

    Interaction definitions from V2826T1M.TXT (CNA segment, lines 457-475).

    Returns:
        dict[str, int]: interaction name → 0 or 1
    """
    diabetes = bool(hcc_set & {35, 36, 37, 38})
    hf = bool(hcc_set & {221, 222, 223, 224, 225, 226})
    chr_lung = bool(hcc_set & {276, 277, 278, 279, 280})
    card_resp_fail = bool(hcc_set & {211, 212, 213})
    has_238 = 238 in hcc_set

    return {
        "DIABETES_HF_V28": int(diabetes and hf),
        "HF_CHR_LUNG_V28": int(hf and chr_lung),
        "HF_KIDNEY_V28": int(hf and bool(hcc_set & {326, 327, 328, 329})),
        "CHR_LUNG_CARD_RESP_FAIL_V28": int(chr_lung and card_resp_fail),
        "HF_HCC238_V28": int(hf and has_238),
    }


def compute_dcount_key(n_hcc):
    """Determine diagnostic count variable name from HCC count.

    From V2826T1M.TXT lines 494-501: D1-D9 are indicator vars,
    D10P is indicator for 10 or more.
    """
    if n_hcc <= 0:
        return None
    if n_hcc <= 9:
        return f"D{n_hcc}"
    return "D10P"


def compute_raf_score(hcc_set, hcc_coeffs, interaction_coeffs, dcount_coeffs):
    """Compute RAF score from final HCC set using CNA segment coefficients.

    Score = sum(HCC coefficients) + sum(interaction coefficients) + D-count bonus.
    Demographics (age/sex bracket) are excluded since data is de-identified.
    """
    score = 0.0

    # HCC coefficients
    for hcc in hcc_set:
        score += hcc_coeffs.get(hcc, 0.0)

    # Interaction terms
    interactions = compute_interactions(hcc_set)
    for name, active in interactions.items():
        if active:
            score += interaction_coeffs.get(name, 0.0)

    # Diagnostic count bonus
    dkey = compute_dcount_key(len(hcc_set))
    if dkey:
        score += dcount_coeffs.get(dkey, 0.0)

    return score


def profile_encounter(icd_codes, dx2hcc, hierarchy, hcc_coeffs,
                       interaction_coeffs, dcount_coeffs):
    """Profile a single encounter: ICD codes → HCC set + RAF score.

    Implements the full CMS-HCC V28 pipeline:
      1. Map ICD codes → CC numbers (from crosswalk)
      2. Apply heart patch (CC223 rule)
      3. Apply hierarchy enforcement
      4. Compute RAF score (CNA segment)

    Args:
        icd_codes: list of ICD-10-CM codes (no dots, uppercase)
        dx2hcc: crosswalk dict from load_cms_crosswalk
        hierarchy: hierarchy dict from load_cms_hierarchy
        hcc_coeffs, interaction_coeffs, dcount_coeffs: from load_cms_coefficients

    Returns:
        (frozenset of HCC numbers, float RAF score)
    """
    # Step 1: Map ICD codes to CC/HCC numbers
    cc_set = set()
    for code in icd_codes:
        if code in dx2hcc:
            cc_set.update(dx2hcc[code])

    if not cc_set:
        return frozenset(), 0.0

    # Step 2: Heart patch (before hierarchy)
    apply_heart_patch(cc_set)

    # Step 3: Copy to HCC set and apply hierarchy
    hcc_set = set(cc_set)
    apply_hierarchy(hcc_set, hierarchy)

    # Step 4: Compute RAF score
    raf = compute_raf_score(hcc_set, hcc_coeffs, interaction_coeffs, dcount_coeffs)

    return frozenset(hcc_set), raf


# =============================================================================
# MAIN
# =============================================================================

def main():
    t0 = time.time()
    print("=" * 70, flush=True)
    print("Building HCC (Hierarchical Condition Category) features", flush=True)
    print("  CMS-HCC Model V28 — Payment Year 2026 (from official CMS files)", flush=True)
    print("  Two variants: all-dx + POA-aware", flush=True)
    print("=" * 70, flush=True)

    # --- Load CMS primary source files ---
    print("\nLoading CMS V28 primary source files...", flush=True)

    crosswalk_path = os.path.join(CMS_DIR, "F2826T1N.TXT")
    hierarchy_path = os.path.join(CMS_DIR, "V28115H1.TXT")
    coefficients_path = os.path.join(CMS_DIR, "C2824T2N.csv")
    labels_path = os.path.join(CMS_DIR, "V28115L3.TXT")

    dx2hcc = load_cms_crosswalk(crosswalk_path)
    hierarchy = load_cms_hierarchy(hierarchy_path)
    hcc_coeffs, interaction_coeffs, dcount_coeffs = load_cms_coefficients(coefficients_path)
    labels = load_cms_labels(labels_path)

    print(f"  Crosswalk: {len(dx2hcc):,} ICD-10-CM codes mapped", flush=True)
    print(f"  Hierarchy: {len(hierarchy)} parent rules", flush=True)
    print(f"  Coefficients: {len(hcc_coeffs)} HCC + {len(interaction_coeffs)} interaction"
          f" + {len(dcount_coeffs)} D-count terms", flush=True)
    print(f"  Labels: {len(labels)} HCC descriptions", flush=True)

    # All possible HCC numbers (from coefficients)
    all_hcc_nums = sorted(hcc_coeffs.keys())
    print(f"  Payment model HCCs: {len(all_hcc_nums)}", flush=True)

    # --- Connect DuckDB ---
    con = duckdb.connect()
    con.execute('SET memory_limit="200GB"')
    con.execute("SET threads=24")

    # Load cohort
    con.execute(f"CREATE VIEW cohort AS SELECT DISTINCT encounterkey FROM read_parquet('{COHORT_FILE}')")
    cohort_n = con.execute("SELECT COUNT(*) FROM cohort").fetchone()[0]
    print(f"\nCohort encounters: {cohort_n:,}", flush=True)

    # --- Query ALL diagnosis records ---
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

    # Check crosswalk coverage
    unique_codes = set(dx_df["icd_code"].unique())
    mapped_codes = unique_codes & set(dx2hcc.keys())
    print(f"ICD codes with HCC mapping: {len(mapped_codes):,}/{len(unique_codes):,}"
          f" ({len(mapped_codes)/len(unique_codes):.1%})", flush=True)

    # --- POA eligibility ---
    dx_df["poa_qualified"] = (
        (dx_df["presentonadmission"].str.lower() == "yes")
        | (dx_df["type"].isin(["Medical History", "Problem List"]))
    )

    # =====================================================================
    # VARIANT 1: ALL DIAGNOSES (standard CMS approach)
    # =====================================================================
    print("\n--- HCC All-Dx Variant ---", flush=True)
    t2 = time.time()

    # Group ICD codes by encounter (all codes)
    all_codes_by_enc = dx_df.groupby("encounterkey")["icd_code"].apply(
        lambda x: list(set(x))
    ).to_dict()
    print(f"  Encounters with ICD codes: {len(all_codes_by_enc):,}", flush=True)

    # Profile each encounter
    print("  Running HCC profiling (all-dx)...", flush=True)
    all_records = []
    total = len(all_codes_by_enc)
    for i, (enc_key, codes) in enumerate(all_codes_by_enc.items()):
        hcc_set, raf = profile_encounter(
            codes, dx2hcc, hierarchy, hcc_coeffs, interaction_coeffs, dcount_coeffs
        )
        all_records.append((enc_key, hcc_set, raf))
        if (i + 1) % 50000 == 0:
            elapsed = time.time() - t2
            print(f"    Profiled {i + 1:,}/{total:,} encounters ({elapsed:.1f}s)", flush=True)

    print(f"  Profiled {total:,}/{total:,} encounters ({time.time() - t2:.1f}s)", flush=True)

    # Convert to wide format
    hcc_all_wide = _to_wide(all_records, "hcc_all", all_hcc_nums)

    # =====================================================================
    # VARIANT 2: POA-AWARE (pre-existing conditions only)
    # =====================================================================
    print("\n--- HCC POA Variant ---", flush=True)
    t3 = time.time()

    # Filter to POA-qualified records only
    poa_dx = dx_df[dx_df["poa_qualified"]]
    poa_codes_by_enc = poa_dx.groupby("encounterkey")["icd_code"].apply(
        lambda x: list(set(x))
    ).to_dict()
    print(f"  Encounters with POA-qualified ICD codes: {len(poa_codes_by_enc):,}", flush=True)

    # Profile each encounter
    print("  Running HCC profiling (POA)...", flush=True)
    poa_records = []
    total = len(poa_codes_by_enc)
    for i, (enc_key, codes) in enumerate(poa_codes_by_enc.items()):
        hcc_set, raf = profile_encounter(
            codes, dx2hcc, hierarchy, hcc_coeffs, interaction_coeffs, dcount_coeffs
        )
        poa_records.append((enc_key, hcc_set, raf))
        if (i + 1) % 50000 == 0:
            elapsed = time.time() - t3
            print(f"    Profiled {i + 1:,}/{total:,} encounters ({elapsed:.1f}s)", flush=True)

    print(f"  Profiled {total:,}/{total:,} encounters ({time.time() - t3:.1f}s)", flush=True)

    # Convert to wide format
    hcc_poa_wide = _to_wide(poa_records, "hcc_poa", all_hcc_nums)

    # =====================================================================
    # MERGE
    # =====================================================================
    print("\n--- Merging ---", flush=True)

    all_encounters = con.execute("SELECT encounterkey FROM cohort").fetchdf()

    result = all_encounters.merge(hcc_all_wide, on="encounterkey", how="left")
    result = result.merge(hcc_poa_wide, on="encounterkey", how="left")

    # Fill NaN for encounters with no HCC codes
    hcc_all_cols = [c for c in result.columns if c.startswith("hcc_all_hcc")]
    hcc_poa_cols = [c for c in result.columns if c.startswith("hcc_poa_hcc")]
    all_binary_cols = hcc_all_cols + hcc_poa_cols

    if all_binary_cols:
        result[all_binary_cols] = result[all_binary_cols].fillna(0).astype("int8")

    for col in ["hcc_all_raf_score", "hcc_poa_raf_score"]:
        if col in result.columns:
            result[col] = result[col].fillna(0.0).astype("float32")

    for col in ["hcc_all_count", "hcc_poa_count"]:
        if col in result.columns:
            result[col] = result[col].fillna(0).astype("int8")

    # =====================================================================
    # SUMMARY
    # =====================================================================
    print(f"\n{'=' * 70}", flush=True)
    print("RESULTS", flush=True)
    print(f"{'=' * 70}", flush=True)
    print(f"Total encounters: {len(result):,}", flush=True)

    # All-dx stats
    print(f"\n--- HCC All-Dx ({len(hcc_all_cols)} categories active) ---", flush=True)
    n_with_any_all = (result["hcc_all_count"] > 0).sum()
    print(f"Encounters with >=1 HCC: {n_with_any_all:,} ({n_with_any_all / len(result):.1%})", flush=True)
    print(f"Mean HCC count: {result['hcc_all_count'].mean():.1f}", flush=True)
    print(f"Mean RAF score: {result['hcc_all_raf_score'].mean():.2f}", flush=True)
    print(f"Median RAF score: {result['hcc_all_raf_score'].median():.2f}", flush=True)
    if hcc_all_cols:
        all_prev = result[hcc_all_cols].sum().sort_values(ascending=False)
        print(f"\nTop 10 HCC categories (all-dx):", flush=True)
        for cat in all_prev.head(10).index:
            n = int(all_prev[cat])
            hcc_num = int(cat.replace("hcc_all_hcc", ""))
            label = labels.get(hcc_num, "")
            print(f"  {cat}: {n:,} ({n / len(result):.1%}) — {label}", flush=True)

    # POA stats
    print(f"\n--- HCC POA ({len(hcc_poa_cols)} categories active) ---", flush=True)
    n_with_any_poa = (result["hcc_poa_count"] > 0).sum()
    print(f"Encounters with >=1 HCC: {n_with_any_poa:,} ({n_with_any_poa / len(result):.1%})", flush=True)
    print(f"Mean HCC count: {result['hcc_poa_count'].mean():.1f}", flush=True)
    print(f"Mean RAF score: {result['hcc_poa_raf_score'].mean():.2f}", flush=True)
    print(f"Median RAF score: {result['hcc_poa_raf_score'].median():.2f}", flush=True)
    if hcc_poa_cols:
        poa_prev = result[hcc_poa_cols].sum().sort_values(ascending=False)
        print(f"\nTop 10 HCC categories (POA):", flush=True)
        for cat in poa_prev.head(10).index:
            n = int(poa_prev[cat])
            hcc_num = int(cat.replace("hcc_poa_hcc", ""))
            label = labels.get(hcc_num, "")
            print(f"  {cat}: {n:,} ({n / len(result):.1%}) — {label}", flush=True)

    # =====================================================================
    # SAVE
    # =====================================================================
    out_parquet = os.path.join(OUTPUT_DIR, "hcc_features.parquet")
    out_csv = os.path.join(OUTPUT_DIR, "hcc_features.csv")
    result.to_parquet(out_parquet, index=False)
    result.to_csv(out_csv, index=False)
    print(f"\nWrote {out_parquet}", flush=True)
    print(f"Wrote {out_csv}", flush=True)
    print(f"Shape: {result.shape}", flush=True)
    print(f"Total time: {time.time() - t0:.1f}s", flush=True)

    con.close()


def _to_wide(records, prefix, all_hcc_nums):
    """Convert profiling results to wide binary DataFrame.

    Args:
        records: list of (encounterkey, frozenset_of_hcc_nums, raf_score)
        prefix: column prefix ('hcc_all' or 'hcc_poa')
        all_hcc_nums: sorted list of all possible HCC numbers

    Returns:
        DataFrame with encounterkey + binary HCC columns + raf_score + count
    """
    # Build long-format records
    long_rows = []
    raf_rows = []
    for enc_key, hcc_set, raf in records:
        raf_rows.append((enc_key, raf))
        for hcc_num in hcc_set:
            long_rows.append((enc_key, hcc_num))

    if long_rows:
        long_df = pd.DataFrame(long_rows, columns=["encounterkey", "hcc_num"])
        long_df["value"] = 1
        long_df = long_df.drop_duplicates(subset=["encounterkey", "hcc_num"])

        wide = long_df.pivot_table(
            index="encounterkey", columns="hcc_num", values="value",
            aggfunc="max", fill_value=0,
        )
        wide.columns = [f"{prefix}_hcc{c}" for c in wide.columns]
        wide = wide.reset_index()
    else:
        wide = pd.DataFrame({"encounterkey": [r[0] for r in records]})

    # Add RAF score
    raf_df = pd.DataFrame(raf_rows, columns=["encounterkey", f"{prefix}_raf_score"])
    wide = wide.merge(raf_df, on="encounterkey", how="left")

    # Count of HCC categories
    hcc_cols = [c for c in wide.columns if c.startswith(f"{prefix}_hcc")]
    if hcc_cols:
        wide[f"{prefix}_count"] = wide[hcc_cols].sum(axis=1).astype("int8")
    else:
        wide[f"{prefix}_count"] = 0

    return wide


if __name__ == "__main__":
    main()
