#!/usr/bin/env python3
"""
Sepsis R01 Time-Varying Covariate Analysis Script
Analyzes the most commonly recorded time-varying covariates from extracted data.
Maps raw variable names to clinical concepts across sites.

Usage:
    python 03_analyze_covariates.py
    python 03_analyze_covariates.py -i Data_All_UCSF_2026-02-06 -i Data_All_SFDPH_2026-02-17
    python 03_analyze_covariates.py --top 100  # Show top 100 instead of 50

Output:
    - Console table of top N covariates by clinical concept
    - top_50_covariates.csv with per-site names and counts
"""

import os
import sys
import argparse
import pandas as pd
from pathlib import Path
from typing import List, Dict

# Default data directories (auto-detect latest)
DEFAULT_DATA_DIRS = [
    Path("Data_All_UCSF_2026-02-17"),
    Path("Data_All_SFDPH_2026-02-17"),
]

# Clinical concept mapping: concept -> list of raw variable names
# Any raw name not mapped here is passed through as its own concept
CONCEPT_MAP = {
    # --- Vitals ---
    'Respirations': ['RESPIRATIONS'],
    'Pulse': ['PULSE'],
    'Blood Pressure': ['BLOOD PRESSURE'],
    'Temperature': ['TEMPERATURE'],
    'MAP': ['R MAP'],
    'MAP (A-Line)': ['R MAP A-LINE', 'R MAP A-LINE 2'],
    'MAP (Measured)': ['RT MAP MEASURED', 'RT MAP MEASURED_IP_CD_UCSF'],
    'Arterial Line BP': ['R ARTERIAL LINE BLOOD PRESSURE', 'R ARTERIAL LINE BLOOD PRESSURE 2'],
    'Pulse Oximetry': [
        'PULSE OXIMETRY',
        'PULSE - PALPATED OR PLETH',
        'R ZSFG PULSE FROM SPO2',
        'R ZSFG PULSE FROM ART LINE',
        'R PULSE OXIMETRY TYPE',
    ],
    'Weight': ['WEIGHT/SCALE'],
    'GCS - Eye': [
        'R CPN ADULT GLASGOW COMA SCALE EYE OPENING',
        'R DPH ED GCS ADULT BEST EYE RESPONSE',
    ],
    'GCS - Verbal': [
        'R CPN ADULT GLASGOW COMA SCALE BEST VERBAL RESPONSE',
        'R DPH ED GCS ADULT BEST VERBAL RESPONSE',
    ],
    'GCS - Motor': [
        'R CPN GLASGOW COMA SCALE BEST MOTOR RESPONSE',
        'R DPH ED GCS ADULT BEST MOTOR RESPONSE',
    ],
    'GCS - Score': [
        'R CPN GLASGOW COMA SCALE SCORE',
        'R CPN GLASGOW COMA SCALE SCORE V2',
        'R DPH ED GCS ADULT SCORE',
        'R DPH ED GCS NUMERIC SCORE',
        'ZSFG R ADULT OR PEDIATIRC GCS TRIGGER ROW?',
        'R ED CLINICAL CALCULATOR - GLASGOW COMA SCALE SCORE',
    ],
    'RASS': [
        'R RICHMOND AGITATION SEDATION SCALE (RASS)',
        'R RICHMOND AGITATION SEDATION SCALE',
        'ED RASS',
        'DPH CV RASS',
    ],
    'Pain Score': [
        'DPH R PAIN SCORE',
        'R IP PAIN SCORE',
        'PAIN SCORE (CATEGORY LIST)',
        'R PATIENT ENTERED PAIN SCORE (CATEGORY LIST)',
    ],

    # --- Respiratory ---
    'FiO2': ['R FIO2 (%)', 'R FIO2', 'R FIO2 2'],
    'O2 Therapy': [
        'R OXYGEN THERAPY',
        'ED R O2 DEVICE',
        'UCSF R AN O2 DELIVERY DEVICE',
        'UCSF R AN O2 DELIVERY VENTILATION',
    ],
    'PEEP': [
        'R RESP PEEP/CPAP SET',
        'R VENT PEEP',
        'R DPH RT PEEP MEASURED',
    ],
    'Vent Mode': [
        'R RT VENT MODE',
        'R RT MODE/BREATH TYPE',
        'R RT (ADULT) VENT TYPE',
        'R IP VENT MODE',
        'R AN VENT MODE',
    ],
    'Vent RR': [
        'R RT VENTILATOR RESPIRATORY RATE  TOTAL',
        'R RESP RR SET',
    ],
    'Tidal Volume': [
        'R RT (ADULT) VENTILATOR TIDAL VOLUME EXHALED',
        'R VENT TIDAL VOLUME SET',
        'R VENT TIDAL VOLUME SPONT',
        'R DPH TIDAL VOLUME MECH',
        'R DPH TIDAL VOLUME ML/KG MECH',
        'R DPH RT TIDAL VOLUME SPONT',
    ],
    'Minute Volume': [
        'R RT VENTILATOR MINUTE VOLUME MEASURED V2',
        'R DPH MINUTE VOLUME MEASURED',
        'R DPH MINUTE VOLUME SPONT',
    ],
    'I:E Ratio': [
        'R RT (ADULT) VENTILATOR I:E RATIO MEASURED',
        'R VENT I:E RATIO',
    ],
    'PaO2/FiO2 Ratio': ['R DPH RT PAO2/FIO2 RATIO', 'R PAO2/FIO2 RATIO'],
    'Pressure Support': ['R DPH PRESS SUPPORTABOVEPEEP', 'R RT VENTILATOR PRESSURE SUPPORT'],
    'OSI': ['R OXYGEN SATURATION INDEX (OSI)'],
    'EtCO2': [
        'R AN ETCO2',
        'UCSF R ANE END TIDAL CO2',
        'R RT ETCO2',
        'R VENT ETCO2',
    ],
    'Resp Rate EtCO2': ['R ZSFG RESP RATE ETCO2'],
    'CPOT': ['R CPOT VENT OR VOCAL COMPLIANCE IP_CD_UCSF'],

    # --- Labs ---
    'WBC': ['WBC Count'],
    'Hemoglobin': ['Hemoglobin', 'POCT, Total Hemoglobin', 'Total Hemoglobin',
                   'Hemoglobin, Plasma, Alternative Method'],
    'Platelet Count': ['Platelet Count'],
    'Creatinine': ['Creatinine', 'Creatinine, Serum', 'Creatinine (POCT)',
                   'Creatinine,Whole Blood'],
    'BUN': ['BUN', 'Urea Nitrogen', 'Urea Nitrogen, serum/plasma'],
    'Sodium': ['Sodium', 'Sodium, Serum / Plasma'],
    'Potassium': ['Potassium', 'Potassium, Serum / Plasma'],
    'Calcium': ['Calcium', 'Calcium, total, Serum / Plasma'],
    'Ionized Calcium': ['Ionized Calcium', 'Ionized Calcium, whole blood',
                        'Calcium, Ionized, whole blood', 'Calcium, Ionized, Serum/Plasma'],
    'Chloride': ['Chloride', 'Chloride, Serum / Plasma'],
    'CO2 (Bicarb)': ['CO2', 'Bicarbonate'],
    'Glucose': ['Glucose, Non Fasting', 'Glucose, Fingerstick', 'Glucose WB',
                'Glucose', 'Glucose, non-fasting'],
    'Anion Gap': ['Anion Gap (No K)', 'Anion Gap'],
    'Lactate': ['Lactate, whole blood', 'Lactate, plasma', 'Lactate, Whole Blood',
                'Lactate, Venous'],
    'PO2': ['PO2', 'pO2', 'PO2, Venous', 'Venous pO2'],
    'Bilirubin (Total)': ['Bilirubin, Total', 'Total Bilirubin'],
    'Bilirubin (Direct)': ['Bilirubin, Direct'],
    'AST': ['AST'],
    'ALT': ['ALT', 'Alanine transaminase'],
    'Alkaline Phosphatase': ['Alkaline Phosphatase'],
    'Total Protein': ['Total Protein', 'Protein, Total, Serum / Plasma'],
    'Albumin': ['Albumin', 'Albumin, Serum g/dL', 'Albumin, Serum / Plasma'],
    'eGFR': ['eGFR Low Estimate', 'eGFR High Estimate',
             'eGFR - low estimate', 'eGFR - high estimate'],
    'Hemoglobin A1C': ['Hemoglobin A1C', 'Hemoglobin A1c'],
    'Hematocrit': ['Hematocrit'],
    'MCV': ['MCV'],
    'RBC Count': ['RBC Count'],
    'MCHC': ['MCHC'],
    'MCH': ['MCH'],
    # Differentials
    'Absolute Neutrophils': ['Absolute Neutrophils', 'Abs Neutrophils'],
    'Absolute Basophils': ['Absolute Basophils', 'Abs Basophils'],
    'Absolute Eosinophils': ['Absolute Eosinophils', 'Abs Eosinophils'],
    'Absolute Lymphocytes': ['Absolute Lymphocytes', 'Abs Lymphocytes'],
    'Absolute Monocytes': ['Absolute Monocytes', 'Abs Monocytes'],
    'Absolute Large Lymphocytes': ['Absolute Large Lymphocytes'],
    # Coagulation
    'INR': ['INR'],
    'Prothrombin Time': ['Prothrombin Time'],
    'Activated PTT': ['Activated PTT', 'aPTT'],
    'Fibrinogen': ['Fibrinogen', 'Fibrinogen, Functional'],
    # Cardiac
    'Troponin I': ['Troponin I', 'Troponin I High Sensitivity (NG/L)'],
    # Inflammatory
    'Procalcitonin': ['Procalcitonin'],
    'CRP': ['CRP,  High sensitivity', 'C-Reactive Protein'],
    # Electrolytes (additional)
    'Magnesium': ['Magnesium', 'Magnesium, Serum / Plasma'],
    'Phosphorus': ['Phosphorus', 'Phosphorus, serum/plasma'],
}


def build_reverse_map() -> Dict[str, str]:
    """Build raw_name -> concept_name reverse lookup"""
    reverse = {}
    for concept, raw_names in CONCEPT_MAP.items():
        for raw in raw_names:
            reverse[raw] = concept
    return reverse


def parse_args():
    parser = argparse.ArgumentParser(
        description='Analyze most commonly recorded time-varying covariates',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        '--input-dir', '-i', action='append', dest='input_dirs',
        help='Input data directory (can specify multiple with -i dir1 -i dir2)'
    )
    parser.add_argument(
        '--top', '-n', type=int, default=50,
        help='Number of top covariates to display (default: 50)'
    )
    parser.add_argument(
        '--output', '-o', type=str, default='top_50_covariates.csv',
        help='Output CSV filename (default: top_50_covariates.csv)'
    )
    return parser.parse_args()


def get_site_name(folder_path: Path) -> str:
    name = folder_path.name
    if 'UCSF' in name:
        return 'UCSF'
    elif 'SFDPH' in name:
        return 'SFDPH'
    return name


def load_parquet_safe(filepath: Path) -> pd.DataFrame:
    if not filepath.exists():
        return pd.DataFrame()
    return pd.read_parquet(filepath)


def get_raw_stats(data_dir: Path, site: str) -> pd.DataFrame:
    """Get per-raw-variable stats for a site. Returns columns:
    [raw_name, data_type, site, records, encounters]
    """
    all_parts = []

    # Labs
    labs = load_parquet_safe(data_dir / 'labs.parquet')
    if not labs.empty:
        labs.columns = labs.columns.str.lower()
        stats = labs.groupby('componentname').agg(
            records=('componentname', 'size'),
            encounters=('encounterkey', 'nunique'),
        ).reset_index().rename(columns={'componentname': 'raw_name'})
        stats['data_type'] = 'Laboratory'
        all_parts.append(stats)

    # Flowsheet assess
    assess = load_parquet_safe(data_dir / 'flowsheet_assess.parquet')
    if not assess.empty:
        assess.columns = assess.columns.str.lower()
        stats = assess.groupby('flowsheetrowname').agg(
            records=('flowsheetrowname', 'size'),
            encounters=('encounterkey', 'nunique'),
        ).reset_index().rename(columns={'flowsheetrowname': 'raw_name'})
        stats['data_type'] = 'Vital Signs'
        all_parts.append(stats)

    # Flowsheet resp
    resp = load_parquet_safe(data_dir / 'flowsheet_resp.parquet')
    if not resp.empty:
        resp.columns = resp.columns.str.lower()
        stats = resp.groupby('flowsheetrowname').agg(
            records=('flowsheetrowname', 'size'),
            encounters=('encounterkey', 'nunique'),
        ).reset_index().rename(columns={'flowsheetrowname': 'raw_name'})
        stats['data_type'] = 'Respiratory'
        all_parts.append(stats)

    if not all_parts:
        return pd.DataFrame()

    result = pd.concat(all_parts, ignore_index=True)
    result['site'] = site
    return result


def build_concept_table(all_raw_stats: List[pd.DataFrame]) -> pd.DataFrame:
    """Aggregate raw stats into clinical concepts with per-site detail."""
    reverse_map = build_reverse_map()
    combined = pd.concat(all_raw_stats, ignore_index=True)

    # Map raw names to concepts; unmapped names become their own concept
    combined['concept'] = combined['raw_name'].map(reverse_map).fillna(combined['raw_name'])

    # Determine data_type per concept (take the first non-null)
    concept_types = combined.groupby('concept')['data_type'].first().to_dict()

    sites = sorted(combined['site'].unique())
    rows = []

    for concept, grp in combined.groupby('concept'):
        row = {
            'concept': concept,
            'data_type': concept_types[concept],
        }
        total_records = 0
        total_encounters = 0

        for site in sites:
            site_grp = grp[grp['site'] == site]
            site_records = int(site_grp['records'].sum())
            site_encounters = int(site_grp['encounters'].sum())
            # Collect the raw names actually present at this site
            site_names = sorted(site_grp.loc[site_grp['records'] > 0, 'raw_name'].tolist())

            row[f'{site}_records'] = site_records
            row[f'{site}_encounters'] = site_encounters
            row[f'{site}_names'] = '; '.join(site_names) if site_names else ''

            total_records += site_records
            total_encounters += site_encounters

        row['total_records'] = total_records
        row['total_encounters'] = total_encounters
        rows.append(row)

    result = pd.DataFrame(rows)
    result = result.sort_values('total_records', ascending=False).reset_index(drop=True)
    return result


def print_results(df: pd.DataFrame, top_n: int):
    if df.empty:
        print("No data found!")
        return

    sites = [c.replace('_records', '') for c in df.columns
             if c.endswith('_records') and c != 'total_records']

    n = min(top_n, len(df))
    print("\n" + "=" * 140)
    print(f"TOP {n} TIME-VARYING COVARIATES BY CLINICAL CONCEPT")
    print("=" * 140)

    header = f"{'Rank':<5} {'Clinical Concept':<25} {'Type':<12}"
    for site in sites:
        header += f" {site + ' Records':>14}"
    header += f" {'Total':>14} {'Encounters':>12}"
    print(header)
    print("-" * 140)

    for idx, row in df.head(n).iterrows():
        line = f"{idx+1:<5} {row['concept'][:24]:<25} {row['data_type']:<12}"
        for site in sites:
            line += f" {int(row[f'{site}_records']):>14,}"
        line += f" {int(row['total_records']):>14,} {int(row['total_encounters']):>12,}"
        print(line)

        # Print site-specific names indented below
        for site in sites:
            names = row.get(f'{site}_names', '')
            if names:
                print(f"{'':>5}   {site}: {names}")

    print("-" * 140)
    print(f"\nTotal clinical concepts: {len(df)}")
    print(f"Total records across all concepts: {df['total_records'].sum():,}")


def main():
    args = parse_args()

    if args.input_dirs:
        data_dirs = [Path(d) for d in args.input_dirs]
    else:
        data_dirs = DEFAULT_DATA_DIRS

    print("=" * 70)
    print("SEPSIS R01 TIME-VARYING COVARIATE ANALYSIS")
    print("=" * 70)

    valid_dirs = []
    for d in data_dirs:
        if d.exists():
            print(f"  Found: {d}")
            valid_dirs.append(d)
        else:
            print(f"  Missing: {d}")

    if not valid_dirs:
        print("\nERROR: No valid data directories found!")
        sys.exit(1)

    # Collect raw stats per site
    all_raw = []
    for data_dir in valid_dirs:
        site = get_site_name(data_dir)
        print(f"\nLoading {site}...")
        raw = get_raw_stats(data_dir, site)
        if not raw.empty:
            print(f"  {len(raw)} raw variables, {raw['records'].sum():,.0f} total records")
            all_raw.append(raw)

    # Build concept-level table
    print("\nMapping to clinical concepts...")
    result = build_concept_table(all_raw)

    # Print
    print_results(result, args.top)

    # Save CSV — order columns logically
    sites = sorted({s for df in all_raw for s in df['site'].unique()})
    col_order = ['concept', 'data_type']
    for site in sites:
        col_order += [f'{site}_names', f'{site}_records', f'{site}_encounters']
    col_order += ['total_records', 'total_encounters']
    # Only keep columns that exist
    col_order = [c for c in col_order if c in result.columns]
    result[col_order].to_csv(args.output, index=False)
    print(f"\n  Saved to: {args.output}")

    print("\n" + "=" * 70)
    print("ANALYSIS COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
