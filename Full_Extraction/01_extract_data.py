#!/usr/bin/env python3
"""
Sepsis R01 Data Extraction Script
Uses DuckDB to query Parquet files and extract data to CSV files

Usage:
    python 01_extract_data.py                              # Extract from deid_cdw (UCSF)
    python 01_extract_data.py --data-asset deid_cdw_sfdph  # Extract from SFDPH
    python 01_extract_data.py --data-asset all             # Extract from both
"""

import os
import sys
import argparse
import pandas as pd
import duckdb
from datetime import datetime

# SFDPH Lab Component Keys
# Verified by querying raw SFDPH labcomponentresultfact joined with ED encounters
# Old 16xxx-17xxx range keys were wrong; correct keys are in 2000-9000 range
SFDPH_LAB_KEYS = {
    # CBC
    'WBC Count': 2810,
    'Hemoglobin': 3165,
    'Hematocrit': 3815,
    'MCV': 4459,
    'RBC Count': 2654,
    'MCHC': 4458,
    'MCH': 4457,
    'Platelet Count': 6230,
    # Auto Differentials
    'Absolute Neutrophils': 3866,
    'Absolute Basophils': 3173,
    'Absolute Eosinophils': 3172,
    'Absolute Lymphocytes': 3170,
    'Absolute Monocytes': 3171,
    'Absolute Large Lymphocytes': 8192,
    # BMP
    'Creatinine, Serum': 5744,
    'Potassium': 3097,
    'Sodium': 3099,
    'Calcium': 3908,
    'BUN': 3911,
    'CO2': 3063,
    'Chloride': 3100,
    'Glucose, Non Fasting': 2235,
    'Glucose, Fingerstick': 9989,
    'Glucose WB': 2581,
    'Anion Gap (No K)': 8181,
    # Liver Panel
    'ALT': 8176,
    'AST': 3060,
    'Alkaline Phosphatase': 3053,
    'Total Bilirubin': 3095,
    'Bilirubin, Direct': 3941,
    'Total Protein': 3094,
    'Albumin, Serum g/dL': 6273,
    # Blood Gas / Lactate
    'Lactate, Whole Blood': 17901,
    'Lactate, Venous': 9964,
    'pO2': 8426,
    'Venous pO2': 3916,
    # Renal
    'eGFR Low Estimate': 18016,
    'eGFR High Estimate': 18017,
    # Coagulation
    'INR': 8379,
    'Prothrombin Time': 17997,
    'Activated PTT': 17998,
    'Fibrinogen': 4173,
    # Cardiac
    'Troponin I': 3132,
    'Troponin I High Sensitivity': 18007,
    # Inflammatory
    'Procalcitonin': 1734,
    'CRP, High sensitivity': 3873,
    # Electrolytes (additional)
    'Magnesium': 3745,
    'Phosphorus': 8421,
    'Ionized Calcium': 3909,
    # Other
    'Hemoglobin A1C': 3082,
}

# UCSF Lab Component Keys
# Existing keys from Sepsis_R01_LabFlow_2023-11-29.xlsx plus newly discovered keys
# from querying raw UCSF labcomponentresultfact
UCSF_LAB_KEYS = {
    # CBC
    'WBC Count': 994,
    'Hemoglobin': 5345,
    'Hematocrit': 5346,
    'RBC Count': 970,
    'MCV': 950,
    'MCH': 948,
    'MCHC': 949,
    'Platelet Count': 967,
    'POCT, Total Hemoglobin': 32195,
    'Hemoglobin, Plasma, Alternative Method': 21555,
    'POCT, Total Hemoglobin (Oximetry)': 21968,
    # Auto Differentials
    'Abs Eosinophils': 926,
    'Abs Basophils': 910,
    'Abs Lymphocytes': 944,
    'Abs Neutrophils': 959,
    'Abs Monocytes': 955,
    # BMP
    'Creatinine': 586,
    'Creatinine (POCT)': 587,
    'Sodium': 5276,
    'Potassium': 5274,
    'Calcium': 6110,
    'Urea Nitrogen': 5238,
    'Bicarbonate': 569,
    'Chloride': 5277,
    'Glucose': 4399,
    'Anion Gap': 563,
    # Liver Panel
    'AST': 5236,
    'Alanine transaminase': 5230,
    'Alkaline Phosphatase': 5229,
    'Bilirubin, Total': 7497,
    'Bilirubin, Direct': 12120,
    'Total Protein': 5271,
    'Albumin': 2444,
    # Blood Gas / Lactate
    'Lactate, blood': 653,
    'Lactate, plasma': 5975,
    'PO2': 683,
    'PO2, Venous': 25090,
    # Renal
    'eGFR High Estimate': 594,
    'eGFR Low Estimate': 595,
    # Coagulation
    'INR': 759,
    'aPTT': 2420,
    'Prothrombin Time': 49399,
    'Fibrinogen, Functional': 6042,
    # Cardiac
    'Troponin I': 2467,
    # Inflammatory
    'Procalcitonin': 25350,
    'CRP': 5993,
    # Electrolytes (additional)
    'Magnesium': 5948,
    'Phosphorus': 5886,
    # Ionized Calcium
    'Calcium, Ionized, whole blood': 573,
    'Calcium, Ionized, Serum/Plasma': 2458,
    # Other
    'Hemoglobin A1c': 2469,
}

# SFDPH Flowsheet Row Keys (different from UCSF)
# These were identified by querying the raw SFDPH flowsheetvaluefact table
# Organized into Assess (vitals) and Resp (respiratory) types, matching UCSF structure
SFDPH_FLOWSHEET_KEYS = {
    # --- Assess (Vitals) ---
    'Temp': 29366,
    'BP': 27705,
    'SpO2': 2,                          # PULSE OXIMETRY
    'Pulse': 33278,
    'Resp': 33957,
    'MAP': 18911,
    'HR-ECG': 1479,                     # R AN HEART RATE ECG
    'Weight/Scale': 9079,
    'GCS Eye': 14847,                   # R DPH ED GCS ADULT BEST EYE RESPONSE
    'GCS Verbal': 14848,                # R DPH ED GCS ADULT BEST VERBAL RESPONSE
    'GCS Motor': 14849,                 # R DPH ED GCS ADULT BEST MOTOR RESPONSE
    'GCS Score': 14850,                 # R DPH ED GCS ADULT SCORE
    'GCS Numeric Score': 14855,         # R DPH ED GCS NUMERIC SCORE
    'Pulse from SpO2': 34185,           # R ZSFG PULSE FROM SPO2
    'MAP A-Line': 18900,
    'MAP A-Line 2': 24384,
    'Pulse from Art Line': 34223,       # R ZSFG PULSE FROM ART LINE
    'Pulse Ox Type': 20691,             # R PULSE OXIMETRY TYPE
    'RASS': 21357,                      # R RICHMOND AGITATION SEDATION SCALE
    'ED RASS': 15204,                   # ED RASS
    'DPH CV RASS': 50318,              # DPH CV RASS (cardiac)
    'Pain Score': 21066,                # DPH R PAIN SCORE
    'Pain Score (Category)': 15593,     # PAIN SCORE (CATEGORY LIST)
    'GCS Trigger': 23619,               # ZSFG R ADULT OR PEDIATIRC GCS TRIGGER ROW?
    'ED GCS Calculator': 13093,         # R ED CLINICAL CALCULATOR - GLASGOW COMA SCALE SCORE
    'Art Line BP': 18901,               # R ARTERIAL LINE BLOOD PRESSURE
    'Art Line BP 2': 18903,             # R ARTERIAL LINE BLOOD PRESSURE 2
    # --- Respiratory ---
    'O2': 1421,
    'Vent Mode (AN)': 1427,             # R AN VENT MODE (anesthesia)
    'etCO2': 1442,
    'Resp Rate-Vent': 1615,
    'FiO2': 18934,                      # R FIO2
    'Vent PEEP': 18942,                 # R VENT PEEP
    'Vent Tidal Volume Set': 18938,     # R VENT TIDAL VOLUME SET
    'Vent Tidal Volume Spont': 24757,   # R VENT TIDAL VOLUME SPONT
    'Vent I:E Ratio': 26144,            # R VENT I:E RATIO
    'IP Vent Mode': 20251,              # R IP VENT MODE
    'PEEP Measured': 32956,             # R DPH RT PEEP MEASURED
    'PEEP/CPAP Set': 24764,             # R RESP PEEP/CPAP SET
    'RR Set': 24772,                    # R RESP RR SET
    'Tidal Volume Mech': 33036,         # R DPH TIDAL VOLUME MECH
    'Tidal Volume ML/KG Mech': 33037,   # R DPH TIDAL VOLUME ML/KG MECH
    'Tidal Volume Spont (DPH)': 33079,  # R DPH RT TIDAL VOLUME SPONT
    'Minute Volume Measured': 33038,    # R DPH MINUTE VOLUME MEASURED
    'Minute Volume Spont': 33081,       # R DPH MINUTE VOLUME SPONT
    'PaO2/FiO2 Ratio': 33524,          # R DPH RT PAO2/FIO2 RATIO
    'Press Support': 33071,             # R DPH PRESS SUPPORTABOVEPEEP
    'Oxygen Therapy': 22553,            # R OXYGEN THERAPY
    'Vent EtCO2': 18944,               # R VENT ETCO2
    'Resp Rate EtCO2': 19,             # R ZSFG RESP RATE ETCO2
}

# UCSF Flowsheet Row Keys
# Existing keys from Sepsis_R01_LabFlow_2023-11-29.xlsx plus newly discovered keys
# from querying raw UCSF flowsheetvaluefact
UCSF_FLOWSHEET_KEYS = {
    # --- Assess (Vitals) ---
    'Weight/Scale': 5106,
    'Respirations': 39413,
    'Blood Pressure': 32710,
    'Temperature': 34432,
    'Pulse': 38524,
    'Pulse - Palpated or Pleth': 8369,
    'MAP': 16489,
    'MAP A-Line': 16478,
    'Art Line BP': 16479,               # R ARTERIAL LINE BLOOD PRESSURE
    'Art Line BP 2': 16481,             # R ARTERIAL LINE BLOOD PRESSURE 2
    'GCS Eye': 8550,                    # R CPN ADULT GLASGOW COMA SCALE EYE OPENING
    'GCS Verbal': 30983,                # R CPN ADULT GLASGOW COMA SCALE BEST VERBAL RESPONSE
    'GCS Motor': 8554,                  # R CPN GLASGOW COMA SCALE BEST MOTOR RESPONSE
    'GCS Score': 30984,                 # R CPN GLASGOW COMA SCALE SCORE
    'GCS Score V2': 105926,             # R CPN GLASGOW COMA SCALE SCORE V2
    'Pain Score': 18412,                # R IP PAIN SCORE
    'Pain Score (Category)': 9258,      # PAIN SCORE (CATEGORY LIST)
    'RASS': 30034,                      # R RICHMOND AGITATION SEDATION SCALE
    # --- Respiratory ---
    'FiO2': 16511,
    'FiO2 2': 41880,
    'PEEP/CPAP Set': 28730,
    'RT Vent Mode': 35186,              # R RT VENT MODE
    'RT Mode/Breath Type': 34086,       # R RT MODE/BREATH TYPE
    'RT Vent RR Total': 36316,          # R RT VENTILATOR RESPIRATORY RATE TOTAL
    'RT MAP Measured': 19958,           # RT MAP MEASURED_IP_CD_UCSF
    'RT Adult Vent Type': 36279,        # R RT (ADULT) VENT TYPE
    'RT Minute Volume V2': 41884,       # R RT VENTILATOR MINUTE VOLUME MEASURED V2
    'RT Tidal Volume Exhaled': 36317,   # R RT (ADULT) VENTILATOR TIDAL VOLUME EXHALED
    'RT I:E Ratio': 36324,              # R RT (ADULT) VENTILATOR I:E RATIO MEASURED
    'OSI': 47494,                       # R OXYGEN SATURATION INDEX (OSI)
    'RR Set': 28738,                    # R RESP RR SET
    'O2 Device (ED)': 8653,             # ED R O2 DEVICE
    'O2 Delivery Device': 100298,       # UCSF R AN O2 DELIVERY DEVICE
    'O2 Delivery Ventilation': 100297,  # UCSF R AN O2 DELIVERY VENTILATION
    'Pressure Support': 36304,          # R RT VENTILATOR PRESSURE SUPPORT
    'PaO2/FiO2 Ratio': 47495,          # R PAO2/FIO2 RATIO
    'EtCO2 (AN)': 2300,                # UCSF R ANE END TIDAL CO2
    'EtCO2 (RT)': 36568,               # R RT ETCO2
    'CPOT': 41883,                      # R CPOT VENT OR VOCAL COMPLIANCE IP_CD_UCSF
}

# Data asset configurations
DATA_ASSETS = {
    'deid_cdw': {
        'name': 'UCSF DeID CDW',
        'path': '/media/ubuntu/HDD Storage/parquet/deid_cdw',
        'folder_suffix': 'UCSF',
        'ed_department': 'EMERGENCY DEPT PARN',
        'has_icu_registry': True,
        'has_death_registry': True,
        'has_sdoh': True,
        'lab_keys_source': 'ucsf_dict',
        'flowsheet_keys_source': 'ucsf_dict',
    },
    'deid_cdw_sfdph': {
        'name': 'SFDPH DeID CDW',
        'path': '/media/ubuntu/HDD Storage/parquet/deid_cdw_sfdph',
        'folder_suffix': 'SFDPH',
        'ed_department': 'ED',
        'has_icu_registry': False,
        'has_death_registry': False,
        'has_sdoh': False,
        'lab_keys_source': 'sfdph_dict',  # Use SFDPH_LAB_KEYS dict
        'flowsheet_keys_source': 'sfdph_dict',  # Use SFDPH_FLOWSHEET_KEYS dict
    }
}


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Extract Sepsis R01 data from DeID CDW parquet files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python 01_extract_data.py                              # UCSF only (default)
    python 01_extract_data.py --data-asset deid_cdw_sfdph  # SFDPH only
    python 01_extract_data.py --data-asset all             # Both UCSF and SFDPH
        """
    )
    parser.add_argument(
        '--data-asset', '-d',
        choices=['deid_cdw', 'deid_cdw_sfdph', 'all'],
        default='deid_cdw',
        help='Data asset to extract from (default: deid_cdw)'
    )
    parser.add_argument(
        '--memory-gb', '-m',
        type=int,
        default=400,
        help='Memory limit for DuckDB in GB (default: 400)'
    )
    parser.add_argument(
        '--threads', '-t',
        type=int,
        default=24,
        help='Number of threads for DuckDB (default: 24)'
    )
    parser.add_argument(
        '--output-dir', '-o',
        type=str,
        default='.',
        help='Base output directory for extracted data (default: current directory)'
    )
    return parser.parse_args()


def find_column(df, col_name):
    """
    Find a column in a DataFrame by name (case-insensitive)

    Args:
        df: pandas DataFrame
        col_name: Column name to find (case-insensitive)

    Returns:
        Actual column name if found, None otherwise
    """
    for col in df.columns:
        if col.lower() == col_name.lower():
            return col
    return None

# Global variables set by run_extraction()
folder_path = None
today = None
asset_config = None


def configure_duckdb(memory_gb: int, num_threads: int):
    """Configure DuckDB settings"""
    print("\n=== Configuring DuckDB ===")
    duckdb.execute(f"""
        SET temp_directory = 'temp_dir.tmp';
        SET preserve_insertion_order = false;
        SET memory_limit = '{memory_gb}GB';
        SET threads TO {num_threads};
    """)
    print(f"✓ Memory limit: {memory_gb}GB")
    print(f"✓ Threads: {num_threads}")
    print(f"✓ Temp directory: temp_dir.tmp")

def register_tables(data_asset='deid_cdw'):
    """
    Register all parquet tables from the specified data asset

    Args:
        data_asset: Name of the data asset (default: 'deid_cdw')
                   Options: deid_cdw, deid_cdw_sfdph

    Returns:
        List of registered table names
    """
    # Skip sample tables and non-data directories
    SKIP_TABLES = {
        'encounterfact_0p1percent',  # Sample table
        'fmreportproperty',          # Report metadata, not patient data
    }

    config = DATA_ASSETS.get(data_asset)
    if not config:
        print(f"ERROR: Unknown data asset: {data_asset}")
        sys.exit(1)

    data_asset_path = config['path']

    if not os.path.exists(data_asset_path):
        print(f"ERROR: Data asset path not found: {data_asset_path}")
        sys.exit(1)

    print(f"\n=== Registering Tables from {config['name']} ===")
    table_list = os.listdir(data_asset_path)

    registered_count = 0
    skipped_count = 0
    for table in table_list:
        if table in SKIP_TABLES:
            skipped_count += 1
            continue
        table_path = f'{data_asset_path}/{table}/*.parquet'
        if os.path.exists(f'{data_asset_path}/{table}'):
            try:
                table_df = duckdb.read_parquet(table_path)
                duckdb.register(table, table_df)
                registered_count += 1
            except Exception as e:
                print(f"  Warning: Could not register table '{table}': {e}")

    print(f"Registered {registered_count} tables from {data_asset} (skipped {skipped_count})")
    return table_list

def load_lab_flowsheet_keys(excel_filepath):
    """Load lab and flowsheet keys from Excel file"""
    print("\n=== Loading Lab and Flowsheet Keys from Excel ===")

    if not os.path.exists(excel_filepath):
        print(f"✗ ERROR: Excel file not found: {excel_filepath}")
        sys.exit(1)

    # Read lab keys
    lab_keys = pd.read_excel(excel_filepath, sheet_name='lab_keys')
    print(f"✓ Loaded {len(lab_keys)} lab keys")

    # Read flowsheet row keys
    flowsheet_row_keys = pd.read_excel(excel_filepath, sheet_name='flowsheet_row_keys')
    print(f"✓ Loaded {len(flowsheet_row_keys)} flowsheet row keys")

    return lab_keys, flowsheet_row_keys

def create_encounter_keys(ed_department: str):
    """Create encounter keys for all qualifying ED visits

    Args:
        ed_department: Name of the ED department to filter on
    """
    print("\n[1/17] Creating Encounter Keys...")
    print(f"  Filtering for ED department: {ed_department}")

    # Note: DuckDB uses different syntax than SQL Server
    # datediff -> date_diff with different argument order
    # We'll calculate age differently

    query = f"""
    SELECT DISTINCT
        ed.EncounterKey AS EncounterKey,
        ed.PatientKey AS PatientKey,
        ed.PatientDurableKey AS PatientDurableKey,
        ed.ArrivalDateKeyValue AS ArrivalDateKeyValue,
        ed.HospitalAdmissionKey AS HospitalAdmissionKey
    FROM edvisitfact ed
    -- join in patient information to enable restricting to adults
    INNER JOIN patientdim pd ON pd.durablekey = ed.PatientDurableKey
    -- join in patient location data to restrict to specific ED
    INNER JOIN patientlocationeventfact plef ON ed.EncounterKey = plef.Encounterkey
    -- join in department names
    INNER JOIN departmentdim dd ON dd.DepartmentKey = plef.DepartmentKey
    WHERE
        -- restrict to >=18 years old at arrival
        date_diff('year', pd.birthdate, ed.ArrivalDateKeyValue) >= 18
        -- select only current rows from PatientDim
        AND pd.iscurrent = 1
        -- restrict to encounters that have a location in the specified ED
        AND dd.DepartmentName = '{ed_department}'
    """

    enc_keys = duckdb.sql(query).df()

    # Debug: print column names
    print(f"  Column names: {enc_keys.columns.tolist()}")

    # Save to Parquet and CSV
    output_path = f"{folder_path}/enc_keys.parquet"
    enc_keys.to_parquet(output_path, index=False)
    enc_keys.to_csv(f"{folder_path}/enc_keys.csv", index=False)
    print(f"✓ Created {len(enc_keys):,} encounter keys")
    print(f"  Saved to: {output_path}")

    # Register as a view for subsequent queries
    duckdb.register('enc_keys_view', enc_keys)

    return enc_keys

def create_patient_keys(enc_keys):
    """Create patient keys from encounter keys"""
    print("\n[2/17] Creating Patient Keys...")

    # Column names might be lowercase from DuckDB
    cols = enc_keys.columns.tolist()
    patient_durable_col = 'patientdurablekey' if 'patientdurablekey' in cols else 'PatientDurableKey'
    patient_key_col = 'patientkey' if 'patientkey' in cols else 'PatientKey'

    pt_keys = enc_keys[[patient_durable_col, patient_key_col]].drop_duplicates()

    # Rename to match expected column names for R script
    pt_keys.columns = ['PatientDurableKey', 'PatientKey']

    # Save to Parquet
    output_path = f"{folder_path}/pt_keys.parquet"
    pt_keys.to_parquet(output_path, index=False)
    pt_keys.to_csv(f"{folder_path}/pt_keys.csv", index=False)
    print(f"✓ Created {len(pt_keys):,} patient keys")
    print(f"  Saved to: {output_path}")

    # Register as a view for subsequent queries
    duckdb.register('pt_keys_view', pt_keys)

    return pt_keys

def extract_duration_dim():
    """Extract Duration Dimension table"""
    print("\n[3/17] Extracting Duration Dimension...")

    query = """
    SELECT *
    FROM durationdim
    WHERE Years > -99 AND Years < 99
    """

    df = duckdb.sql(query).df()
    output_path = f"{folder_path}/dur.parquet"
    df.to_parquet(output_path, index=False)
    df.to_csv(f"{folder_path}/dur.csv", index=False)
    print(f"✓ Saved {len(df):,} rows to {output_path}")
    return df

def extract_imaging_data():
    """Extract imaging data"""
    print("\n[4/17] Extracting Imaging Data...")

    # All imaging procedures
    # Note: UCSF uses 'ORDERABLES', SFDPH uses 'PROCEDURES'
    query = """
    SELECT
        FirstProcedureName,
        FirstProcedureCategory,
        COUNT(*) AS Count
    FROM imagingfact
    INNER JOIN enc_keys_view
        ON imagingfact.EncounterKey = enc_keys_view.EncounterKey
    WHERE
        FirstProcedureCategory IN (
            'IMG CT ORDERABLES', 'IMG CT PROCEDURES',
            'IMG DIAGNOSTIC IMAGING ORDERABLES',
            'IMG FILM OVERREAD ORDERABLES',
            'IMG FILM STORAGE ORDERABLES',
            'IMG FLUOROSCOPY ORDERABLES', 'IMG FLUOROSCOPY PROCEDURES',
            'IMG IR ORDERABLES', 'IMG IR PROCEDURES',
            'IMG MRI ORDERABLES', 'IMG MRI PROCEDURES',
            'IMG XR PROCEDURES', 'IMG US PROCEDURES',
            'IMG NM PROCEDURES', 'IMG DEXA PROCEDURES'
        )
    GROUP BY FirstProcedureName, FirstProcedureCategory
    """
    img_all_proc = duckdb.sql(query).df()
    img_all_proc.to_parquet(f"{folder_path}/img_all_proc.parquet", index=False)
    img_all_proc.to_csv(f"{folder_path}/img_all_proc.csv", index=False)
    print(f"  Saved img_all_proc: {len(img_all_proc):,} rows")

    # Lung imaging procedures (summary)
    query = """
    SELECT
        FirstProcedureName,
        FirstProcedureCategory,
        COUNT(*) AS Count
    FROM imagingfact
    INNER JOIN enc_keys_view
        ON imagingfact.EncounterKey = enc_keys_view.EncounterKey
    WHERE
        LOWER(FirstProcedureName) LIKE '%chest%' OR
        LOWER(FirstProcedureName) LIKE '%lung%'
    GROUP BY FirstProcedureName, FirstProcedureCategory
    """
    img_lung_proc = duckdb.sql(query).df()
    img_lung_proc.to_parquet(f"{folder_path}/img_lung_proc.parquet", index=False)
    img_lung_proc.to_csv(f"{folder_path}/img_lung_proc.csv", index=False)
    print(f"  ✓ Saved img_lung_proc: {len(img_lung_proc):,} rows")

    # Lung imaging details
    query = """
    SELECT imagingfact.*
    FROM imagingfact
    INNER JOIN enc_keys_view
        ON imagingfact.EncounterKey = enc_keys_view.EncounterKey
    WHERE
        LOWER(FirstProcedureName) LIKE '%chest%' OR
        LOWER(FirstProcedureName) LIKE '%lung%'
    """
    img_lung = duckdb.sql(query).df()
    img_lung.to_parquet(f"{folder_path}/img_lung.parquet", index=False)
    img_lung.to_csv(f"{folder_path}/img_lung.csv", index=False)
    print(f"  ✓ Saved img_lung: {len(img_lung):,} rows")

    return img_all_proc, img_lung_proc, img_lung

def extract_encounter_data():
    """Extract encounter data"""
    print("\n[5/17] Extracting Encounter Data...")

    query = """
    SELECT encounterfact.*
    FROM encounterfact
    INNER JOIN enc_keys_view
    ON encounterfact.EncounterKey = enc_keys_view.EncounterKey
    """

    df = duckdb.sql(query).df()
    df.to_parquet(f"{folder_path}/enc.parquet", index=False)
    df.to_csv(f"{folder_path}/enc.csv", index=False)
    print(f"✓ Saved enc: {len(df):,} rows")
    return df

def extract_encounter_diagnosis_data():
    """Extract encounter data with primary diagnosis"""
    print("\n[6/17] Extracting Encounter Data with Primary Diagnosis...")

    query = """
    SELECT encounterfact.*,
        dtd.Value as icd_Value,
        dtd.DiagnosisName as icd_diagnosisname,
        dtd.DisplayString as icd_displaystring,
        dtd.l1_name AS icd_l1_name,
        dtd.l2_name AS icd_l2_name,
        dtd.l3_name AS icd_l3_name,
        dtd.l4_name AS icd_l4_name,
        dtd.l5_name AS icd_l5_name,
        dtd.l6_name AS icd_l6_name,
        dtd.l7_name AS icd_l7_name,
        dtd.l8_name AS icd_l8_name,
        dtd.l9_name AS icd_l9_name
    FROM encounterfact
    INNER JOIN enc_keys_view
    ON encounterfact.EncounterKey = enc_keys_view.EncounterKey
    LEFT JOIN diagnosisterminologydim as dtd
    ON encounterfact.PrimaryDiagnosisKey = dtd.DiagnosisKey
    AND dtd.Type='ICD-10-CM'
    """

    df = duckdb.sql(query).df()
    df.to_parquet(f"{folder_path}/enc_dx.parquet", index=False)
    df.to_csv(f"{folder_path}/enc_dx.csv", index=False)
    print(f"✓ Saved enc_dx: {len(df):,} rows")
    return df

def extract_patient_data():
    """Extract patient data"""
    print("\n[7/17] Extracting Patient Data...")

    query = """
    SELECT patientdim.*
    FROM patientdim
    INNER JOIN pt_keys_view
    ON patientdim.PatientKey = pt_keys_view.PatientKey
    """

    df = duckdb.sql(query).df()
    df.to_parquet(f"{folder_path}/pt.parquet", index=False)
    df.to_csv(f"{folder_path}/pt.csv", index=False)
    print(f"✓ Saved pt: {len(df):,} rows")
    return df

def extract_ed_data():
    """Extract ED visit data"""
    print("\n[8/17] Extracting ED Visit Data...")

    query = """
    SELECT edvisitfact.*
    FROM edvisitfact
    INNER JOIN enc_keys_view
    ON edvisitfact.EncounterKey = enc_keys_view.EncounterKey
    """

    df = duckdb.sql(query).df()
    df.to_parquet(f"{folder_path}/ed.parquet", index=False)
    df.to_csv(f"{folder_path}/ed.csv", index=False)
    print(f"✓ Saved ed: {len(df):,} rows")
    return df

def extract_hospital_admission_data():
    """Extract hospital admission data"""
    print("\n[9/17] Extracting Hospital Admission Data...")

    query = """
    SELECT hospitaladmissionfact.*
    FROM hospitaladmissionfact
    INNER JOIN enc_keys_view
    ON hospitaladmissionfact.EncounterKey = enc_keys_view.EncounterKey
    """

    df = duckdb.sql(query).df()
    df.to_parquet(f"{folder_path}/hosp_adm.parquet", index=False)
    df.to_csv(f"{folder_path}/hosp_adm.csv", index=False)
    print(f"✓ Saved hosp_adm: {len(df):,} rows")
    return df

def extract_icu_data(has_icu_registry: bool = True):
    """Extract ICU stay registry data

    Args:
        has_icu_registry: Whether the data asset has ICU registry table
    """
    print("\n[10/18] Extracting ICU Stay Registry Data...")

    if not has_icu_registry:
        print("  ⚠ ICU registry table not available for this data asset - skipping")
        return None

    query = """
    SELECT icustayregistrydatamart.*
    FROM icustayregistrydatamart
    INNER JOIN enc_keys_view
    ON icustayregistrydatamart.EncounterKey = enc_keys_view.EncounterKey
    """

    df = duckdb.sql(query).df()
    df.to_parquet(f"{folder_path}/icu_reg.parquet", index=False)
    df.to_csv(f"{folder_path}/icu_reg.csv", index=False)
    print(f"✓ Saved icu_reg: {len(df):,} rows")
    return df

def extract_diagnosis_events():
    """Extract all diagnosis events for selected encounters"""
    print("\n[11/17] Extracting Diagnosis Event Data...")

    query = """
    SELECT diagnosiseventfact.*,
            dtd.Type AS icd_type,
            dtd.Value AS icd_value,
            dtd.l1_name AS icd_l1_name,
            dtd.l2_name AS icd_l2_name,
            dtd.l3_name AS icd_l3_name,
            dtd.l4_name AS icd_l4_name,
            dtd.l5_name AS icd_l5_name,
            dtd.l6_name AS icd_l6_name,
            dtd.l7_name AS icd_l7_name,
            dtd.l8_name AS icd_l8_name,
            dtd.l9_name AS icd_l9_name
    FROM diagnosiseventfact
    INNER JOIN enc_keys_view
    ON diagnosiseventfact.EncounterKey = enc_keys_view.EncounterKey
    LEFT JOIN diagnosisterminologydim as dtd
    ON diagnosiseventfact.DiagnosisKey = dtd.DiagnosisKey
    WHERE dtd.Type='ICD-10-CM'
    ORDER BY diagnosiseventfact.PatientDurableKey, diagnosiseventfact.StartDateKeyValue, diagnosiseventfact.DiagnosisKey
    """

    df = duckdb.sql(query).df()

    # Sort by PatientDurableKey, StartDateKeyValue, DiagnosisKey
    # This replicates the logic from process_data.Rmd
    sort_cols = []
    for col in ['PatientDurableKey', 'StartDateKeyValue', 'DiagnosisKey']:
        # Handle both lowercase and PascalCase column names
        actual_col = col if col in df.columns else col.lower()
        if actual_col in df.columns:
            sort_cols.append(actual_col)

    if sort_cols:
        df = df.sort_values(sort_cols)

    df.to_parquet(f"{folder_path}/dx_enc_icd.parquet", index=False)
    df.to_csv(f"{folder_path}/dx_enc_icd.csv", index=False)
    print(f"✓ Saved dx_enc_icd: {len(df):,} rows")
    return df

def extract_antibiotics():
    """Extract antibiotic medications"""
    print("\n[12/17] Extracting Antibiotic Medications...")

    query = """
    SELECT medicationadministrationfact.*
    FROM medicationadministrationfact
    INNER JOIN enc_keys_view
        ON medicationadministrationfact.EncounterKey = enc_keys_view.EncounterKey
    WHERE MedicationTherapeuticClass='ANTIBIOTICS'
    """

    df = duckdb.sql(query).df()
    df.to_parquet(f"{folder_path}/med_ab.parquet", index=False)
    df.to_csv(f"{folder_path}/med_ab.csv", index=False)
    print(f"✓ Saved med_ab: {len(df):,} rows")
    return df

def extract_vasopressors():
    """Extract vasopressor medications"""
    print("\n[13/17] Extracting Vasopressor Medications...")

    # First get the medication codes
    med_code_query = """
    SELECT *
    FROM medicationcodedim
    WHERE (
        UPPER(MedicationName) LIKE '%NOREPINEPHRINE%' OR
        UPPER(MedicationName) LIKE '%VASOPRESSIN%' OR
        UPPER(MedicationName) LIKE '%EPINEPHRINE%' OR
        UPPER(MedicationName) LIKE '%DOBUTAMINE%' OR
        UPPER(MedicationName) LIKE '%MILRINONE%' OR
        UPPER(MedicationName) LIKE '%PHENYLEPHRINE%' OR
        UPPER(MedicationName) LIKE '%DOPAMINE%')
    """
    med_vp_code = duckdb.sql(med_code_query).df()

    print(f"  Found {len(med_vp_code)} total vasopressor medications")
    print(f"  Available columns: {med_vp_code.columns.tolist()}")

    # Find column names (case-insensitive)
    route_col = find_column(med_vp_code, 'MedicationRoute')
    med_key_col = find_column(med_vp_code, 'MedicationKey')

    if not med_key_col:
        raise ValueError(f"MedicationKey column not found. Available columns: {med_vp_code.columns.tolist()}")

    # Filter to IV only if MedicationRoute column exists
    if route_col:
        # Case-insensitive matching for IV routes (includes 'Intravenous', 'intravenous', 'injection')
        iv_mask = med_vp_code[route_col].str.lower().isin(['intravenous', 'injection'])
        med_vp_code_iv = med_vp_code[iv_mask]
        print(f"  Filtering by {route_col} in ['intravenous', 'injection'] (case-insensitive)")
        print(f"  Found {len(med_vp_code_iv)} IV vasopressor medications")
    else:
        print(f"  Warning: MedicationRoute column not found, using all medications")
        med_vp_code_iv = med_vp_code

    med_vp_keys = med_vp_code_iv[[med_key_col]].drop_duplicates()
    # Rename to standard case for consistency
    med_vp_keys.columns = ['MedicationKey']

    print(f"  Using {len(med_vp_keys)} vasopressor medication keys")

    # Register as view
    duckdb.register('med_vp_keys_view', med_vp_keys)

    # Pull medication administration data
    query = """
    SELECT medicationadministrationfact.*
    FROM medicationadministrationfact
    INNER JOIN enc_keys_view
    ON medicationadministrationfact.EncounterKey = enc_keys_view.EncounterKey
    INNER JOIN med_vp_keys_view
    ON medicationadministrationfact.MedicationKey = med_vp_keys_view.MedicationKey
    """

    df = duckdb.sql(query).df()
    df.to_parquet(f"{folder_path}/med_vp.parquet", index=False)
    df.to_csv(f"{folder_path}/med_vp.csv", index=False)
    print(f"✓ Saved med_vp: {len(df):,} rows")
    return df

def extract_flowsheet_data(flowsheet_row_keys):
    """Extract flowsheet data

    Args:
        flowsheet_row_keys: Either a DataFrame with flowsheet keys (for UCSF/Excel) or None (for SFDPH/dict)
    """
    print("\n[14/17] Extracting Flowsheet Data...")

    # Determine flowsheet keys based on data asset configuration
    flowsheet_keys_source = asset_config.get('flowsheet_keys_source', 'excel')

    if flowsheet_keys_source == 'sfdph_dict':
        # Use SFDPH flowsheet keys dictionary
        assess_key_names = [
            'Temp', 'BP', 'SpO2', 'Pulse', 'Resp', 'MAP', 'HR-ECG',
            'Weight/Scale', 'GCS Eye', 'GCS Verbal', 'GCS Motor',
            'GCS Score', 'GCS Numeric Score', 'Pulse from SpO2',
            'MAP A-Line', 'MAP A-Line 2', 'Pulse from Art Line',
            'Pulse Ox Type', 'RASS', 'ED RASS', 'DPH CV RASS',
            'Pain Score', 'Pain Score (Category)', 'GCS Trigger',
            'ED GCS Calculator', 'Art Line BP', 'Art Line BP 2',
        ]
        resp_key_names = [
            'O2', 'Vent Mode (AN)', 'etCO2', 'Resp Rate-Vent',
            'FiO2', 'Vent PEEP', 'Vent Tidal Volume Set',
            'Vent Tidal Volume Spont', 'Vent I:E Ratio', 'IP Vent Mode',
            'PEEP Measured', 'PEEP/CPAP Set', 'RR Set',
            'Tidal Volume Mech', 'Tidal Volume ML/KG Mech',
            'Tidal Volume Spont (DPH)', 'Minute Volume Measured',
            'Minute Volume Spont', 'PaO2/FiO2 Ratio', 'Press Support',
            'Oxygen Therapy', 'Vent EtCO2', 'Resp Rate EtCO2',
        ]
        assess_keys_list = [SFDPH_FLOWSHEET_KEYS[k] for k in assess_key_names]
        resp_keys_list = [SFDPH_FLOWSHEET_KEYS[k] for k in resp_key_names]
        print(f"  Using SFDPH flowsheet keys dictionary")
        assess_keys_df = pd.DataFrame({'FlowsheetRowKey': assess_keys_list})
        resp_keys_df = pd.DataFrame({'FlowsheetRowKey': resp_keys_list})
    elif flowsheet_keys_source == 'ucsf_dict':
        # Use UCSF flowsheet keys dictionary
        assess_key_names = [
            'Weight/Scale', 'Respirations', 'Blood Pressure', 'Temperature',
            'Pulse', 'Pulse - Palpated or Pleth', 'MAP', 'MAP A-Line',
            'Art Line BP', 'Art Line BP 2',
            'GCS Eye', 'GCS Verbal', 'GCS Motor',
            'GCS Score', 'GCS Score V2',
            'Pain Score', 'Pain Score (Category)', 'RASS',
        ]
        resp_key_names = [
            'FiO2', 'FiO2 2', 'PEEP/CPAP Set',
            'RT Vent Mode', 'RT Mode/Breath Type', 'RT Vent RR Total',
            'RT MAP Measured', 'RT Adult Vent Type', 'RT Minute Volume V2',
            'RT Tidal Volume Exhaled', 'RT I:E Ratio', 'OSI', 'RR Set',
            'O2 Device (ED)', 'O2 Delivery Device', 'O2 Delivery Ventilation',
            'Pressure Support', 'PaO2/FiO2 Ratio',
            'EtCO2 (AN)', 'EtCO2 (RT)', 'CPOT',
        ]
        assess_keys_list = [UCSF_FLOWSHEET_KEYS[k] for k in assess_key_names]
        resp_keys_list = [UCSF_FLOWSHEET_KEYS[k] for k in resp_key_names]
        print(f"  Using UCSF flowsheet keys dictionary")
        assess_keys_df = pd.DataFrame({'FlowsheetRowKey': assess_keys_list})
        resp_keys_df = pd.DataFrame({'FlowsheetRowKey': resp_keys_list})
    else:
        # Use Excel-based flowsheet keys (fallback)
        type_col = find_column(flowsheet_row_keys, 'Type')
        flowsheet_key_col = find_column(flowsheet_row_keys, 'FlowsheetRowKey')

        if not type_col or not flowsheet_key_col:
            raise ValueError(f"Required columns not found. Available: {flowsheet_row_keys.columns.tolist()}")

        assess_keys = flowsheet_row_keys[flowsheet_row_keys[type_col] == 'Assess'][flowsheet_key_col].drop_duplicates()
        assess_keys_df = pd.DataFrame({'FlowsheetRowKey': assess_keys})

        resp_keys = flowsheet_row_keys[flowsheet_row_keys[type_col] == 'Resp'][flowsheet_key_col].drop_duplicates()
        resp_keys_df = pd.DataFrame({'FlowsheetRowKey': resp_keys})

    # Extract Assess flowsheet data
    print("  Extracting Assess flowsheet data...")
    print(f"    Using {len(assess_keys_df)} assess flowsheet keys")
    duckdb.register('flowsheet_assess_keys', assess_keys_df)

    query = """
    SELECT fvf.*
    FROM flowsheetvaluefact AS fvf
    INNER JOIN enc_keys_view AS es
    ON fvf.encounterkey = es.encounterkey
    INNER JOIN flowsheet_assess_keys AS frk
    ON frk.FlowsheetRowKey = fvf.FlowsheetRowKey
    """

    flowsheet_assess = duckdb.sql(query).df()
    flowsheet_assess.to_parquet(f"{folder_path}/flowsheet_assess.parquet", index=False)
    flowsheet_assess.to_csv(f"{folder_path}/flowsheet_assess.csv", index=False)
    print(f"  ✓ Saved flowsheet_assess: {len(flowsheet_assess):,} rows")

    # Extract Resp flowsheet data
    print("  Extracting Resp flowsheet data...")
    print(f"    Using {len(resp_keys_df)} resp flowsheet keys")
    duckdb.register('flowsheet_resp_keys', resp_keys_df)

    query = """
    SELECT fvf.*
    FROM flowsheetvaluefact AS fvf
    INNER JOIN enc_keys_view AS es
    ON fvf.encounterkey = es.encounterkey
    INNER JOIN flowsheet_resp_keys AS frk
    ON frk.FlowsheetRowKey = fvf.FlowsheetRowKey
    """

    flowsheet_resp = duckdb.sql(query).df()
    flowsheet_resp.to_parquet(f"{folder_path}/flowsheet_resp.parquet", index=False)
    flowsheet_resp.to_csv(f"{folder_path}/flowsheet_resp.csv", index=False)
    print(f"  ✓ Saved flowsheet_resp: {len(flowsheet_resp):,} rows")

    return flowsheet_assess, flowsheet_resp

def extract_labs(lab_keys):
    """Extract lab data

    Args:
        lab_keys: Either a DataFrame with lab keys (for UCSF/Excel) or None (for SFDPH/dict)
    """
    print("\n[15/17] Extracting Lab Data...")

    # Determine lab keys based on data asset configuration
    lab_keys_source = asset_config.get('lab_keys_source', 'excel')

    if lab_keys_source == 'sfdph_dict':
        # Use SFDPH lab keys dictionary
        lab_component_keys = pd.DataFrame({
            'LabComponentKey': list(SFDPH_LAB_KEYS.values())
        })
        print(f"  Using SFDPH lab keys dictionary ({len(lab_component_keys)} keys)")
    elif lab_keys_source == 'ucsf_dict':
        # Use UCSF lab keys dictionary
        lab_component_keys = pd.DataFrame({
            'LabComponentKey': list(UCSF_LAB_KEYS.values())
        })
        print(f"  Using UCSF lab keys dictionary ({len(lab_component_keys)} keys)")
    else:
        # Use Excel-based lab keys (fallback)
        lab_comp_col = find_column(lab_keys, 'LabComponentResultLabComponentKey')

        if not lab_comp_col:
            raise ValueError(f"LabComponentResultLabComponentKey column not found. Available: {lab_keys.columns.tolist()}")

        lab_component_keys = lab_keys[[lab_comp_col]].rename(
            columns={lab_comp_col: 'LabComponentKey'}
        ).drop_duplicates()

    print(f"  Filtering for {len(lab_component_keys)} lab component keys...")

    # Register as view
    duckdb.register('lab_component_keys_view', lab_component_keys)

    query = """
    SELECT lab.*
    FROM labcomponentresultfact AS lab
    INNER JOIN enc_keys_view AS es
    ON lab.encounterkey = es.encounterkey
    INNER JOIN lab_component_keys_view AS lck
    ON lab.LabComponentKey = lck.LabComponentKey
    """

    df = duckdb.sql(query).df()
    df.to_parquet(f"{folder_path}/labs.parquet", index=False)
    df.to_csv(f"{folder_path}/labs.csv", index=False)
    print(f"✓ Saved labs: {len(df):,} rows")
    return df

def extract_death_registry(has_death_registry: bool = True):
    """Extract death registry data

    Args:
        has_death_registry: Whether the data asset has death registry table
    """
    print("\n[16/18] Extracting Death Registry Data...")

    if not has_death_registry:
        print("  ⚠ Death registry table not available for this data asset - skipping")
        return None

    query = """
    SELECT pdr.*
    FROM patientdeathregistrydimx pdr
    INNER JOIN enc_keys_view
    ON pdr.PatientDurableKey = enc_keys_view.PatientDurableKey
    """

    df = duckdb.sql(query).df()
    df.to_parquet(f"{folder_path}/death.parquet", index=False)
    df.to_csv(f"{folder_path}/death.csv", index=False)
    print(f"✓ Saved death: {len(df):,} rows")
    return df

def extract_insurance():
    """Extract insurance/coverage data"""
    print("\n[17/18] Extracting Insurance Data...")

    query = """
    SELECT es.EncounterKey, cov.*
    FROM coveragedim AS cov
    INNER JOIN hospitaladmissionfact AS haf
        ON haf.PrimaryCoverageKey = cov.CoverageKey
    INNER JOIN enc_keys_view AS es
        ON haf.EncounterKey = es.EncounterKey
    """

    df = duckdb.sql(query).df()
    df.to_parquet(f"{folder_path}/insurance.parquet", index=False)
    df.to_csv(f"{folder_path}/insurance.csv", index=False)
    print(f"✓ Saved insurance: {len(df):,} rows")
    return df


def extract_sdoh(has_sdoh: bool = True):
    """Extract Social Determinants of Health (SDOH) data

    Args:
        has_sdoh: Whether the data asset has SDOH tables

    Returns:
        Tuple of (sdoh_fact, sdoh_answer, sdoh_domain) DataFrames, or (None, None, None) if unavailable
    """
    print("\n[18/18] Extracting SDOH Data...")

    if not has_sdoh:
        print("  ⚠ SDOH tables not available for this data asset - skipping")
        return None, None, None

    # Extract main SDOH fact table (encounter-level)
    query = """
    SELECT sdf.*
    FROM socialdeterminantfact sdf
    INNER JOIN enc_keys_view
    ON sdf.EncounterKey = enc_keys_view.EncounterKey
    """
    sdoh_fact = duckdb.sql(query).df()
    sdoh_fact.to_parquet(f"{folder_path}/sdoh_fact.parquet", index=False)
    sdoh_fact.to_csv(f"{folder_path}/sdoh_fact.csv", index=False)
    print(f"  ✓ Saved sdoh_fact: {len(sdoh_fact):,} rows")

    # Extract SDOH answers (linked to SDOH facts)
    if len(sdoh_fact) > 0:
        # Get the socialdeterminantkey values from our cohort
        sdoh_keys = sdoh_fact['socialdeterminantkey'].dropna().unique().tolist()
        if len(sdoh_keys) > 0:
            # Register sdoh keys as a view for the join
            sdoh_keys_df = pd.DataFrame({'socialdeterminantkey': sdoh_keys})
            duckdb.register('sdoh_keys_view', sdoh_keys_df)

            query = """
            SELECT sda.*
            FROM socialdeterminantanswerfact sda
            INNER JOIN sdoh_keys_view
            ON sda.socialdeterminantkey = sdoh_keys_view.socialdeterminantkey
            """
            sdoh_answer = duckdb.sql(query).df()
            sdoh_answer.to_parquet(f"{folder_path}/sdoh_answer.parquet", index=False)
            sdoh_answer.to_csv(f"{folder_path}/sdoh_answer.csv", index=False)
            print(f"  ✓ Saved sdoh_answer: {len(sdoh_answer):,} rows")
        else:
            sdoh_answer = None
            print(f"  ⚠ No SDOH answers found - skipping sdoh_answer.parquet")
    else:
        sdoh_answer = None
        print(f"  ⚠ No SDOH facts found - skipping sdoh_answer.parquet")

    # Extract SDOH patient domain (patient-level summary)
    query = """
    SELECT sdpd.*
    FROM socialdeterminantpatientdomainfact sdpd
    INNER JOIN pt_keys_view
    ON sdpd.PatientDurableKey = pt_keys_view.PatientDurableKey
    """
    sdoh_domain = duckdb.sql(query).df()
    sdoh_domain.to_parquet(f"{folder_path}/sdoh_domain.parquet", index=False)
    sdoh_domain.to_csv(f"{folder_path}/sdoh_domain.csv", index=False)
    print(f"  ✓ Saved sdoh_domain: {len(sdoh_domain):,} rows")

    # Print summary of SDOH domains found
    if len(sdoh_fact) > 0:
        domain_col = 'socialdeterminantdomain' if 'socialdeterminantdomain' in sdoh_fact.columns else None
        if domain_col:
            print(f"\n  SDOH Domains found:")
            for domain, count in sdoh_fact[domain_col].value_counts().head(10).items():
                print(f"    - {domain}: {count:,}")

    return sdoh_fact, sdoh_answer, sdoh_domain


def run_extraction(data_asset: str, config: dict, lab_keys: pd.DataFrame,
                   flowsheet_row_keys: pd.DataFrame, output_dir: str = '.'):
    """Run extraction for a single data asset

    Args:
        data_asset: Name of the data asset (e.g., 'deid_cdw')
        config: Configuration dict for this data asset
        lab_keys: DataFrame with lab component keys
        flowsheet_row_keys: DataFrame with flowsheet row keys
        output_dir: Base directory for output (default: current directory)
    """
    global folder_path, today, asset_config

    asset_config = config
    today = datetime.now().strftime("%Y-%m-%d")
    folder_name = f"Data_All_{config['folder_suffix']}_{today}"
    folder_path = os.path.join(output_dir, folder_name)
    os.makedirs(folder_path, exist_ok=True)

    print("\n" + "=" * 70)
    print(f"EXTRACTING FROM: {config['name']}")
    print("=" * 70)
    print(f"\n✓ Data will be extracted to: {folder_path}")
    print(f"✓ Date: {today}")

    # Register parquet tables
    register_tables(data_asset)

    # Create encounter and patient keys
    enc_keys = create_encounter_keys(config['ed_department'])
    pt_keys = create_patient_keys(enc_keys)

    # Extract all data (saves individual Parquet files + returns dataframes)
    dur = extract_duration_dim()
    img_all_proc, img_lung_proc, img_lung = extract_imaging_data()
    enc = extract_encounter_data()
    enc_dx = extract_encounter_diagnosis_data()
    pt = extract_patient_data()
    ed = extract_ed_data()
    hosp_adm = extract_hospital_admission_data()
    icu_reg = extract_icu_data(config['has_icu_registry'])
    dx_enc_icd = extract_diagnosis_events()
    med_ab = extract_antibiotics()
    med_vp = extract_vasopressors()
    flowsheet_assess, flowsheet_resp = extract_flowsheet_data(flowsheet_row_keys)
    labs = extract_labs(lab_keys)
    death = extract_death_registry(config['has_death_registry'])
    insurance = extract_insurance()
    sdoh_fact, sdoh_answer, sdoh_domain = extract_sdoh(config['has_sdoh'])

    # Save all tables to a single unified DuckDB file
    print("\n" + "=" * 70)
    print("Creating unified DuckDB file with all tables...")
    print("=" * 70)

    unified_db_path = f"{folder_path}/HTESepsis_{config['folder_suffix']}DeIDCDW_All_{today}.duckdb"
    unified_con = duckdb.connect(unified_db_path)

    # Build dict of tables to register (only include non-None tables)
    tables_to_register = {
        'enc_keys': enc_keys,
        'pt_keys': pt_keys,
        'dur': dur,
        'img_all_proc': img_all_proc,
        'img_lung_proc': img_lung_proc,
        'img_lung': img_lung,
        'enc': enc,
        'enc_dx': enc_dx,
        'pt': pt,
        'ed': ed,
        'hosp_adm': hosp_adm,
        'dx_enc_icd': dx_enc_icd,
        'med_ab': med_ab,
        'med_vp': med_vp,
        'flowsheet_assess': flowsheet_assess,
        'flowsheet_resp': flowsheet_resp,
        'labs': labs,
        'insurance': insurance,
    }

    # Add optional tables if they exist
    if lab_keys is not None:
        tables_to_register['lab_keys'] = lab_keys
    if flowsheet_row_keys is not None:
        tables_to_register['flowsheet_row_keys'] = flowsheet_row_keys
    if icu_reg is not None:
        tables_to_register['icu_reg'] = icu_reg
    if death is not None:
        tables_to_register['death'] = death
    if sdoh_fact is not None:
        tables_to_register['sdoh_fact'] = sdoh_fact
    if sdoh_answer is not None:
        tables_to_register['sdoh_answer'] = sdoh_answer
    if sdoh_domain is not None:
        tables_to_register['sdoh_domain'] = sdoh_domain

    # Register and create permanent tables
    for table_name, df in tables_to_register.items():
        unified_con.register(table_name, df)
        unified_con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {table_name}")

    unified_con.close()

    print(f"\n✓ Unified database saved to: {unified_db_path}")
    print(f"  Contains {len(tables_to_register)} tables with all extracted data")

    print("\n" + "=" * 70)
    print(f"✓ {config['name']} EXTRACTION COMPLETED SUCCESSFULLY")
    print("=" * 70)
    print(f"\nIndividual Parquet files saved to: {folder_path}/*.parquet")
    print(f"Unified database saved to: {unified_db_path}")

    return folder_path


def main():
    """Main extraction workflow"""
    args = parse_args()

    print("=" * 70)
    print("SEPSIS R01 DATA EXTRACTION - DuckDB/Parquet Version")
    print("=" * 70)

    # Configure DuckDB
    configure_duckdb(args.memory_gb, args.threads)

    # Check if any data asset still needs Excel keys
    if args.data_asset == 'all':
        assets_to_check = list(DATA_ASSETS.keys())
    else:
        assets_to_check = [args.data_asset]

    needs_excel = any(
        DATA_ASSETS[a].get('lab_keys_source') == 'excel' or
        DATA_ASSETS[a].get('flowsheet_keys_source') == 'excel'
        for a in assets_to_check
    )

    lab_keys, flowsheet_row_keys = None, None
    if needs_excel:
        excel_filepath = "Sepsis_R01_LabFlow_2023-11-29.xlsx"
        if not os.path.exists(excel_filepath):
            print(f"\n✗ ERROR: Excel file not found: {excel_filepath}")
            print("Please ensure the file is in the current directory.")
            sys.exit(1)
        lab_keys, flowsheet_row_keys = load_lab_flowsheet_keys(excel_filepath)
    else:
        print("\n=== Using dict-based keys for all data assets (Excel not needed) ===")

    # Determine which data assets to extract
    if args.data_asset == 'all':
        assets_to_extract = list(DATA_ASSETS.keys())
    else:
        assets_to_extract = [args.data_asset]

    print(f"\n✓ Will extract from: {', '.join(assets_to_extract)}")

    extracted_folders = []

    try:
        for data_asset in assets_to_extract:
            config = DATA_ASSETS[data_asset]
            output_folder = run_extraction(data_asset, config, lab_keys, flowsheet_row_keys, args.output_dir)
            extracted_folders.append(output_folder)

        # Final summary
        print("\n" + "=" * 70)
        print("ALL EXTRACTIONS COMPLETED SUCCESSFULLY")
        print("=" * 70)
        print("\nExtracted data folders:")
        for folder in extracted_folders:
            print(f"  - {folder}")
        print(f"\nTo read individual Parquet files in R:")
        print(f"  library(arrow); df <- read_parquet('<folder>/enc_keys.parquet')")
        print(f"\nTo read unified database in Python:")
        print(f"  import duckdb; con = duckdb.connect('<folder>/<db_file>.duckdb')")
        print(f"  enc_keys = con.execute('SELECT * FROM enc_keys').df()")

    except Exception as e:
        print(f"\n✗ ERROR during extraction: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
