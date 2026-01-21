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

# SFDPH Lab Component Keys (different from UCSF)
# These were identified by querying the raw SFDPH labcomponentresultfact table
SFDPH_LAB_KEYS = {
    'WBC Count': 17512,
    'Hemoglobin': 16682,
    'Hematocrit': 16678,
    'MCV': 16885,
    'RBC Count': 17221,
    'MCHC': 16884,
    'MCH': 16883,
    'Platelet Count': 17135,
    'Auto Abs Neutrophil': 16284,
    'Auto Abs Basophil': 16279,
    'Auto Abs Eosinophil': 16280,
    'Auto Abs Lymphocyte': 16282,
    'Auto Abs Monocyte': 16283,
    'Creatinine': 5744,
    'Potassium': 3097,
    'Sodium': 3099,
    'Calcium': 3908,
    'BUN': 3911,
    'CO2': 3063,
    'Chloride': 3100,
    'Glucose': 2235,
    'Albumin': 16215,
    'ALT': 16226,
    'Lactate': 2810,  # WBC in CSF but also Lactate
}

# SFDPH Flowsheet Row Keys (different from UCSF)
# These were identified by querying the raw SFDPH flowsheetvaluefact table
SFDPH_FLOWSHEET_KEYS = {
    # Vitals for assessment
    'Temp': 29366,
    'BP': 27705,
    'SpO2': 2,
    'Pulse': 33278,
    'Resp': 33957,
    'MAP': 18911,
    'HR-ECG': 1479,
    # Respiratory
    'O2': 1421,
    'Vent Mode': 1427,
    'etCO2': 1442,
    'Resp Rate-Vent': 1615,
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
        'lab_keys_source': 'excel',  # Use Excel file for lab keys
        'flowsheet_keys_source': 'excel',  # Use Excel file for flowsheet keys
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
        # Assess keys: Temp, BP, SpO2, Pulse, Resp, MAP, HR-ECG
        assess_keys_list = [
            SFDPH_FLOWSHEET_KEYS['Temp'],
            SFDPH_FLOWSHEET_KEYS['BP'],
            SFDPH_FLOWSHEET_KEYS['SpO2'],
            SFDPH_FLOWSHEET_KEYS['Pulse'],
            SFDPH_FLOWSHEET_KEYS['Resp'],
            SFDPH_FLOWSHEET_KEYS['MAP'],
            SFDPH_FLOWSHEET_KEYS['HR-ECG'],
        ]
        # Resp keys: O2, Vent Mode, etCO2, Resp Rate-Vent
        resp_keys_list = [
            SFDPH_FLOWSHEET_KEYS['O2'],
            SFDPH_FLOWSHEET_KEYS['Vent Mode'],
            SFDPH_FLOWSHEET_KEYS['etCO2'],
            SFDPH_FLOWSHEET_KEYS['Resp Rate-Vent'],
        ]
        print(f"  Using SFDPH flowsheet keys dictionary")
        assess_keys_df = pd.DataFrame({'FlowsheetRowKey': assess_keys_list})
        resp_keys_df = pd.DataFrame({'FlowsheetRowKey': resp_keys_list})
    else:
        # Use Excel-based flowsheet keys (UCSF)
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
    else:
        # Use Excel-based lab keys (UCSF)
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
        'lab_keys': lab_keys,
        'flowsheet_row_keys': flowsheet_row_keys,
    }

    # Add optional tables if they exist
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

    # Excel file path for lab and flowsheet keys
    excel_filepath = "Sepsis_R01_LabFlow_2023-11-29.xlsx"

    if not os.path.exists(excel_filepath):
        print(f"\n✗ ERROR: Excel file not found: {excel_filepath}")
        print("Please ensure the file is in the current directory.")
        sys.exit(1)

    # Load lab and flowsheet keys from Excel (shared across data assets)
    lab_keys, flowsheet_row_keys = load_lab_flowsheet_keys(excel_filepath)

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
