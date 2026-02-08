#!/usr/bin/env python3
"""
Analyze dx_enc_icd.csv to extract:
1. Top 200 most common ICD codes with diagnosis names
2. All unique department names
"""

import pandas as pd
import re
from pathlib import Path

# Configuration
DATA_DIR = Path("/media/ubuntu/Archive/nick/Sepsis-R01/Data_All_UCSF_2025-12-22")
INPUT_FILE = DATA_DIR / "dx_enc_icd.csv"
OUTPUT_DIR = Path("/home/ubuntu/src/Sepsis-R01/Full_Extraction")

# Output files
TOP_ICD_OUTPUT = OUTPUT_DIR / "top_5000_icd_codes.csv"
DEPARTMENTS_OUTPUT = OUTPUT_DIR / "department_names.csv"


def get_sepsis_category(icd_code: str, diagnosis_name: str,
                         l1_name: str, l2_name: str, l3_name: str) -> str:
    """
    Categorize ICD codes into sepsis-related clinical categories.

    Returns one of:
    - 'sepsis_bacteremia': Direct sepsis/bacteremia/fungemia codes
    - 'infection_respiratory': Pneumonia, lung abscess, empyema
    - 'infection_urinary': UTI, pyelonephritis
    - 'infection_abdominal': Peritonitis, cholangitis, appendicitis, etc.
    - 'infection_skin': Cellulitis, abscess, necrotizing fasciitis
    - 'infection_other': CNS, bone/joint, endocarditis, device-related
    - 'organ_dysfunction': AKI, respiratory failure, hepatic failure, DIC
    - 'fever': Fever codes
    - 'shock': Shock states
    - None: Not sepsis-related
    """
    # Handle NaN values
    icd_code = str(icd_code).upper() if pd.notna(icd_code) else ''
    diagnosis_name = str(diagnosis_name).upper() if pd.notna(diagnosis_name) else ''
    l1_name = str(l1_name).upper() if pd.notna(l1_name) else ''
    l2_name = str(l2_name).upper() if pd.notna(l2_name) else ''
    l3_name = str(l3_name).upper() if pd.notna(l3_name) else ''

    all_text = f"{icd_code} {diagnosis_name} {l1_name} {l2_name} {l3_name}"

    # ==========================================================================
    # EXCLUSIONS
    # ==========================================================================
    if icd_code.startswith('Z'):  # History, status codes
        return None
    if icd_code.startswith('N18') or icd_code.startswith('N19'):  # Chronic kidney disease
        return None

    # ==========================================================================
    # SEPSIS / BACTEREMIA (direct codes)
    # ==========================================================================
    if re.match(r'^A4[01]', icd_code):  # A40-A41 Sepsis
        return 'sepsis_bacteremia'
    if re.match(r'^R65\.2', icd_code):  # Severe sepsis
        return 'sepsis_bacteremia'
    if icd_code.startswith('R65.1'):  # SIRS
        return 'sepsis_bacteremia'
    if icd_code.startswith('R78.81'):  # Bacteremia
        return 'sepsis_bacteremia'
    # Check for sepsis/bacteremia keywords, but exclude "ASEPTIC" (means no infection)
    if 'ASEPTIC' not in diagnosis_name:
        sepsis_keywords = ['SEPSIS', 'SEPTIC', 'BACTEREMIA', 'FUNGEMIA']
        for kw in sepsis_keywords:
            if kw in diagnosis_name:
                return 'sepsis_bacteremia'

    # ==========================================================================
    # INFECTION - RESPIRATORY
    # ==========================================================================
    resp_infection_prefixes = [
        'J09', 'J10', 'J11',  # Influenza
        'J12', 'J13', 'J14', 'J15', 'J16', 'J17', 'J18',  # Pneumonia
        'J20', 'J21', 'J22',  # Acute lower respiratory
        'J85', 'J86',  # Lung abscess, empyema
    ]
    for prefix in resp_infection_prefixes:
        if icd_code.startswith(prefix):
            return 'infection_respiratory'
    # Exclude cryptogenic organizing pneumonia (J84.116) - autoimmune, not infectious
    if 'PNEUMONIA' in all_text and 'CRYPTOGENIC' not in all_text:
        return 'infection_respiratory'
    if 'EMPYEMA' in all_text:
        return 'infection_respiratory'

    # ==========================================================================
    # INFECTION - URINARY
    # ==========================================================================
    uti_prefixes = ['N10', 'N30.0', 'N30.8', 'N30.9', 'N39.0']
    for prefix in uti_prefixes:
        if icd_code.startswith(prefix):
            return 'infection_urinary'
    if 'PYELONEPHRITIS' in all_text or 'URINARY TRACT INFECTION' in all_text:
        return 'infection_urinary'

    # ==========================================================================
    # INFECTION - ABDOMINAL
    # ==========================================================================
    abdominal_prefixes = [
        'K65',  # Peritonitis
        'K35',  # Appendicitis
        'K57.0', 'K57.2', 'K57.4', 'K57.8',  # Diverticulitis
        'K61', 'K63.0',  # GI abscess
        'K81.0',  # Acute cholecystitis
        'K83.0',  # Cholangitis
    ]
    for prefix in abdominal_prefixes:
        if icd_code.startswith(prefix):
            return 'infection_abdominal'
    abdominal_kw = ['PERITONITIS', 'CHOLANGITIS', 'APPENDICITIS', 'DIVERTICULITIS']
    for kw in abdominal_kw:
        if kw in all_text:
            return 'infection_abdominal'

    # ==========================================================================
    # INFECTION - SKIN/SOFT TISSUE
    # ==========================================================================
    # L05 split: L05.0 = pilonidal cyst WITH abscess (include), L05.9 = WITHOUT abscess (exclude)
    skin_prefixes = ['L00', 'L01', 'L02', 'L03', 'L04', 'L05.0', 'L08', 'M72.6']
    for prefix in skin_prefixes:
        if icd_code.startswith(prefix):
            return 'infection_skin'
    # Keywords need special handling to exclude negative phrases
    skin_kw_simple = ['CELLULITIS', 'NECROTIZING FASCIITIS']
    for kw in skin_kw_simple:
        if kw in all_text:
            return 'infection_skin'
    # ABSCESS keyword - exclude patterns like "WITHOUT ABSCESS", "WITHOUT ... ABSCESS"
    # (matches mastitis without abscess, pilonidal cyst without abscess, diverticulosis without perforation or abscess)
    if 'ABSCESS' in all_text and not re.search(r'WITHOUT\b.*\bABSCESS', all_text):
        return 'infection_skin'
    # GANGRENE keyword - exclude:
    # - "WITHOUT GANGRENE" patterns (hernias, diabetes without gangrene)
    # - I70 (atherosclerosis) - vascular gangrene, not infectious
    # - I73 (Raynaud's) - autoimmune/vascular, not infectious
    if 'GANGRENE' in all_text and not re.search(r'WITHOUT\b.*\bGANGRENE', all_text):
        # Exclude vascular/atherosclerotic gangrene (I70.x, I73.x)
        if not icd_code.startswith('I70') and not icd_code.startswith('I73'):
            return 'infection_skin'

    # ==========================================================================
    # INFECTION - OTHER (CNS, bone/joint, cardiac, device)
    # ==========================================================================
    # Exclusion for nonpyogenic meningitis (G03.0) - not bacterial
    if icd_code.startswith('G03.0'):
        return None
    other_infection_prefixes = [
        # Bacterial
        'A00', 'A01', 'A02', 'A03', 'A04', 'A05',
        'A20', 'A21', 'A22', 'A23', 'A24', 'A25', 'A26', 'A27', 'A28',
        'A30', 'A31', 'A32', 'A33', 'A34', 'A35', 'A36', 'A37', 'A38', 'A39',
        'A42', 'A43', 'A44', 'A46', 'A48', 'A49',
        # CNS
        'G00', 'G01', 'G02', 'G03', 'G04', 'G05', 'G06',
        # Bone/joint
        'M00', 'M86.0', 'M86.1', 'M86.2',
        # Cardiac
        'I30.1', 'I33', 'I40.0',
        # Device
        'T80.2', 'T82.6', 'T82.7', 'T83.5', 'T83.6', 'T84.5', 'T84.6', 'T84.7', 'T85.7',
    ]
    for prefix in other_infection_prefixes:
        if icd_code.startswith(prefix):
            return 'infection_other'
    # Exclude nonpyogenic meningitis from keyword matching
    if 'NONPYOGENIC' in all_text:
        pass  # Don't categorize as infection
    else:
        other_kw = ['MENINGITIS', 'ENCEPHALITIS', 'ENDOCARDITIS', 'OSTEOMYELITIS']
        for kw in other_kw:
            if kw in all_text:
                return 'infection_other'

    # ==========================================================================
    # SHOCK
    # ==========================================================================
    if icd_code.startswith('R57'):
        return 'shock'
    if 'SHOCK' in diagnosis_name:
        return 'shock'

    # ==========================================================================
    # ORGAN DYSFUNCTION (acute only)
    # ==========================================================================
    # AKI
    if icd_code.startswith('N17'):
        return 'organ_dysfunction'
    # Acute respiratory failure, ARDS
    if icd_code.startswith('J96.0') or icd_code.startswith('J96.9') or icd_code.startswith('J80'):
        return 'organ_dysfunction'
    # Acute hepatic failure
    if icd_code.startswith('K72.0') or icd_code.startswith('K72.9'):
        return 'organ_dysfunction'
    # DIC
    if icd_code.startswith('D65'):
        return 'organ_dysfunction'
    # Encephalopathy / AMS
    if icd_code == 'R41.82' or icd_code.startswith('G93.4'):
        return 'organ_dysfunction'
    # Lactic acidosis
    if icd_code == 'E87.2':
        return 'organ_dysfunction'
    organ_kw = ['ACUTE KIDNEY', 'ACUTE RENAL FAILURE', 'ACUTE RESPIRATORY FAILURE',
                'ACUTE HEPATIC', 'ACUTE LIVER FAILURE',
                'DISSEMINATED INTRAVASCULAR', 'LACTIC ACIDOSIS']
    for kw in organ_kw:
        if kw in all_text:
            return 'organ_dysfunction'
    # Short keywords need word boundaries to avoid false matches
    # DIC: avoid "EPISODIC", "PERIODIC", etc.
    # ARDS: avoid "BACKWARDS", "STANDARDS", etc.
    if re.search(r'\bDIC\b', all_text) or re.search(r'\bARDS\b', all_text):
        return 'organ_dysfunction'

    # ==========================================================================
    # FEVER
    # ==========================================================================
    if icd_code.startswith('R50'):
        return 'fever'
    # Exclude "chills without fever" (R68.83)
    if 'FEVER' in diagnosis_name and 'WITHOUT FEVER' not in diagnosis_name:
        return 'fever'

    return None


def analyze_icd_codes(df: pd.DataFrame) -> pd.DataFrame:
    """Extract top 5000 most common ICD codes with diagnosis names and sepsis categories."""
    print("Analyzing ICD codes...")

    # Group by ICD code and get counts, also capture diagnosis name
    # Use the most common diagnosis name for each ICD code
    icd_counts = (
        df.groupby('icd_value')
        .agg(
            count=('icd_value', 'size'),
            diagnosisname=('diagnosisname', lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else x.iloc[0]),
            icd_type=('icd_type', 'first'),
            icd_l1_name=('icd_l1_name', 'first'),
            icd_l2_name=('icd_l2_name', 'first'),
            icd_l3_name=('icd_l3_name', 'first'),
        )
        .reset_index()
        .sort_values('count', ascending=False)
        .head(5000)
    )

    # Add sepsis category classification
    icd_counts['sepsis_category'] = icd_counts.apply(
        lambda row: get_sepsis_category(
            row['icd_value'], row['diagnosisname'],
            row['icd_l1_name'], row['icd_l2_name'], row['icd_l3_name']
        ),
        axis=1
    )

    # Reorder columns for readability
    icd_counts = icd_counts[['icd_value', 'diagnosisname', 'count', 'sepsis_category',
                             'icd_type', 'icd_l1_name', 'icd_l2_name', 'icd_l3_name']]

    return icd_counts


def is_icu(dept_name: str) -> bool:
    """
    Determine if a department is an ICU.
    UCSF uses both ICU and ICC naming conventions (e.g., 13ICU, 9ICU, 10ICC, 6ICC).
    Also matches common ICU-related terms.
    """
    if pd.isna(dept_name):
        return False

    name_upper = dept_name.upper()

    # Pattern for UCSF-style ICU/ICC naming (e.g., 13ICU, 9ICU, 10ICC, 6ICC)
    if re.search(r'\d+\s*(ICU|ICC)', name_upper):
        return True

    # Common ICU-related keywords
    icu_keywords = [
        'ICU', 'ICC', 'INTENSIVE CARE', 'CRITICAL CARE',
        'MICU', 'SICU', 'PICU', 'NICU', 'CCU', 'CVICU',
        'BURN UNIT', 'NEURO ICU', 'CARDIAC ICU'
    ]

    for keyword in icu_keywords:
        if keyword in name_upper:
            return True

    return False


def analyze_departments(df: pd.DataFrame) -> pd.DataFrame:
    """Extract all unique department names with counts and ICU classification."""
    print("Analyzing departments...")

    # Get unique departments with counts and specialty info
    dept_counts = (
        df.groupby('departmentname')
        .agg(
            count=('departmentname', 'size'),
            departmentspecialty=('departmentspecialty', 'first')
        )
        .reset_index()
        .sort_values('count', ascending=False)
    )

    # Add ICU classification
    dept_counts['is_icu'] = dept_counts['departmentname'].apply(is_icu)

    return dept_counts


def main():
    print(f"Reading data from: {INPUT_FILE}")
    print("This may take a moment for large files...")

    # Read only the columns we need to save memory
    cols_needed = [
        'icd_value', 'diagnosisname', 'icd_type',
        'icd_l1_name', 'icd_l2_name', 'icd_l3_name',
        'departmentname', 'departmentspecialty'
    ]

    df = pd.read_csv(INPUT_FILE, usecols=cols_needed, low_memory=False)
    print(f"Loaded {len(df):,} rows")

    # Analyze ICD codes
    top_icd = analyze_icd_codes(df)
    top_icd.to_csv(TOP_ICD_OUTPUT, index=False)
    print(f"\nTop 5000 ICD codes saved to: {TOP_ICD_OUTPUT}")
    print(f"Total unique ICD codes in data: {df['icd_value'].nunique():,}")

    # Analyze departments
    departments = analyze_departments(df)
    departments.to_csv(DEPARTMENTS_OUTPUT, index=False)
    print(f"\nDepartment names saved to: {DEPARTMENTS_OUTPUT}")
    print(f"Total unique departments: {len(departments):,}")

    # Print summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)

    print("\nTop 10 ICD codes:")
    print(top_icd[['icd_value', 'diagnosisname', 'count', 'sepsis_category']].head(10).to_string(index=False))

    # Show sepsis category summary
    sepsis_related = top_icd[top_icd['sepsis_category'].notna()]
    print(f"\nSepsis-related ICD codes: {len(sepsis_related)} of {len(top_icd)}")

    # Category breakdown
    print("\n" + "="*60)
    print("SEPSIS CATEGORY BREAKDOWN (these become your ~9 dummy variables)")
    print("="*60)
    category_counts = top_icd['sepsis_category'].value_counts()
    for cat, count in category_counts.items():
        if cat is not None:
            print(f"  {cat}: {count} ICD codes")

    print("\nTop 5 codes per category:")
    for cat in ['sepsis_bacteremia', 'infection_respiratory', 'infection_urinary',
                'infection_abdominal', 'infection_skin', 'infection_other',
                'organ_dysfunction', 'shock', 'fever']:
        cat_df = top_icd[top_icd['sepsis_category'] == cat].head(5)
        if len(cat_df) > 0:
            print(f"\n{cat.upper()}:")
            print(cat_df[['icd_value', 'diagnosisname', 'count']].to_string(index=False))

    print("\nTop 10 departments:")
    print(departments[['departmentname', 'count', 'is_icu']].head(10).to_string(index=False))

    # Show ICU summary
    icu_depts = departments[departments['is_icu']]
    print(f"\nICU departments found: {len(icu_depts)}")
    print("\nAll ICU departments:")
    print(icu_depts[['departmentname', 'count']].to_string(index=False))


if __name__ == "__main__":
    main()
