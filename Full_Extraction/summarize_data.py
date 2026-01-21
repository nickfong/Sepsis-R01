#!/usr/bin/env python3
"""
Community-Onset Lung Sepsis (COLS) Dataset Summary Script

This script generates a comprehensive clinical summary of the COLS dataset,
focused on early treatment decision support for:
- Antibiotics
- Fluid resuscitation
- Vasopressors

Aligned with Specific Aims 1 & 2:
- Aim 1: Develop robust CDSS model to predict individual treatment effects
- Aim 2: Validate CDSS in real-life clinical conditions

Author: Auto-generated for Sepsis R01 Study
Date: 2025
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import json

# Color codes for terminal output
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

def print_header(text):
    """Print formatted section header"""
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'=' * 80}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{text.center(80)}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'=' * 80}{Colors.END}\n")

def print_subheader(text):
    """Print formatted subsection header"""
    print(f"\n{Colors.BOLD}{Colors.CYAN}{text}{Colors.END}")
    print(f"{Colors.CYAN}{'-' * len(text)}{Colors.END}")

def format_number(n, decimals=0):
    """Format number with commas"""
    if pd.isna(n):
        return "N/A"
    if decimals == 0:
        return f"{int(n):,}"
    return f"{n:,.{decimals}f}"

def format_percentage(n, total, decimals=1):
    """Format percentage with count"""
    if total == 0:
        return "N/A"
    pct = (n / total) * 100
    return f"{format_number(n)} ({pct:.{decimals}f}%)"

def get_column(df, col_name):
    """
    Get column from DataFrame handling both lowercase and mixed-case names

    Args:
        df: pandas DataFrame
        col_name: Column name (case-insensitive)

    Returns:
        Actual column name if found, None otherwise
    """
    for col in df.columns:
        if col.lower() == col_name.lower():
            return col
    return None

def calculate_summary_stats(series, name="Variable"):
    """Calculate and format summary statistics for continuous variables"""
    if len(series) == 0:
        return f"{name}: No data"

    stats = {
        'count': len(series),
        'missing': series.isna().sum(),
        'mean': series.mean(),
        'std': series.std(),
        'min': series.min(),
        'q25': series.quantile(0.25),
        'median': series.median(),
        'q75': series.quantile(0.75),
        'max': series.max()
    }

    output = f"{Colors.BOLD}{name}:{Colors.END}\n"
    output += f"  N: {format_number(stats['count'])} (Missing: {format_number(stats['missing'])})\n"
    output += f"  Mean ± SD: {stats['mean']:.1f} ± {stats['std']:.1f}\n"
    output += f"  Median [IQR]: {stats['median']:.1f} [{stats['q25']:.1f} - {stats['q75']:.1f}]\n"
    output += f"  Range: {stats['min']:.1f} - {stats['max']:.1f}\n"

    return output

def load_data(data_folder):
    """
    Load all data from parquet files

    Args:
        data_folder: Path to folder containing parquet files

    Returns:
        Dictionary of dataframes
    """
    print_header("LOADING DATA")

    data_files = {
        'enc_keys': 'enc_keys.parquet',
        'pt_keys': 'pt_keys.parquet',
        'enc': 'enc.parquet',
        'enc_dx': 'enc_dx.parquet',
        'pt': 'pt.parquet',
        'ed': 'ed.parquet',
        'hosp_adm': 'hosp_adm.parquet',
        'icu_reg': 'icu_reg.parquet',
        'dx_enc_icd': 'dx_enc_icd.parquet',
        'med_ab': 'med_ab.parquet',
        'med_vp': 'med_vp.parquet',
        'flowsheet_assess': 'flowsheet_assess.parquet',
        'flowsheet_resp': 'flowsheet_resp.parquet',
        'labs': 'labs.parquet',
        'death': 'death.parquet',
        'insurance': 'insurance.parquet',
        'img_lung': 'img_lung.parquet',
        'img_lung_proc': 'img_lung_proc.parquet',
        'dur': 'dur.parquet'
    }

    data = {}
    for key, filename in data_files.items():
        filepath = os.path.join(data_folder, filename)
        if os.path.exists(filepath):
            try:
                df = pd.read_parquet(filepath)
                data[key] = df
                print(f"{Colors.GREEN}✓{Colors.END} Loaded {key}: {format_number(len(df))} rows")
            except Exception as e:
                print(f"{Colors.YELLOW}⚠{Colors.END} Warning: Could not load {key}: {e}")
                data[key] = pd.DataFrame()
        else:
            print(f"{Colors.YELLOW}⚠{Colors.END} Warning: File not found: {filename}")
            data[key] = pd.DataFrame()

    print(f"\n{Colors.GREEN}✓ Data loading complete{Colors.END}")
    return data

def summarize_cohort_overview(data):
    """Generate cohort overview statistics"""
    print_header("COHORT OVERVIEW")

    enc_keys = data['enc_keys']
    pt = data['pt']
    ed = data['ed']

    # Basic counts
    n_encounters = len(enc_keys)
    patient_durable_col = get_column(enc_keys, 'PatientDurableKey')
    n_patients = enc_keys[patient_durable_col].nunique() if patient_durable_col else 0

    print(f"{Colors.BOLD}Study Population:{Colors.END}")
    print(f"  Total Unique Patients: {format_number(n_patients)}")
    print(f"  Total ED Encounters: {format_number(n_encounters)}")
    if n_patients > 0:
        avg_encounters = n_encounters / n_patients
        print(f"  Encounters per Patient: {avg_encounters:.2f}")

    # Date range
    arrival_date_col = get_column(enc_keys, 'ArrivalDateKeyValue')
    if arrival_date_col:
        dates = pd.to_datetime(enc_keys[arrival_date_col])
        print(f"\n{Colors.BOLD}Data Time Period:{Colors.END}")
        print(f"  First Encounter: {dates.min().strftime('%Y-%m-%d')}")
        print(f"  Last Encounter: {dates.max().strftime('%Y-%m-%d')}")
        print(f"  Duration: {(dates.max() - dates.min()).days} days")

    # Demographics
    if not pt.empty:
        print_subheader("Demographics")

        # Age
        birthdate_col = get_column(pt, 'BirthDate')
        if birthdate_col:
            # Calculate age at current date
            birthdate = pd.to_datetime(pt[birthdate_col])
            age = (datetime.now() - birthdate).dt.days / 365.25
            print(calculate_summary_stats(age, "Age (years)"))

        # Sex
        sex_col = get_column(pt, 'Sex')
        if sex_col:
            print(f"\n{Colors.BOLD}Sex:{Colors.END}")
            sex_counts = pt[sex_col].value_counts()
            for sex, count in sex_counts.items():
                print(f"  {sex}: {format_percentage(count, len(pt))}")

        # Race
        race_col = get_column(pt, 'Race')
        if race_col:
            print(f"\n{Colors.BOLD}Race:{Colors.END}")
            race_counts = pt[race_col].value_counts()
            for race, count in race_counts.head(10).items():
                print(f"  {race}: {format_percentage(count, len(pt))}")

        # Ethnicity
        ethnic_col = get_column(pt, 'EthnicGroup')
        if ethnic_col:
            print(f"\n{Colors.BOLD}Ethnicity:{Colors.END}")
            eth_counts = pt[ethnic_col].value_counts()
            for eth, count in eth_counts.items():
                print(f"  {eth}: {format_percentage(count, len(pt))}")

def summarize_clinical_outcomes(data):
    """Summarize key clinical outcomes"""
    print_header("CLINICAL OUTCOMES")

    enc_keys = data['enc_keys']
    hosp_adm = data['hosp_adm']
    icu_reg = data['icu_reg']
    death = data['death']
    ed = data['ed']

    n_encounters = len(enc_keys)

    # Hospital Admissions
    print_subheader("Hospital Admissions")
    enc_key_col = get_column(hosp_adm, 'EncounterKey')
    n_admitted = hosp_adm[enc_key_col].nunique() if (not hosp_adm.empty and enc_key_col) else 0
    print(f"  Admitted from ED: {format_percentage(n_admitted, n_encounters)}")

    los_col = get_column(hosp_adm, 'LengthOfStayDays')
    if not hosp_adm.empty and los_col:
        los = hosp_adm[los_col]
        print(f"\n{Colors.BOLD}Hospital Length of Stay (days):{Colors.END}")
        print(calculate_summary_stats(los, "LOS"))

    # ICU Admissions
    print_subheader("ICU Admissions")
    icu_enc_col = get_column(icu_reg, 'EncounterKey')
    n_icu = icu_reg[icu_enc_col].nunique() if (not icu_reg.empty and icu_enc_col) else 0
    print(f"  ICU Admissions: {format_percentage(n_icu, n_encounters)}")
    if n_admitted > 0:
        print(f"  ICU Rate (among admitted): {format_percentage(n_icu, n_admitted)}")

    icu_los_col = get_column(icu_reg, 'IcuStayLengthDays')
    if not icu_reg.empty and icu_los_col:
        icu_los = icu_reg[icu_los_col]
        print(calculate_summary_stats(icu_los, "ICU Length of Stay"))

    # Mortality
    print_subheader("Mortality")
    death_pt_col = get_column(death, 'PatientDurableKey')
    n_deaths = death[death_pt_col].nunique() if (not death.empty and death_pt_col) else 0
    enc_pt_col = get_column(enc_keys, 'PatientDurableKey')
    n_patients = enc_keys[enc_pt_col].nunique() if enc_pt_col else 0
    print(f"  Deaths (from registry): {format_percentage(n_deaths, n_patients)}")

    # ED disposition
    ed_disp_col = get_column(ed, 'DischargeDispositionName')
    if not ed.empty and ed_disp_col:
        print(f"\n{Colors.BOLD}ED Disposition:{Colors.END}")
        disp_counts = ed[ed_disp_col].value_counts()
        for disp, count in disp_counts.head(10).items():
            print(f"  {disp}: {format_percentage(count, len(ed))}")

def summarize_sepsis_indicators(data):
    """Summarize sepsis-related diagnoses"""
    print_header("SEPSIS INDICATORS")

    enc_dx = data['enc_dx']
    dx_enc_icd = data['dx_enc_icd']

    # Primary diagnoses
    if not enc_dx.empty:
        print_subheader("Primary ED Diagnoses (Top 20)")

        dx_name_col = get_column(enc_dx, 'icd_diagnosisname')
        if dx_name_col:
            dx_counts = enc_dx[dx_name_col].value_counts()
            for dx, count in dx_counts.head(20).items():
                print(f"  {dx}: {format_number(count)}")

        # Look for sepsis-related codes
        print_subheader("Sepsis-Related Primary Diagnoses")
        sepsis_keywords = ['sepsis', 'septic', 'bacteremia', 'septicemia']

        if dx_name_col:
            for keyword in sepsis_keywords:
                mask = enc_dx[dx_name_col].str.contains(keyword, case=False, na=False)
                n_sepsis = mask.sum()
                if n_sepsis > 0:
                    print(f"  {keyword.title()}: {format_percentage(n_sepsis, len(enc_dx))}")

        # Pneumonia/lung infection
        print_subheader("Pneumonia/Lung Infection Diagnoses")
        lung_keywords = ['pneumonia', 'pneumonitis', 'lung infection', 'respiratory']

        if dx_name_col:
            for keyword in lung_keywords:
                mask = enc_dx[dx_name_col].str.contains(keyword, case=False, na=False)
                n_lung = mask.sum()
                if n_lung > 0:
                    print(f"  {keyword.title()}: {format_percentage(n_lung, len(enc_dx))}")

    # All diagnosis events
    if not dx_enc_icd.empty:
        print_subheader("All Diagnosis Events")
        print(f"  Total diagnosis events: {format_number(len(dx_enc_icd))}")
        dx_key_col = get_column(dx_enc_icd, 'DiagnosisKey')
        if dx_key_col:
            print(f"  Unique diagnoses: {format_number(dx_enc_icd[dx_key_col].nunique())}")

        # ICD-10 hierarchy analysis
        for level in range(1, 10):
            col = get_column(dx_enc_icd, f'icd_l{level}_name')
            if col:
                unique_categories = dx_enc_icd[col].nunique()
                if unique_categories > 0:
                    print(f"  ICD-10 Level {level} categories: {format_number(unique_categories)}")

def summarize_treatment_patterns(data):
    """Summarize antibiotic and vasopressor treatment patterns"""
    print_header("TREATMENT PATTERNS - Critical for CDSS Development")

    enc_keys = data['enc_keys']
    med_ab = data['med_ab']
    med_vp = data['med_vp']
    ed = data['ed']

    n_encounters = len(enc_keys)

    # Antibiotics
    print_subheader("Antibiotic Administration")

    if not med_ab.empty:
        ab_enc_col = get_column(med_ab, 'EncounterKey')
        n_enc_with_ab = med_ab[ab_enc_col].nunique() if ab_enc_col else 0
        print(f"  Encounters with antibiotics: {format_percentage(n_enc_with_ab, n_encounters)}")
        print(f"  Total antibiotic administrations: {format_number(len(med_ab))}")

        ab_med_col = get_column(med_ab, 'MedicationName')
        if ab_med_col:
            print(f"\n{Colors.BOLD}Most Common Antibiotics (Top 15):{Colors.END}")
            ab_counts = med_ab[ab_med_col].value_counts()
            for med, count in ab_counts.head(15).items():
                # Count encounters, not administrations
                n_enc = med_ab[med_ab[ab_med_col] == med][ab_enc_col].nunique() if ab_enc_col else 0
                print(f"  {med}: {format_number(count)} doses, {n_enc} encounters")

        # Timing analysis
        ab_admin_col = get_column(med_ab, 'AdministrationInstant')
        ed_enc_col = get_column(ed, 'EncounterKey')
        ed_arrival_col = get_column(ed, 'ArrivalInstant')
        if ab_admin_col and ab_enc_col and ed_enc_col and ed_arrival_col and not ed.empty:
            print(f"\n{Colors.BOLD}Timing Analysis:{Colors.END}")
            # Merge with ED arrival time
            med_ab_merged = med_ab.merge(
                ed[[ed_enc_col, ed_arrival_col]],
                left_on=ab_enc_col,
                right_on=ed_enc_col,
                how='left'
            )
            if ed_arrival_col in med_ab_merged.columns:
                arrival = pd.to_datetime(med_ab_merged[ed_arrival_col])
                admin = pd.to_datetime(med_ab_merged[ab_admin_col])
                time_to_ab = (admin - arrival).dt.total_seconds() / 60  # minutes

                print(f"  Time to first antibiotic (minutes):")
                print(f"    Median [IQR]: {time_to_ab.median():.0f} [{time_to_ab.quantile(0.25):.0f} - {time_to_ab.quantile(0.75):.0f}]")
                print(f"    Mean ± SD: {time_to_ab.mean():.0f} ± {time_to_ab.std():.0f}")
    else:
        print(f"  {Colors.YELLOW}No antibiotic data available{Colors.END}")

    # Vasopressors
    print_subheader("Vasopressor Administration")

    if not med_vp.empty:
        vp_enc_col = get_column(med_vp, 'EncounterKey')
        n_enc_with_vp = med_vp[vp_enc_col].nunique() if vp_enc_col else 0
        print(f"  Encounters with vasopressors: {format_percentage(n_enc_with_vp, n_encounters)}")
        print(f"  Total vasopressor administrations: {format_number(len(med_vp))}")

        vp_med_col = get_column(med_vp, 'MedicationName')
        if vp_med_col:
            print(f"\n{Colors.BOLD}Vasopressor Types:{Colors.END}")
            vp_counts = med_vp[vp_med_col].value_counts()
            for med, count in vp_counts.items():
                n_enc = med_vp[med_vp[vp_med_col] == med][vp_enc_col].nunique() if vp_enc_col else 0
                print(f"  {med}: {format_number(count)} doses, {n_enc} encounters")

        # Timing analysis
        vp_admin_col = get_column(med_vp, 'AdministrationInstant')
        ed_enc_col = get_column(ed, 'EncounterKey')
        ed_arrival_col = get_column(ed, 'ArrivalInstant')
        if vp_admin_col and vp_enc_col and ed_enc_col and ed_arrival_col and not ed.empty:
            print(f"\n{Colors.BOLD}Timing Analysis:{Colors.END}")
            med_vp_merged = med_vp.merge(
                ed[[ed_enc_col, ed_arrival_col]],
                left_on=vp_enc_col,
                right_on=ed_enc_col,
                how='left'
            )
            if ed_arrival_col in med_vp_merged.columns:
                arrival = pd.to_datetime(med_vp_merged[ed_arrival_col])
                admin = pd.to_datetime(med_vp_merged[vp_admin_col])
                time_to_vp = (admin - arrival).dt.total_seconds() / 60  # minutes

                print(f"  Time to first vasopressor (minutes):")
                print(f"    Median [IQR]: {time_to_vp.median():.0f} [{time_to_vp.quantile(0.25):.0f} - {time_to_vp.quantile(0.75):.0f}]")
                print(f"    Mean ± SD: {time_to_vp.mean():.0f} ± {time_to_vp.std():.0f}")
    else:
        print(f"  {Colors.YELLOW}No vasopressor data available{Colors.END}")

def summarize_imaging(data):
    """Summarize imaging procedures"""
    print_header("IMAGING PROCEDURES")

    enc_keys = data['enc_keys']
    img_lung = data['img_lung']
    img_lung_proc = data['img_lung_proc']

    n_encounters = len(enc_keys)

    if not img_lung.empty:
        img_enc_col = get_column(img_lung, 'EncounterKey')
        n_enc_with_img = img_lung[img_enc_col].nunique() if img_enc_col else 0
        print(f"Encounters with lung imaging: {format_percentage(n_enc_with_img, n_encounters)}")
        print(f"Total lung imaging procedures: {format_number(len(img_lung))}")

        proc_name_col = get_column(img_lung_proc, 'FirstProcedureName')
        count_col = get_column(img_lung_proc, 'Count')
        if not img_lung_proc.empty and proc_name_col:
            print(f"\n{Colors.BOLD}Lung Imaging Procedure Types:{Colors.END}")
            for idx, row in img_lung_proc.iterrows():
                proc_name = row.get(proc_name_col, 'Unknown')
                count = row.get(count_col, 0) if count_col else 0
                print(f"  {proc_name}: {format_number(count)}")
    else:
        print(f"{Colors.YELLOW}No lung imaging data available{Colors.END}")

def summarize_physiological_data(data):
    """Summarize availability of labs and vital signs"""
    print_header("PHYSIOLOGICAL DATA AVAILABILITY")

    enc_keys = data['enc_keys']
    labs = data['labs']
    flowsheet_assess = data['flowsheet_assess']
    flowsheet_resp = data['flowsheet_resp']

    n_encounters = len(enc_keys)

    # Lab data
    print_subheader("Laboratory Tests")

    if not labs.empty:
        lab_enc_col = get_column(labs, 'EncounterKey')
        n_enc_with_labs = labs[lab_enc_col].nunique() if lab_enc_col else 0
        print(f"Encounters with lab data: {format_percentage(n_enc_with_labs, n_encounters)}")
        print(f"Total lab results: {format_number(len(labs))}")

        lab_comp_col = get_column(labs, 'ComponentCommonName')
        if lab_comp_col:
            print(f"\n{Colors.BOLD}Most Common Lab Tests (Top 20):{Colors.END}")
            lab_counts = labs[lab_comp_col].value_counts()
            for lab, count in lab_counts.head(20).items():
                n_enc = labs[labs[lab_comp_col] == lab][lab_enc_col].nunique() if lab_enc_col else 0
                print(f"  {lab}: {format_number(count)} results, {n_enc} encounters")

        # Key sepsis-related labs
        print_subheader("Key Sepsis-Related Labs")
        sepsis_labs = {
            'lactate': ['lactate', 'lactic'],
            'WBC': ['wbc', 'white blood cell', 'leukocyte'],
            'creatinine': ['creatinine', 'creat'],
            'procalcitonin': ['procalcitonin', 'pct'],
            'CRP': ['c-reactive protein', 'crp'],
            'blood culture': ['blood culture']
        }

        if lab_comp_col:
            for lab_name, keywords in sepsis_labs.items():
                for keyword in keywords:
                    mask = labs[lab_comp_col].str.contains(keyword, case=False, na=False)
                    if mask.sum() > 0:
                        n_results = mask.sum()
                        n_enc = labs[mask][lab_enc_col].nunique() if lab_enc_col else 0
                        print(f"  {lab_name}: {format_number(n_results)} results, {n_enc} encounters")
                        break
    else:
        print(f"{Colors.YELLOW}No lab data available{Colors.END}")

    # Flowsheet data (vital signs)
    print_subheader("Vital Signs and Assessments")

    if not flowsheet_assess.empty:
        assess_enc_col = get_column(flowsheet_assess, 'EncounterKey')
        n_enc_with_assess = flowsheet_assess[assess_enc_col].nunique() if assess_enc_col else 0
        print(f"Encounters with assessments: {format_percentage(n_enc_with_assess, n_encounters)}")
        print(f"Total assessment records: {format_number(len(flowsheet_assess))}")
    else:
        print(f"{Colors.YELLOW}No assessment flowsheet data available{Colors.END}")

    if not flowsheet_resp.empty:
        resp_enc_col = get_column(flowsheet_resp, 'EncounterKey')
        n_enc_with_resp = flowsheet_resp[resp_enc_col].nunique() if resp_enc_col else 0
        print(f"Encounters with respiratory data: {format_percentage(n_enc_with_resp, n_encounters)}")
        print(f"Total respiratory records: {format_number(len(flowsheet_resp))}")
    else:
        print(f"{Colors.YELLOW}No respiratory flowsheet data available{Colors.END}")

def summarize_data_quality(data):
    """Assess data quality and completeness"""
    print_header("DATA QUALITY METRICS")

    print_subheader("Dataset Completeness")

    critical_tables = {
        'enc_keys': 'Encounter Keys',
        'pt': 'Patient Demographics',
        'ed': 'ED Visit Data',
        'hosp_adm': 'Hospital Admissions',
        'dx_enc_icd': 'Diagnoses',
        'med_ab': 'Antibiotics',
        'labs': 'Laboratory Tests',
        'flowsheet_assess': 'Vital Signs'
    }

    for table_key, table_name in critical_tables.items():
        df = data.get(table_key, pd.DataFrame())
        if not df.empty:
            print(f"{Colors.GREEN}✓{Colors.END} {table_name}: {format_number(len(df))} records")
        else:
            print(f"{Colors.RED}✗{Colors.END} {table_name}: No data")

    # Missing data analysis for key fields
    print_subheader("Key Field Completeness")

    # Patient demographics
    pt = data['pt']
    if not pt.empty:
        key_fields = ['Sex', 'Race', 'EthnicGroup', 'BirthDate']
        print(f"\n{Colors.BOLD}Patient Demographics:{Colors.END}")
        for field in key_fields:
            col = get_column(pt, field)
            if col:
                n_missing = pt[col].isna().sum()
                pct_complete = ((len(pt) - n_missing) / len(pt)) * 100
                print(f"  {field}: {pct_complete:.1f}% complete ({len(pt) - n_missing:,}/{len(pt):,})")

    # Encounter data
    ed = data['ed']
    if not ed.empty:
        key_fields = ['ArrivalInstant', 'DischargeDispositionName', 'DepartureInstant']
        print(f"\n{Colors.BOLD}ED Encounter Data:{Colors.END}")
        for field in key_fields:
            col = get_column(ed, field)
            if col:
                n_missing = ed[col].isna().sum()
                pct_complete = ((len(ed) - n_missing) / len(ed)) * 100
                print(f"  {field}: {pct_complete:.1f}% complete ({len(ed) - n_missing:,}/{len(ed):,})")

def summarize_insurance(data):
    """Summarize insurance/payor information"""
    print_header("INSURANCE AND PAYOR DATA")

    insurance = data['insurance']

    if not insurance.empty:
        print(f"Total insurance records: {format_number(len(insurance))}")

        payor_class_col = get_column(insurance, 'PayorFinancialClass')
        if payor_class_col:
            print(f"\n{Colors.BOLD}Payor Financial Class:{Colors.END}")
            payor_counts = insurance[payor_class_col].value_counts()
            for payor, count in payor_counts.items():
                print(f"  {payor}: {format_percentage(count, len(insurance))}")

        payor_name_col = get_column(insurance, 'PayorName')
        if payor_name_col:
            print(f"\n{Colors.BOLD}Top 10 Payors:{Colors.END}")
            payor_counts = insurance[payor_name_col].value_counts()
            for payor, count in payor_counts.head(10).items():
                print(f"  {payor}: {format_number(count)}")
    else:
        print(f"{Colors.YELLOW}No insurance data available{Colors.END}")

def save_summary_json(data, output_folder):
    """Save summary statistics to JSON file"""
    print_header("SAVING SUMMARY STATISTICS")

    # Get column names safely
    enc_pt_col = get_column(data['enc_keys'], 'PatientDurableKey')
    hosp_enc_col = get_column(data['hosp_adm'], 'EncounterKey')
    icu_enc_col = get_column(data['icu_reg'], 'EncounterKey')
    death_pt_col = get_column(data['death'], 'PatientDurableKey')
    med_ab_enc_col = get_column(data['med_ab'], 'EncounterKey')
    med_vp_enc_col = get_column(data['med_vp'], 'EncounterKey')

    summary = {
        'generated_at': datetime.now().isoformat(),
        'cohort': {
            'n_patients': int(data['enc_keys'][enc_pt_col].nunique()) if (not data['enc_keys'].empty and enc_pt_col) else 0,
            'n_encounters': len(data['enc_keys']),
        },
        'outcomes': {
            'n_hospital_admissions': int(data['hosp_adm'][hosp_enc_col].nunique()) if (not data['hosp_adm'].empty and hosp_enc_col) else 0,
            'n_icu_admissions': int(data['icu_reg'][icu_enc_col].nunique()) if (not data['icu_reg'].empty and icu_enc_col) else 0,
            'n_deaths': int(data['death'][death_pt_col].nunique()) if (not data['death'].empty and death_pt_col) else 0,
        },
        'treatments': {
            'encounters_with_antibiotics': int(data['med_ab'][med_ab_enc_col].nunique()) if (not data['med_ab'].empty and med_ab_enc_col) else 0,
            'encounters_with_vasopressors': int(data['med_vp'][med_vp_enc_col].nunique()) if (not data['med_vp'].empty and med_vp_enc_col) else 0,
        },
        'data_availability': {
            'lab_results': len(data['labs']),
            'flowsheet_assess_records': len(data['flowsheet_assess']),
            'flowsheet_resp_records': len(data['flowsheet_resp']),
            'imaging_procedures': len(data['img_lung']),
        }
    }

    output_file = os.path.join(output_folder, f"summary_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(output_file, 'w') as f:
        json.dump(summary, f, indent=2)

    print(f"{Colors.GREEN}✓{Colors.END} Summary statistics saved to: {output_file}")

def main():
    """Main execution function"""
    print_header("COMMUNITY-ONSET LUNG SEPSIS (COLS) DATASET SUMMARY")
    print(f"{Colors.BOLD}Study: Sepsis R01 - Clinical Decision Support System Development{Colors.END}")
    print(f"Focus: Early treatment optimization (antibiotics, fluids, vasopressors)")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Determine data folder
    today = datetime.now().strftime("%Y-%m-%d")
    default_folder = f"Data_All_{today}"

    if len(sys.argv) > 1:
        data_folder = sys.argv[1]
    else:
        data_folder = default_folder

    if not os.path.exists(data_folder):
        print(f"\n{Colors.RED}✗ Error: Data folder not found: {data_folder}{Colors.END}")
        print(f"\nUsage: python summarize_data.py [data_folder]")
        print(f"Example: python summarize_data.py Data_All_2025-10-14")
        sys.exit(1)

    print(f"\n{Colors.BOLD}Data Folder:{Colors.END} {data_folder}")

    try:
        # Load all data
        data = load_data(data_folder)

        # Generate all summary sections
        summarize_cohort_overview(data)
        summarize_clinical_outcomes(data)
        summarize_sepsis_indicators(data)
        summarize_treatment_patterns(data)
        summarize_imaging(data)
        summarize_physiological_data(data)
        summarize_data_quality(data)
        summarize_insurance(data)

        # Save summary JSON
        save_summary_json(data, data_folder)

        print_header("SUMMARY COMPLETE")
        print(f"\n{Colors.GREEN}{Colors.BOLD}✓ Dataset summary generated successfully!{Colors.END}\n")

    except Exception as e:
        print(f"\n{Colors.RED}✗ Error during summary generation: {e}{Colors.END}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
