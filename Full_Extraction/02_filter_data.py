#!/usr/bin/env python3
"""
Sepsis R01 Cohort Filtering Script
Filters extracted data according to sepsis inclusion criteria

Usage:
    python 02_filter_data.py -i Data_All_UCSF_2024-01-15
    python 02_filter_data.py -i Data_All_SFDPH_2024-01-15
    python 02_filter_data.py -i Data_All_UCSF_2024-01-15 -i Data_All_SFDPH_2024-01-15  # Both

Inclusion Criteria:
1. Adults ≥18 years old (already applied in extraction)
2. Presenting to the emergency department (already applied in extraction)
3. Time zero defined by:
   - CBC obtained within first 2 hours of ED presentation
   - PLUS temperature <36°C or >38.5°C and/or WBC >12,000 or <4,000/µL

Additional Flags (not exclusion criteria):
- has_chest_imaging: Chest x-ray or CT chest ordered within first 4 hours
- has_hypotension: At least 1 hypotensive episode (SBP < 90) during first 4 hours
"""

import os
import sys
import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List

# Configuration
WBC_HIGH_THRESHOLD = 12.0  # x10^3/µL (12,000/µL)
WBC_LOW_THRESHOLD = 4.0    # x10^3/µL (4,000/µL)
TEMP_HIGH_THRESHOLD = 38.5  # Celsius
TEMP_LOW_THRESHOLD = 36.0   # Celsius
HYPOTENSION_SBP_THRESHOLD = 90  # mmHg
CBC_WINDOW_HOURS = 2
IMAGING_WINDOW_HOURS = 4
HYPOTENSION_WINDOW_HOURS = 4


def load_parquet(folder_path: str, filename: str) -> pd.DataFrame:
    """Load a parquet file from the data folder"""
    filepath = os.path.join(folder_path, filename)
    if not os.path.exists(filepath):
        print(f"  Warning: {filename} not found")
        return pd.DataFrame()
    df = pd.read_parquet(filepath)
    print(f"  Loaded {filename}: {len(df):,} rows")
    return df


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names to lowercase for consistent access"""
    df.columns = df.columns.str.lower()
    return df


def parse_datetime(series: pd.Series) -> pd.Series:
    """Parse datetime series, handling various formats"""
    return pd.to_datetime(series, errors='coerce')


def find_ed_arrival_time(ed_df: pd.DataFrame) -> pd.DataFrame:
    """Extract ED arrival times for each encounter"""
    ed_df = standardize_columns(ed_df.copy())

    # Find arrival time column - prefer instant columns, otherwise combine date + time
    arrival_col = None
    arrival_time_of_day_col = None

    # First try instant columns (already have full datetime)
    for col in ['arrivalinstant']:
        if col in ed_df.columns:
            arrival_col = col
            break

    # If no instant column, look for date + time of day columns
    if arrival_col is None:
        for date_col, tod_col in [('arrivaldatekeyvalue', 'arrivaltimeofdaykeyvalue'),
                                   ('arrivaldate', 'arrivaltime')]:
            if date_col in ed_df.columns:
                arrival_col = date_col
                if tod_col in ed_df.columns:
                    arrival_time_of_day_col = tod_col
                break

    if arrival_col is None:
        raise ValueError(f"Could not find arrival time column. Available: {ed_df.columns.tolist()}")

    result = ed_df[['encounterkey']].copy()

    # Combine date and time if separate columns exist
    if arrival_time_of_day_col is not None:
        # Combine date + time of day into full datetime
        result['ed_arrival_time'] = pd.to_datetime(
            ed_df[arrival_col].astype(str) + ' ' + ed_df[arrival_time_of_day_col].astype(str),
            errors='coerce'
        )
    else:
        result['ed_arrival_time'] = parse_datetime(ed_df[arrival_col])

    return result.drop_duplicates()


def filter_cbc_within_window(labs_df: pd.DataFrame,
                              ed_times_df: pd.DataFrame,
                              window_hours: int = CBC_WINDOW_HOURS) -> pd.DataFrame:
    """
    Find encounters with CBC obtained within specified hours of ED arrival

    Returns DataFrame with encounterkey, first_cbc_time, and first_wbc_value
    """
    labs_df = standardize_columns(labs_df.copy())

    # Find WBC results - different keys for different data sources
    # UCSF: LabComponentKey 994 (WBCCOUNT)
    # SFDPH: LabComponentKey 2810 (WBC Count)
    wbc_keys = [994, 2810]
    wbc_labs = labs_df[labs_df['labcomponentkey'].isin(wbc_keys)].copy()

    if len(wbc_labs) == 0:
        print(f"  Warning: No WBC labs found with keys {wbc_keys}")
        return pd.DataFrame(columns=['encounterkey', 'first_cbc_time', 'first_wbc_value'])

    # Find result time column - prefer instant columns, otherwise combine date + time
    time_col = None
    time_of_day_col = None
    for col in ['resultinstant', 'collectioninstant']:
        if col in wbc_labs.columns:
            time_col = col
            break

    # If no instant column, look for date + time of day columns
    if time_col is None:
        for date_col, tod_col in [('resultdatekeyvalue', 'resulttimeofdaykeyvalue'),
                                   ('collectiondatekeyvalue', 'collectiontimeofdaykeyvalue')]:
            if date_col in wbc_labs.columns:
                time_col = date_col
                if tod_col in wbc_labs.columns:
                    time_of_day_col = tod_col
                break

    if time_col is None:
        raise ValueError(f"Could not find lab result time column. Available: {wbc_labs.columns.tolist()}")

    # Find value column
    value_col = None
    for col in ['value', 'numericvalue', 'resultvalue']:
        if col in wbc_labs.columns:
            value_col = col
            break

    if value_col is None:
        raise ValueError(f"Could not find lab value column. Available: {wbc_labs.columns.tolist()}")

    # Combine date and time if separate columns exist
    if time_of_day_col is not None:
        # Combine date + time of day into full datetime
        wbc_labs['lab_time'] = pd.to_datetime(
            wbc_labs[time_col].astype(str) + ' ' + wbc_labs[time_of_day_col].astype(str),
            errors='coerce'
        )
    else:
        wbc_labs['lab_time'] = parse_datetime(wbc_labs[time_col])
    wbc_labs['wbc_value'] = pd.to_numeric(wbc_labs[value_col], errors='coerce')

    # Merge with ED arrival times
    merged = wbc_labs.merge(ed_times_df, on='encounterkey', how='inner')

    # Calculate time from ED arrival
    merged['hours_from_arrival'] = (
        merged['lab_time'] - merged['ed_arrival_time']
    ).dt.total_seconds() / 3600

    # Filter to labs within window (and not before arrival)
    within_window = merged[
        (merged['hours_from_arrival'] >= 0) &
        (merged['hours_from_arrival'] <= window_hours)
    ].copy()

    # Get first CBC per encounter
    within_window = within_window.sort_values(['encounterkey', 'lab_time'])
    first_cbc = within_window.groupby('encounterkey').first().reset_index()

    result = first_cbc[['encounterkey', 'lab_time', 'wbc_value']].copy()
    result.columns = ['encounterkey', 'first_cbc_time', 'first_wbc_value']

    return result


def filter_abnormal_temp_or_wbc(flowsheet_df: pd.DataFrame,
                                 cbc_df: pd.DataFrame,
                                 ed_times_df: pd.DataFrame,
                                 window_hours: int = CBC_WINDOW_HOURS) -> pd.DataFrame:
    """
    Find encounters with abnormal temperature OR abnormal WBC within the time window

    Criteria:
    - Temperature <36°C or >38.5°C
    - WBC >12,000 or <4,000/µL

    Returns DataFrame with encounterkey and criteria flags
    """
    flowsheet_df = standardize_columns(flowsheet_df.copy())

    # Temperature FlowsheetRowKey - different keys for different data sources
    # UCSF: 34432, SFDPH: 29366
    temp_keys = [34432, 29366]
    temp_data = flowsheet_df[flowsheet_df['flowsheetrowkey'].isin(temp_keys)].copy()

    # Find time column - prefer instant columns, otherwise combine date + time
    time_col = None
    time_of_day_col = None
    for col in ['takeninstant', 'recordedinstant']:
        if col in temp_data.columns:
            time_col = col
            break

    # If no instant column, look for date + time of day columns
    if time_col is None:
        for date_col, tod_col in [('takendatekeyvalue', 'takentimeofdaykeyvalue'),
                                   ('recordeddatekeyvalue', 'recordedtimeofdaykeyvalue')]:
            if date_col in temp_data.columns:
                time_col = date_col
                if tod_col in temp_data.columns:
                    time_of_day_col = tod_col
                break

    if time_col is None and len(temp_data) > 0:
        raise ValueError(f"Could not find flowsheet time column. Available: {temp_data.columns.tolist()}")

    # Find value column
    value_col = None
    for col in ['value', 'numericvalue', 'displayvalue']:
        if col in temp_data.columns:
            value_col = col
            break

    results = []

    # Process temperature data
    if len(temp_data) > 0 and time_col and value_col:
        # Combine date and time if separate columns exist
        if time_of_day_col is not None:
            temp_data['vital_time'] = pd.to_datetime(
                temp_data[time_col].astype(str) + ' ' + temp_data[time_of_day_col].astype(str),
                errors='coerce'
            )
        else:
            temp_data['vital_time'] = parse_datetime(temp_data[time_col])
        temp_data['temp_value'] = pd.to_numeric(temp_data[value_col], errors='coerce')

        # Merge with ED arrival times
        temp_merged = temp_data.merge(ed_times_df, on='encounterkey', how='inner')

        # Calculate time from ED arrival
        temp_merged['hours_from_arrival'] = (
            temp_merged['vital_time'] - temp_merged['ed_arrival_time']
        ).dt.total_seconds() / 3600

        # Filter to vitals within window
        temp_window = temp_merged[
            (temp_merged['hours_from_arrival'] >= 0) &
            (temp_merged['hours_from_arrival'] <= window_hours)
        ]

        # Find abnormal temperatures
        abnormal_temp = temp_window[
            (temp_window['temp_value'] < TEMP_LOW_THRESHOLD) |
            (temp_window['temp_value'] > TEMP_HIGH_THRESHOLD)
        ]

        temp_encounters = abnormal_temp[['encounterkey']].drop_duplicates()
        temp_encounters['has_abnormal_temp'] = True
        results.append(temp_encounters)

    # Process WBC data (from cbc_df which already has first WBC)
    if len(cbc_df) > 0:
        wbc_abnormal = cbc_df[
            (cbc_df['first_wbc_value'] > WBC_HIGH_THRESHOLD) |
            (cbc_df['first_wbc_value'] < WBC_LOW_THRESHOLD)
        ][['encounterkey']].copy()
        wbc_abnormal['has_abnormal_wbc'] = True
        results.append(wbc_abnormal)

    if not results:
        return pd.DataFrame(columns=['encounterkey', 'has_abnormal_temp', 'has_abnormal_wbc', 'meets_sirs_criteria'])

    # Combine results
    combined = pd.concat(results, ignore_index=True)
    combined = combined.groupby('encounterkey').first().reset_index()
    combined['has_abnormal_temp'] = combined.get('has_abnormal_temp', pd.Series(dtype=bool)).fillna(False).astype(bool)
    combined['has_abnormal_wbc'] = combined.get('has_abnormal_wbc', pd.Series(dtype=bool)).fillna(False).astype(bool)

    # Filter to those with either abnormal temp OR abnormal WBC
    combined['meets_sirs_criteria'] = combined['has_abnormal_temp'] | combined['has_abnormal_wbc']

    return combined


def filter_chest_imaging(img_df: pd.DataFrame,
                          ed_times_df: pd.DataFrame,
                          window_hours: int = IMAGING_WINDOW_HOURS) -> pd.DataFrame:
    """
    Find encounters with chest x-ray or CT chest within specified hours of ED arrival

    Returns DataFrame with encounterkey and imaging flags
    """
    img_df = standardize_columns(img_df.copy())

    if len(img_df) == 0:
        return pd.DataFrame(columns=['encounterkey', 'has_chest_imaging', 'first_imaging_time'])

    # Find time column - prefer instant columns, otherwise combine date + time
    time_col = None
    time_of_day_col = None
    for col in ['orderinginstant', 'orderinstant', 'examstartinstant', 'startinstant']:
        if col in img_df.columns:
            time_col = col
            break

    # If no instant column, look for date + time of day columns
    if time_col is None:
        for date_col, tod_col in [('orderingdatekeyvalue', 'orderingtimeofdaykeyvalue'),
                                   ('orderdatekeyvalue', 'ordertimeofdaykeyvalue'),
                                   ('examstartdatekeyvalue', 'examstarttimeofdaykeyvalue'),
                                   ('startdatekeyvalue', 'starttimeofdaykeyvalue')]:
            if date_col in img_df.columns:
                time_col = date_col
                if tod_col in img_df.columns:
                    time_of_day_col = tod_col
                break

    if time_col is None:
        raise ValueError(f"Could not find imaging time column. Available: {img_df.columns.tolist()}")

    # Combine date and time if separate columns exist
    if time_of_day_col is not None:
        img_df['imaging_time'] = pd.to_datetime(
            img_df[time_col].astype(str) + ' ' + img_df[time_of_day_col].astype(str),
            errors='coerce'
        )
    else:
        img_df['imaging_time'] = parse_datetime(img_df[time_col])

    # Merge with ED arrival times
    merged = img_df.merge(ed_times_df, on='encounterkey', how='inner')

    # Calculate time from ED arrival
    merged['hours_from_arrival'] = (
        merged['imaging_time'] - merged['ed_arrival_time']
    ).dt.total_seconds() / 3600

    # Filter to imaging within window
    within_window = merged[
        (merged['hours_from_arrival'] >= 0) &
        (merged['hours_from_arrival'] <= window_hours)
    ].copy()

    # Get first imaging per encounter
    within_window = within_window.sort_values(['encounterkey', 'imaging_time'])
    first_imaging = within_window.groupby('encounterkey').first().reset_index()

    result = first_imaging[['encounterkey', 'imaging_time']].copy()
    result.columns = ['encounterkey', 'first_imaging_time']
    result['has_chest_imaging'] = True

    return result


def filter_hypotension(flowsheet_df: pd.DataFrame,
                        ed_times_df: pd.DataFrame,
                        window_hours: int = HYPOTENSION_WINDOW_HOURS) -> pd.DataFrame:
    """
    Find encounters with at least 1 hypotensive episode (SBP < 90) within specified hours

    Returns DataFrame with encounterkey and hypotension flag
    """
    flowsheet_df = standardize_columns(flowsheet_df.copy())

    # Blood Pressure FlowsheetRowKey - different keys for different data sources
    # UCSF: 32710, SFDPH: 27705
    bp_keys = [32710, 27705]
    bp_data = flowsheet_df[flowsheet_df['flowsheetrowkey'].isin(bp_keys)].copy()

    if len(bp_data) == 0:
        print("  Warning: No blood pressure data found")
        return pd.DataFrame(columns=['encounterkey', 'has_hypotension', 'first_hypotension_time', 'min_sbp'])

    # Find time column - prefer instant columns, otherwise combine date + time
    time_col = None
    time_of_day_col = None
    for col in ['takeninstant', 'recordedinstant']:
        if col in bp_data.columns:
            time_col = col
            break

    # If no instant column, look for date + time of day columns
    if time_col is None:
        for date_col, tod_col in [('takendatekeyvalue', 'takentimeofdaykeyvalue'),
                                   ('recordeddatekeyvalue', 'recordedtimeofdaykeyvalue')]:
            if date_col in bp_data.columns:
                time_col = date_col
                if tod_col in bp_data.columns:
                    time_of_day_col = tod_col
                break

    if time_col is None:
        raise ValueError(f"Could not find flowsheet time column. Available: {bp_data.columns.tolist()}")

    # Find value column - BP is usually stored as "120/80" format
    value_col = None
    for col in ['value', 'displayvalue']:
        if col in bp_data.columns:
            value_col = col
            break

    if value_col is None:
        raise ValueError(f"Could not find BP value column. Available: {bp_data.columns.tolist()}")

    # Combine date and time if separate columns exist
    if time_of_day_col is not None:
        bp_data['vital_time'] = pd.to_datetime(
            bp_data[time_col].astype(str) + ' ' + bp_data[time_of_day_col].astype(str),
            errors='coerce'
        )
    else:
        bp_data['vital_time'] = parse_datetime(bp_data[time_col])

    # Extract systolic BP (first number before /)
    def extract_sbp(bp_str):
        try:
            if pd.isna(bp_str):
                return np.nan
            bp_str = str(bp_str)
            if '/' in bp_str:
                return float(bp_str.split('/')[0])
            return float(bp_str)
        except (ValueError, IndexError):
            return np.nan

    bp_data['sbp'] = bp_data[value_col].apply(extract_sbp)

    # Merge with ED arrival times
    merged = bp_data.merge(ed_times_df, on='encounterkey', how='inner')

    # Calculate time from ED arrival
    merged['hours_from_arrival'] = (
        merged['vital_time'] - merged['ed_arrival_time']
    ).dt.total_seconds() / 3600

    # Filter to vitals within window
    within_window = merged[
        (merged['hours_from_arrival'] >= 0) &
        (merged['hours_from_arrival'] <= window_hours)
    ].copy()

    # Find hypotensive episodes
    hypotensive = within_window[within_window['sbp'] < HYPOTENSION_SBP_THRESHOLD].copy()

    # Get first hypotensive episode and min SBP per encounter
    if len(hypotensive) > 0:
        hypotensive = hypotensive.sort_values(['encounterkey', 'vital_time'])
        first_hypo = hypotensive.groupby('encounterkey').agg({
            'vital_time': 'first',
            'sbp': 'min'
        }).reset_index()
        first_hypo.columns = ['encounterkey', 'first_hypotension_time', 'min_sbp']
        first_hypo['has_hypotension'] = True
        return first_hypo

    return pd.DataFrame(columns=['encounterkey', 'has_hypotension', 'first_hypotension_time', 'min_sbp'])


def process_single_folder(input_folder: str, output_folder: str) -> pd.DataFrame:
    """Process a single data folder and return the COLS cohort

    Args:
        input_folder: Path to folder containing extracted parquet files
        output_folder: Path to output folder

    Returns:
        DataFrame with COLS cohort for this folder
    """
    print("\n" + "=" * 70)
    print(f"Processing: {input_folder}")
    print("=" * 70)

    os.makedirs(output_folder, exist_ok=True)

    # Load required data
    print("\n=== Loading Data ===")
    ed_df = load_parquet(input_folder, 'ed.parquet')
    labs_df = load_parquet(input_folder, 'labs.parquet')
    flowsheet_assess_df = load_parquet(input_folder, 'flowsheet_assess.parquet')
    img_lung_df = load_parquet(input_folder, 'img_lung.parquet')
    enc_keys_df = load_parquet(input_folder, 'enc_keys.parquet')

    if len(ed_df) == 0:
        print("Error: No ED data found")
        sys.exit(1)

    # Get ED arrival times
    print("\n=== Processing ED Arrival Times ===")
    ed_times = find_ed_arrival_time(ed_df)
    print(f"  Found {len(ed_times):,} encounters with ED arrival times")

    # Step 1: Filter CBC within 2 hours
    print(f"\n=== Step 1: CBC within {CBC_WINDOW_HOURS} hours of ED arrival ===")
    cbc_df = filter_cbc_within_window(labs_df, ed_times, CBC_WINDOW_HOURS)
    print(f"  Encounters with CBC within {CBC_WINDOW_HOURS}h: {len(cbc_df):,}")

    # Step 2: Filter abnormal temp or WBC
    print(f"\n=== Step 2: Abnormal Temperature or WBC ===")
    print(f"  Temperature thresholds: <{TEMP_LOW_THRESHOLD}°C or >{TEMP_HIGH_THRESHOLD}°C")
    print(f"  WBC thresholds: <{WBC_LOW_THRESHOLD} or >{WBC_HIGH_THRESHOLD} x10^3/µL")
    abnormal_df = filter_abnormal_temp_or_wbc(flowsheet_assess_df, cbc_df, ed_times, CBC_WINDOW_HOURS)
    meets_sirs = abnormal_df[abnormal_df['meets_sirs_criteria']]
    print(f"  Encounters with abnormal temp: {abnormal_df['has_abnormal_temp'].sum():,}")
    print(f"  Encounters with abnormal WBC: {abnormal_df['has_abnormal_wbc'].sum():,}")
    print(f"  Encounters meeting temp OR WBC criteria: {len(meets_sirs):,}")

    # Step 3: Filter chest imaging within 4 hours
    print(f"\n=== Step 3: Chest Imaging within {IMAGING_WINDOW_HOURS} hours ===")
    imaging_df = filter_chest_imaging(img_lung_df, ed_times, IMAGING_WINDOW_HOURS)
    print(f"  Encounters with chest imaging within {IMAGING_WINDOW_HOURS}h: {len(imaging_df):,}")

    # Step 4: Identify hypotensive subpopulation
    print(f"\n=== Step 4: Hypotension Subpopulation (SBP < {HYPOTENSION_SBP_THRESHOLD}) ===")
    hypotension_df = filter_hypotension(flowsheet_assess_df, ed_times, HYPOTENSION_WINDOW_HOURS)
    print(f"  Encounters with hypotension within {HYPOTENSION_WINDOW_HOURS}h: {len(hypotension_df):,}")

    # Combine criteria for filtered cohort
    print("\n=== Combining Inclusion Criteria ===")

    # Start with encounters that have CBC within 2h
    filtered_cohort = cbc_df[['encounterkey', 'first_cbc_time', 'first_wbc_value']].copy()

    # Merge with abnormal temp/WBC criteria
    filtered_cohort = filtered_cohort.merge(
        meets_sirs[['encounterkey', 'has_abnormal_temp', 'has_abnormal_wbc', 'meets_sirs_criteria']],
        on='encounterkey',
        how='inner'
    )
    print(f"  After CBC + abnormal temp/WBC: {len(filtered_cohort):,}")

    # Add chest imaging flag (not an exclusion criterion, just tracking)
    filtered_cohort = filtered_cohort.merge(
        imaging_df[['encounterkey', 'has_chest_imaging', 'first_imaging_time']],
        on='encounterkey',
        how='left'
    )
    filtered_cohort['has_chest_imaging'] = filtered_cohort['has_chest_imaging'].fillna(False)
    print(f"  With chest imaging: {filtered_cohort['has_chest_imaging'].sum():,}")

    # Add hypotension flag (not an exclusion criterion, just tracking)
    filtered_cohort = filtered_cohort.merge(
        hypotension_df[['encounterkey', 'has_hypotension', 'first_hypotension_time', 'min_sbp']],
        on='encounterkey',
        how='left'
    )
    filtered_cohort['has_hypotension'] = filtered_cohort['has_hypotension'].fillna(False)

    # Define time zero as first CBC time
    filtered_cohort['time_zero'] = filtered_cohort['first_cbc_time']

    # Add ED arrival time
    filtered_cohort = filtered_cohort.merge(ed_times, on='encounterkey', how='left')

    # Summary
    print("\n" + "=" * 70)
    print("FILTERED COHORT SUMMARY")
    print("=" * 70)
    print(f"\nTotal encounters in extraction: {len(enc_keys_df):,}")
    print(f"Filtered cohort (all criteria met): {len(filtered_cohort):,}")
    print(f"  - With chest imaging: {filtered_cohort['has_chest_imaging'].sum():,}")
    print(f"  - With hypotension: {filtered_cohort['has_hypotension'].sum():,}")

    if len(enc_keys_df) > 0:
        pct = 100 * len(filtered_cohort) / len(enc_keys_df)
        print(f"\nFiltered cohort is {pct:.1f}% of total ED encounters")

    # Save outputs (parquet and CSV)
    print("\n=== Saving Results ===")

    # Save filtered cohort
    output_path = os.path.join(output_folder, 'filtered_cohort.parquet')
    filtered_cohort.to_parquet(output_path, index=False)
    print(f"  Saved filtered cohort: {output_path}")
    filtered_cohort.to_csv(output_path.replace('.parquet', '.csv'), index=False)

    # Save hypotensive subpopulation
    filtered_hypotensive = filtered_cohort[filtered_cohort['has_hypotension']]
    output_path = os.path.join(output_folder, 'filtered_cohort_hypotensive.parquet')
    filtered_hypotensive.to_parquet(output_path, index=False)
    print(f"  Saved hypotensive subpopulation: {output_path}")
    filtered_hypotensive.to_csv(output_path.replace('.parquet', '.csv'), index=False)

    # Save intermediate files for debugging/analysis
    output_path = os.path.join(output_folder, 'filtered_cbc_within_2h.parquet')
    cbc_df.to_parquet(output_path, index=False)
    print(f"  Saved CBC within 2h: {output_path}")
    cbc_df.to_csv(output_path.replace('.parquet', '.csv'), index=False)

    output_path = os.path.join(output_folder, 'filtered_abnormal_temp_wbc.parquet')
    abnormal_df.to_parquet(output_path, index=False)
    print(f"  Saved abnormal temp/WBC: {output_path}")
    abnormal_df.to_csv(output_path.replace('.parquet', '.csv'), index=False)

    output_path = os.path.join(output_folder, 'filtered_chest_imaging.parquet')
    imaging_df.to_parquet(output_path, index=False)
    print(f"  Saved chest imaging: {output_path}")
    imaging_df.to_csv(output_path.replace('.parquet', '.csv'), index=False)

    output_path = os.path.join(output_folder, 'filtered_hypotension.parquet')
    hypotension_df.to_parquet(output_path, index=False)
    print(f"  Saved hypotension: {output_path}")
    hypotension_df.to_csv(output_path.replace('.parquet', '.csv'), index=False)

    # Add source folder identifier
    folder_name = os.path.basename(input_folder)
    filtered_cohort['source_folder'] = folder_name

    print(f"\nFiltering completed for {folder_name}")

    return filtered_cohort


def main():
    parser = argparse.ArgumentParser(
        description='Filter sepsis cohort from extracted data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python 02_filter_data.py -i Data_All_UCSF_2024-01-15
    python 02_filter_data.py -i Data_All_SFDPH_2024-01-15
    python 02_filter_data.py -i Data_All_UCSF_2024-01-15 -i Data_All_SFDPH_2024-01-15
        """
    )
    parser.add_argument('--input-folder', '-i', required=True, action='append',
                        help='Path to folder containing extracted parquet files (can be specified multiple times)')
    parser.add_argument('--output-folder', '-o', default=None,
                        help='Path to output folder for combined results (default: Filtered_Combined_<date>)')
    args = parser.parse_args()

    print("=" * 70)
    print("SEPSIS R01 COHORT FILTERING")
    print("=" * 70)

    input_folders = args.input_folder
    print(f"\nInput folders: {len(input_folders)}")
    for folder in input_folders:
        print(f"  - {folder}")

    # Validate input folders
    for folder in input_folders:
        if not os.path.exists(folder):
            print(f"\nError: Input folder not found: {folder}")
            sys.exit(1)

    all_cohorts = []

    # Process each input folder
    for input_folder in input_folders:
        # Output goes to same folder as input by default
        output_folder = input_folder
        cohort = process_single_folder(input_folder, output_folder)
        all_cohorts.append(cohort)

    # If multiple folders, create combined output
    if len(input_folders) > 1:
        print("\n" + "=" * 70)
        print("CREATING COMBINED COHORT")
        print("=" * 70)

        combined_cohort = pd.concat(all_cohorts, ignore_index=True)

        # Create combined output folder
        today = datetime.now().strftime("%Y-%m-%d")
        combined_folder = args.output_folder or f"Filtered_Combined_{today}"
        os.makedirs(combined_folder, exist_ok=True)

        # Save combined cohort (parquet and CSV)
        output_path = os.path.join(combined_folder, 'filtered_cohort_combined.parquet')
        combined_cohort.to_parquet(output_path, index=False)
        print(f"  Saved combined filtered cohort: {output_path}")
        csv_path = os.path.join(combined_folder, 'filtered_cohort_combined.csv')
        combined_cohort.to_csv(csv_path, index=False)
        print(f"  Saved combined filtered cohort: {csv_path}")

        # Save combined hypotensive subpopulation (parquet and CSV)
        combined_hypo = combined_cohort[combined_cohort['has_hypotension']]
        output_path = os.path.join(combined_folder, 'filtered_cohort_hypotensive_combined.parquet')
        combined_hypo.to_parquet(output_path, index=False)
        print(f"  Saved combined hypotensive subpopulation: {output_path}")
        csv_path = os.path.join(combined_folder, 'filtered_cohort_hypotensive_combined.csv')
        combined_hypo.to_csv(csv_path, index=False)
        print(f"  Saved combined hypotensive subpopulation: {csv_path}")

        # Summary by source
        print("\n=== Combined Cohort Summary by Source ===")
        for source, group in combined_cohort.groupby('source_folder'):
            print(f"  {source}: {len(group):,} encounters ({group['has_hypotension'].sum():,} hypotensive)")

        print(f"\nTotal combined filtered cohort: {len(combined_cohort):,}")
        print(f"  - With chest imaging: {combined_cohort['has_chest_imaging'].sum():,}")
        print(f"  - With hypotension: {combined_cohort['has_hypotension'].sum():,}")

    print("\n" + "=" * 70)
    print("ALL FILTERING COMPLETED SUCCESSFULLY")
    print("=" * 70)


if __name__ == "__main__":
    main()
