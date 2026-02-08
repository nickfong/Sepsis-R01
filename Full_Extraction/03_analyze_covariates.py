#!/usr/bin/env python3
"""
Sepsis R01 Time-Varying Covariate Analysis Script
Analyzes the most commonly recorded time-varying covariates from extracted data.

Usage:
    python 03_analyze_covariates.py
    python 03_analyze_covariates.py -i Data_All_UCSF_2025-12-22 -i Data_All_SFDPH_2025-12-22
    python 03_analyze_covariates.py --top 100  # Show top 100 instead of 50

Output:
    - Console table of top N covariates
    - top_50_covariates.csv with detailed breakdown
"""

import os
import sys
import argparse
import pandas as pd
from pathlib import Path
from typing import List, Dict, Tuple

# Default data directories
DEFAULT_DATA_DIRS = [
    Path("/media/ubuntu/Archive/nick/Sepsis-R01/Data_All_UCSF_2025-12-22"),
    Path("/media/ubuntu/Archive/nick/Sepsis-R01/Data_All_SFDPH_2025-12-22"),
]


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Analyze most commonly recorded time-varying covariates',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python 03_analyze_covariates.py                    # Use default paths
    python 03_analyze_covariates.py -i /path/to/data  # Specify data folder(s)
    python 03_analyze_covariates.py --top 100         # Show top 100
        """
    )
    parser.add_argument(
        '--input-dir', '-i',
        action='append',
        dest='input_dirs',
        help='Input data directory (can specify multiple with -i dir1 -i dir2)'
    )
    parser.add_argument(
        '--top', '-n',
        type=int,
        default=50,
        help='Number of top covariates to display (default: 50)'
    )
    parser.add_argument(
        '--output', '-o',
        type=str,
        default='top_50_covariates.csv',
        help='Output CSV filename (default: top_50_covariates.csv)'
    )
    return parser.parse_args()


def get_site_name(folder_path: Path) -> str:
    """Extract site name from folder path (e.g., UCSF or SFDPH)"""
    name = folder_path.name
    if 'UCSF' in name:
        return 'UCSF'
    elif 'SFDPH' in name:
        return 'SFDPH'
    else:
        return name


def load_parquet_safe(filepath: Path) -> pd.DataFrame:
    """Load parquet file, return empty DataFrame if not found"""
    if not filepath.exists():
        return pd.DataFrame()
    return pd.read_parquet(filepath)


def analyze_labs(data_dir: Path, site: str) -> pd.DataFrame:
    """Analyze lab covariates from a single site"""
    filepath = data_dir / 'labs.parquet'
    df = load_parquet_safe(filepath)

    if df.empty:
        print(f"  Warning: {filepath} not found or empty")
        return pd.DataFrame()

    # Standardize column names
    df.columns = df.columns.str.lower()

    # Group by component name
    stats = df.groupby('componentname').agg(
        record_count=('componentname', 'size'),
        unique_encounters=('encounterkey', 'nunique')
    ).reset_index()

    stats['covariate_name'] = stats['componentname']
    stats['data_type'] = 'Laboratory'
    stats['site'] = site

    return stats[['covariate_name', 'data_type', 'site', 'record_count', 'unique_encounters']]


def analyze_flowsheet(data_dir: Path, site: str, file_type: str) -> pd.DataFrame:
    """Analyze flowsheet covariates from a single site

    Args:
        data_dir: Path to data directory
        site: Site name (UCSF or SFDPH)
        file_type: Either 'assess' (vitals) or 'resp' (respiratory)
    """
    filename = f'flowsheet_{file_type}.parquet'
    filepath = data_dir / filename
    df = load_parquet_safe(filepath)

    if df.empty:
        print(f"  Warning: {filepath} not found or empty")
        return pd.DataFrame()

    # Standardize column names
    df.columns = df.columns.str.lower()

    # Group by flowsheet row name
    stats = df.groupby('flowsheetrowname').agg(
        record_count=('flowsheetrowname', 'size'),
        unique_encounters=('encounterkey', 'nunique')
    ).reset_index()

    stats['covariate_name'] = stats['flowsheetrowname']
    stats['data_type'] = 'Vital Signs' if file_type == 'assess' else 'Respiratory'
    stats['site'] = site

    return stats[['covariate_name', 'data_type', 'site', 'record_count', 'unique_encounters']]


def combine_site_data(all_stats: List[pd.DataFrame]) -> pd.DataFrame:
    """Combine statistics from all sites and data types"""
    if not all_stats:
        return pd.DataFrame()

    combined = pd.concat(all_stats, ignore_index=True)

    # Pivot to get site-specific columns
    pivot_records = combined.pivot_table(
        index=['covariate_name', 'data_type'],
        columns='site',
        values='record_count',
        aggfunc='sum',
        fill_value=0
    ).reset_index()

    pivot_encounters = combined.pivot_table(
        index=['covariate_name', 'data_type'],
        columns='site',
        values='unique_encounters',
        aggfunc='sum',
        fill_value=0
    ).reset_index()

    # Flatten column names for records
    pivot_records.columns = [
        f'{col}_records' if col not in ['covariate_name', 'data_type'] else col
        for col in pivot_records.columns
    ]

    # Flatten column names for encounters
    pivot_encounters.columns = [
        f'{col}_encounters' if col not in ['covariate_name', 'data_type'] else col
        for col in pivot_encounters.columns
    ]

    # Merge records and encounters
    result = pivot_records.merge(
        pivot_encounters,
        on=['covariate_name', 'data_type'],
        how='outer'
    )

    # Calculate totals
    record_cols = [c for c in result.columns if c.endswith('_records')]
    encounter_cols = [c for c in result.columns if c.endswith('_encounters')]

    result['total_records'] = result[record_cols].sum(axis=1)
    result['total_encounters'] = result[encounter_cols].sum(axis=1)

    # Add site column indicating which site(s) have data
    def get_sites(row):
        sites = []
        for col in record_cols:
            if row[col] > 0:
                site_name = col.replace('_records', '')
                sites.append(site_name)
        return ', '.join(sorted(sites)) if sites else 'None'

    result['sites'] = result.apply(get_sites, axis=1)

    # Sort by total records descending
    result = result.sort_values('total_records', ascending=False).reset_index(drop=True)

    return result


def print_results(df: pd.DataFrame, top_n: int):
    """Print formatted results table"""
    if df.empty:
        print("No data found!")
        return

    # Get site columns dynamically
    record_cols = [c for c in df.columns if c.endswith('_records') and c != 'total_records']
    sites = [c.replace('_records', '') for c in record_cols]

    print("\n" + "=" * 100)
    print(f"TOP {min(top_n, len(df))} TIME-VARYING COVARIATES BY RECORD COUNT")
    print("=" * 100)

    # Header
    header = f"{'Rank':<5} {'Covariate Name':<40} {'Type':<12} {'Sites':<12} "
    for site in sites:
        header += f"{site:>12} "
    header += f"{'Total':>14} {'Encounters':>12}"
    print(header)
    print("-" * 110)

    # Data rows
    for idx, row in df.head(top_n).iterrows():
        line = f"{idx+1:<5} {row['covariate_name'][:39]:<40} {row['data_type']:<12} {row['sites']:<12} "
        for site in sites:
            col = f"{site}_records"
            if col in row:
                line += f"{int(row[col]):>12,} "
            else:
                line += f"{'N/A':>12} "
        line += f"{int(row['total_records']):>14,} {int(row['total_encounters']):>12,}"
        print(line)

    print("-" * 100)

    # Summary
    print(f"\nTotal unique covariates: {len(df)}")
    print(f"Total records across all covariates: {df['total_records'].sum():,}")


def main():
    """Main analysis workflow"""
    args = parse_args()

    # Determine input directories
    if args.input_dirs:
        data_dirs = [Path(d) for d in args.input_dirs]
    else:
        data_dirs = DEFAULT_DATA_DIRS

    print("=" * 70)
    print("SEPSIS R01 TIME-VARYING COVARIATE ANALYSIS")
    print("=" * 70)

    # Validate directories
    valid_dirs = []
    for d in data_dirs:
        if d.exists():
            print(f"✓ Found data directory: {d}")
            valid_dirs.append(d)
        else:
            print(f"✗ Directory not found: {d}")

    if not valid_dirs:
        print("\nERROR: No valid data directories found!")
        sys.exit(1)

    # Collect statistics from all sources
    all_stats = []

    for data_dir in valid_dirs:
        site = get_site_name(data_dir)
        print(f"\nAnalyzing {site}...")

        # Labs
        print(f"  Loading labs...")
        lab_stats = analyze_labs(data_dir, site)
        if not lab_stats.empty:
            print(f"    Found {len(lab_stats)} lab components, {lab_stats['record_count'].sum():,} records")
            all_stats.append(lab_stats)

        # Flowsheet - Vitals
        print(f"  Loading vitals (flowsheet_assess)...")
        vitals_stats = analyze_flowsheet(data_dir, site, 'assess')
        if not vitals_stats.empty:
            print(f"    Found {len(vitals_stats)} vital signs, {vitals_stats['record_count'].sum():,} records")
            all_stats.append(vitals_stats)

        # Flowsheet - Respiratory
        print(f"  Loading respiratory (flowsheet_resp)...")
        resp_stats = analyze_flowsheet(data_dir, site, 'resp')
        if not resp_stats.empty:
            print(f"    Found {len(resp_stats)} respiratory params, {resp_stats['record_count'].sum():,} records")
            all_stats.append(resp_stats)

    # Combine all statistics
    print("\nCombining data from all sites...")
    combined = combine_site_data(all_stats)

    # Print results
    print_results(combined, args.top)

    # Save to CSV
    output_path = args.output
    combined.to_csv(output_path, index=False)
    print(f"\n✓ Detailed results saved to: {output_path}")

    # Also save a simpler version for quick reference
    simple_cols = ['covariate_name', 'data_type', 'total_records', 'total_encounters']
    site_record_cols = [c for c in combined.columns if c.endswith('_records') and c != 'total_records']
    simple_output = combined[simple_cols + site_record_cols].copy()
    simple_output = simple_output.rename(columns={'covariate_name': 'name', 'data_type': 'type'})

    print("\n" + "=" * 70)
    print("ANALYSIS COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
