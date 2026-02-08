# Recent Changes to Sepsis R01 Data Extraction Pipeline

## Overview

I recently completed two major enhancements to our data extraction pipeline:

1. **SFDPH (San Francisco Department of Public Health) data extraction support**
2. **Cohort filtering script** to apply our sepsis inclusion criteria

## What I Did

### 1. SFDPH Data Extraction (`01_extract_data.py`)

I extended our existing extraction script to support **both UCSF and SFDPH** data sources.

#### Key Changes:
- **Multi-institution support**: The script can now extract from:
  - `deid_cdw` (UCSF Parnassus ED) - our original source
  - `deid_cdw_sfdph` (San Francisco General Hospital ED) - **NEW**
  - Both simultaneously with `--data-asset all`

- **SFDPH-specific configurations**: I identified and mapped SFDPH's different coding systems:
  - Lab component keys (e.g., WBC Count = 17512 in SFDPH vs 994 in UCSF)
  - Flowsheet row keys (e.g., Temperature = 29366 in SFDPH vs 34432 in UCSF)
  - ED department name ("ED" vs "EMERGENCY DEPT PARN")

- **Handled data differences**: SFDPH doesn't have:
  - ICU registry data
  - Death registry data
  - SDOH (Social Determinants of Health) data

#### How It Works:

```bash
# Extract UCSF only (default)
python 01_extract_data.py

# Extract SFDPH only
python 01_extract_data.py --data-asset deid_cdw_sfdph

# Extract both UCSF and SFDPH
python 01_extract_data.py --data-asset all
```

#### Output Structure:
Each extraction creates a dated folder with the institution identifier:
- `Data_All_UCSF_2025-01-05/` - UCSF data
- `Data_All_SFDPH_2025-01-05/` - SFDPH data

Both contain the same file structure:
- 20 individual `.parquet` files (enc_keys, labs, flowsheet_assess, etc.)
- 1 unified `.duckdb` file with all tables

### 2. Cohort Filtering Script (`02_filter_data.py`)

I created a **new filtering script** that applies our sepsis inclusion criteria to extracted data.

#### Inclusion Criteria Implemented:

**Primary Criteria (MUST meet both):**
1. **CBC within 2 hours** of ED arrival
2. **Abnormal temp OR abnormal WBC** within first 2 hours:
   - Temperature <36°C or >38.5°C
   - WBC >12,000 or <4,000/µL

**Additional Flags (tracked but not exclusion criteria):**
- `has_chest_imaging`: Chest x-ray or CT within first 4 hours
- `has_hypotension`: At least 1 SBP <90 mmHg reading within first 4 hours

#### How It Works:

```bash
# Filter UCSF data
python 02_filter_data.py -i Data_All_UCSF_2024-01-15

# Filter SFDPH data
python 02_filter_data.py -i Data_All_SFDPH_2024-01-15

# Filter both and create combined cohort
python 02_filter_data.py -i Data_All_UCSF_2024-01-15 -i Data_All_SFDPH_2024-01-15
```

#### Output Files:

The script saves outputs to each input folder:
- `filtered_cohort.parquet` - All encounters meeting inclusion criteria
- `filtered_cohort_hypotensive.parquet` - Hypotensive subpopulation
- `filtered_cbc_within_2h.parquet` - Intermediate: CBC timing
- `filtered_abnormal_temp_wbc.parquet` - Intermediate: SIRS criteria
- `filtered_chest_imaging.parquet` - Intermediate: Imaging tracking
- `filtered_hypotension.parquet` - Intermediate: Hypotension tracking

If multiple folders are provided, it also creates a combined output folder:
- `Filtered_Combined_YYYY-MM-DD/`
  - `filtered_cohort_combined.parquet`
  - `filtered_cohort_hypotensive_combined.parquet`

#### Key Features:

- **Time zero definition**: First CBC time becomes our "time zero" for the study
- **Hospital-agnostic**: Same logic works for both UCSF and SFDPH despite different data structures
- **Detailed tracking**: Saves intermediate results for debugging and analysis
- **Clinical validation**: Uses standard sepsis criteria (temperature, WBC thresholds)

## File Organization

```
Full_Extraction/
├── 01_extract_data.py              # Data extraction (UCSF + SFDPH)
├── 02_filter_data.py               # Cohort filtering (NEW)
├── Sepsis_R01_LabFlow_2023-11-29.xlsx  # UCSF lab/flowsheet keys
├── Data_All_UCSF_YYYY-MM-DD/      # UCSF extraction output
│   ├── *.parquet (20 files)
│   ├── HTESepsis_UCSFDeIDCDW_All_*.duckdb
│   └── filtered_*.parquet (6 files)  # NEW: Filtering outputs
├── Data_All_SFDPH_YYYY-MM-DD/     # SFDPH extraction output (NEW)
│   ├── *.parquet (17 files - no ICU/death/SDOH)
│   ├── HTESepsis_SFDPHDeIDCDW_All_*.duckdb
│   └── filtered_*.parquet (6 files)  # NEW: Filtering outputs
└── Filtered_Combined_YYYY-MM-DD/  # Combined cohort (NEW)
    ├── filtered_cohort_combined.parquet
    └── filtered_cohort_hypotensive_combined.parquet
```

## Example Workflow

```bash
# Step 1: Extract UCSF and SFDPH data
python 01_extract_data.py --data-asset all

# Step 2: Filter both cohorts and create combined dataset
python 02_filter_data.py \
    -i Data_All_UCSF_2025-01-05 \
    -i Data_All_SFDPH_2025-01-05

# Step 3: Analyze filtered cohort
python
>>> import pandas as pd
>>> cohort = pd.read_parquet('Filtered_Combined_2025-01-05/filtered_cohort_combined.parquet')
>>> print(cohort.groupby('source_folder').size())
```

## Technical Challenges I Solved

### 1. SFDPH Lab Keys Discovery
SFDPH uses completely different lab component keys than UCSF. I:
- Queried the raw `labcomponentresultfact` table to find popular lab tests
- Cross-referenced component names to identify CBC and chemistry panel keys
- Created `check_sfdph_labs.py` to validate the mapping

### 2. Temporal Alignment Across Institutions
Both institutions record times differently:
- UCSF: Multiple datetime columns (`ArrivalDateKeyValue`, `ResultInstant`, etc.)
- SFDPH: Similar but different column availability

I made the filter script **column-agnostic** by checking for multiple possible column names and using whichever exists.

### 3. Missing Data Handling
SFDPH doesn't have ICU registry data, death registry, or SDOH tables. I:
- Made the extraction script conditionally extract these tables only when available
- Ensured filtering script works with just the core clinical data

## What This Enables

1. **Multi-site sepsis research**: We can now study sepsis across UCSF and SFGH patient populations
2. **Reproducible cohorts**: Filtering script creates consistent, criteria-based cohorts
3. **Subpopulation analysis**: Easy identification of hypotensive subgroup for treatment studies
4. **Quality control**: Intermediate files allow validation of each filtering step

## Next Steps

For analysis, you can:
1. Load the combined filtered cohort
2. Link back to the full extracted data using `encounterkey`
3. Perform time-based analyses using `time_zero` as the reference point
4. Compare UCSF vs SFDPH populations using `source_folder` column

## Questions?

See:
- [README.md](README.md) - Full extraction workflow documentation
- [FILES.md](FILES.md) - Detailed description of all data files
- Source code comments in `01_extract_data.py` and `02_filter_data.py`
