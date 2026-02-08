# Sepsis R01 Data Extraction - Parquet Workflow

This directory contains a streamlined **single-step** workflow for extracting clinical data from the UCSF De-Identified Clinical Data Warehouse (DeID CDW) using **DuckDB and Parquet files**.

## Overview

**One command extracts all data:**
```bash
python 01_extract_data.py
```

All data extraction and processing is now consolidated into a single Python script. No separate R processing step is required!

## System Requirements

**This code runs on a local high-memory workstation:**
- **RAM:** 503GB (uses 400GB for DuckDB)
- **CPU:** 32 cores (uses 24 threads for DuckDB)
- **GPU:** 2x NVIDIA RTX A5000 (24GB each) - available for future ML work
- **Data location:** `/media/ubuntu/HDD Storage/parquet/`

## Requirements

### Python Dependencies
```bash
pip install pandas duckdb pyarrow openpyxl
```

**Setting up the environment:**
```bash
# Create virtual environment (first time only)
python3 -m venv .venv

# Activate the virtual environment
source .venv/bin/activate

# Install required packages
pip install -r requirements.txt
```

### R Dependencies (for reading Parquet files)
```r
install.packages("arrow")
```

### Additional Requirements
- Excel file: `Sepsis_R01_LabFlow_2023-11-29.xlsx` (must be in the same directory)
- Access to parquet files: `/media/ubuntu/HDD Storage/parquet/deid_cdw/`

## Usage

**Run extraction:**

```bash
# Activate the virtual environment
source .venv/bin/activate

# Run the extraction script (UCSF only - default)
python 01_extract_data.py

# Extract SFDPH only
python 01_extract_data.py --data-asset deid_cdw_sfdph

# Extract both UCSF and SFDPH
python 01_extract_data.py --data-asset all
```

**Run in background with logging:**

```bash
nohup bash -c "source .venv/bin/activate && python -u 01_extract_data.py --data-asset all" > 01_extract_data_$(date +%Y%m%d%H%M%S).log 2>&1 &
```

### What the Script Does

1. Registers DuckDB tables from Parquet files at `/media/ubuntu/HDD Storage/parquet/deid_cdw/`
2. Creates encounter and patient key filters (adult ED visits at Parnassus or SFGH)
3. Extracts 17 clinical datasets
4. **Applies all necessary data transformations** (sorting dx_enc_icd)
5. Saves all data as Parquet files in `Data_All_<SITE>_YYYY-MM-DD/` folder

### DuckDB Configuration

- Memory limit: 400GB (configurable with `--memory-gb`)
- Threads: 24 (configurable with `--threads`)
- Temporary directory for large operations

### Output

**Individual Parquet files:** `Data_All_YYYY-MM-DD/*.parquet` (20 files)
**Unified database:** `Data_All_YYYY-MM-DD/HTESepsis_UCSFDeIDCDW_All_YYYY-MM-DD.duckdb` (1 file with all 22 tables)

## Output Files

### Individual Parquet Files

All 20 tables are saved as separate Parquet files in `Data_All_YYYY-MM-DD/`:

| File | Description |
|------|-------------|
| `enc_keys.parquet` | Encounter keys for qualifying ED visits |
| `pt_keys.parquet` | Patient keys (unique patients) |
| `dur.parquet` | Duration dimension table |
| `img_all_proc.parquet` | All imaging procedures summary |
| `img_lung_proc.parquet` | Lung/chest imaging procedures summary |
| `img_lung.parquet` | Lung/chest imaging details |
| `enc.parquet` | Encounter fact table |
| `enc_dx.parquet` | Encounters with primary diagnosis |
| `pt.parquet` | Patient dimension table |
| `ed.parquet` | ED visit fact table |
| `hosp_adm.parquet` | Hospital admission fact table |
| `icu_reg.parquet` | ICU stay registry data |
| `dx_enc_icd.parquet` | All diagnosis events (ICD-10-CM) - **sorted by patient, date, diagnosis** |
| `med_ab.parquet` | Antibiotic medications |
| `med_vp.parquet` | Vasopressor medications (IV only) |
| `flowsheet_assess.parquet` | Assessment flowsheet data |
| `flowsheet_resp.parquet` | Respiratory flowsheet data |
| `labs.parquet` | Laboratory results |
| `death.parquet` | Death registry data |
| `insurance.parquet` | Insurance/coverage data |

### Unified Database File

**ONE file with ALL tables:** `HTESepsis_UCSFDeIDCDW_All_YYYY-MM-DD.duckdb`

This DuckDB database contains all 22 tables (20 data tables + lab_keys + flowsheet_row_keys) in a single file, similar to the old RData format but with better performance and cross-language compatibility.

## Reading Data Files

### Reading the Unified Database (RECOMMENDED)

The unified DuckDB file is the easiest way to load all data, similar to the old RData workflow:

#### In R
```r
library(duckdb)

# Connect to the unified database
con <- dbConnect(duckdb(), "Data_All_2025-10-16/HTESepsis_UCSFDeIDCDW_All_2025-10-16.duckdb")

# Load a single table
enc_keys <- dbReadTable(con, "enc_keys")

# List all tables
dbListTables(con)

# Load all tables into environment
for (table in dbListTables(con)) {
  assign(table, dbReadTable(con, table), envir = .GlobalEnv)
}

dbDisconnect(con)
```

#### In Python
```python
import duckdb

# Connect to the unified database
con = duckdb.connect("Data_All_2025-10-16/HTESepsis_UCSFDeIDCDW_All_2025-10-16.duckdb")

# Load a single table
enc_keys = con.execute("SELECT * FROM enc_keys").df()

# Load all tables into a dictionary
tables = con.execute("SHOW TABLES").df()['name'].tolist()
data_dict = {table: con.execute(f"SELECT * FROM {table}").df() for table in tables}

con.close()
```

### Reading Individual Parquet Files

#### In R
```r
library(arrow)

# Read a single file
enc_keys <- read_parquet("Data_All_2025-10-16/enc_keys.parquet")

# Read all files into a list
library(purrr)
data_folder <- "Data_All_2025-10-16"
files <- list.files(data_folder, pattern = "\\.parquet$", full.names = TRUE)
data_list <- map(files, read_parquet)
names(data_list) <- tools::file_path_sans_ext(basename(files))
```

#### In Python
```python
import pandas as pd

# Read a single file
enc_keys = pd.read_parquet("Data_All_2025-10-16/enc_keys.parquet")

# Read all files into a dictionary
from pathlib import Path

data_folder = Path("Data_All_2025-10-16")
data_dict = {
    f.stem: pd.read_parquet(f)
    for f in data_folder.glob("*.parquet")
}
```

## Inclusion Criteria

**Encounter Selection:**
- ED visits at Parnassus ED (`EMERGENCY DEPT PARN`)
- Patients ≥18 years old at time of ED arrival
- All dates included (no date restrictions)

**Medication Filtering:**
- Antibiotics: All records with `MedicationTherapeuticClass='ANTIBIOTICS'`
- Vasopressors: IV route only, including:
  - Norepinephrine, Vasopressin, Epinephrine, Dobutamine, Milrinone, Phenylephrine, Dopamine

## Benefits of New Workflow

✅ **Single command** - One script does everything
✅ **Individual files** - 20 Parquet files for flexibility
✅ **Unified file** - ONE DuckDB file with all tables (like RData)
✅ **Smaller sizes** - 5-10x compression vs CSV
✅ **Faster I/O** - Columnar storage, selective reading
✅ **Type preservation** - Dates, integers, etc. maintained
✅ **Cross-language** - Works in R and Python
✅ **No column issues** - Exact names preserved

## Migration from Old Workflow

**Old workflow:**
```
extract_data.py → CSVs → process_data.Rmd → RData (R-only)
```

**New workflow:**
```
extract_data.py → Parquet files + DuckDB file (R & Python)
```

**What changed:**
- ✅ Individual Parquet files (new) - flexible access
- ✅ Unified DuckDB file (replaces RData) - R & Python compatible
- ❌ CSV files (eliminated) - slow, loses types
- ❌ process_data.Rmd (eliminated) - logic moved to Python
- ❌ RData format (eliminated) - R-only, incompatible with Python

## Data Transformations

The only data transformation applied:
- `dx_enc_icd.parquet` is sorted by `PatientDurableKey`, `StartDateKeyValue`, `DiagnosisKey`

This replicates the logic from the legacy `process_data.Rmd` workflow.

## Troubleshooting

### Column name case sensitivity
DuckDB may return lowercase column names from Parquet files. The script handles this automatically by checking for both PascalCase and lowercase versions.

### Memory issues
Reduce `--memory-gb` argument (default is 400GB) or process in smaller batches.

## Dataset Summary and Analysis

After extracting data, use the summary script to generate a comprehensive clinical overview:

```bash
# Generate summary for today's data
python summarize_data.py

# Or specify a specific data folder
python summarize_data.py Data_All_2025-10-14
```

### What the Summary Script Provides

The `summarize_data.py` script generates a comprehensive clinical report focused on the study's CDSS development aims:

#### 1. Cohort Overview
- Total patients and ED encounters
- Date range of data collection
- Demographics (age, sex, race, ethnicity)
- Encounters per patient

#### 2. Clinical Outcomes
- Hospital admission rates from ED
- ICU admission rates
- Mortality statistics (in-hospital and death registry)
- Length of stay distributions (ED, hospital, ICU)
- ED disposition patterns

#### 3. Sepsis Indicators
- Top 20 primary ED diagnoses
- Sepsis-related diagnoses prevalence
- Pneumonia and lung infection rates
- ICD-10 code hierarchy analysis

#### 4. Treatment Patterns (CRITICAL FOR CDSS)
**Antibiotics:**
- Encounters receiving antibiotics
- Top 15 most commonly administered agents
- Time to first antibiotic dose (median, IQR, mean ± SD)
- Administration patterns

**Vasopressors:**
- Encounters requiring vasopressor support
- Vasopressor types (norepinephrine, vasopressin, epinephrine, etc.)
- Time to vasopressor initiation
- Usage frequency by agent

#### 5. Imaging Procedures
- Lung/chest imaging rates
- Procedure types (CT, X-ray, fluoroscopy)
- Imaging frequency across encounters

#### 6. Physiological Data Availability
**Laboratory Tests:**
- Top 20 most common lab tests
- Key sepsis markers coverage:
  - Lactate, WBC, creatinine, procalcitonin, CRP
  - Blood culture rates
- Availability across encounters

**Vital Signs and Assessments:**
- Assessment flowsheet coverage
- Respiratory monitoring data availability

#### 7. Data Quality Metrics
- Dataset completeness for all tables
- Missing data rates for critical fields
- Field-level completeness percentages

#### 8. Insurance/Payor Data
- Payor financial class distribution
- Top 10 insurance providers

### Output Files

1. **Console Output**: Formatted, color-coded summary with tables and statistics
2. **JSON Summary**: `summary_stats_YYYYMMDD_HHMMSS.json` with machine-readable statistics

Example JSON structure:
```json
{
  "generated_at": "2025-10-17T10:30:00",
  "cohort": {
    "n_patients": 50000,
    "n_encounters": 65000
  },
  "outcomes": {
    "n_hospital_admissions": 25000,
    "n_icu_admissions": 5000,
    "n_deaths": 2000
  },
  "treatments": {
    "encounters_with_antibiotics": 30000,
    "encounters_with_vasopressors": 3000
  },
  "data_availability": {
    "lab_results": 500000,
    "flowsheet_assess_records": 1000000,
    "flowsheet_resp_records": 200000,
    "imaging_procedures": 15000
  }
}
```

### Clinical Interpretation Guide

**Understanding Treatment Timing:**
- **Time to Antibiotics Goal**: <1 hour for sepsis (Surviving Sepsis Campaign)
- **Time to Vasopressors**: After adequate fluid resuscitation for refractory hypotension
- **CDSS Value**: Predict which patients benefit from faster or delayed treatment

**Risk Stratification Indicators:**
- **Low Risk**: Normal vitals, no organ dysfunction, normal lactate
- **High Risk**: Multi-organ dysfunction, persistent hypotension, elevated lactate (>2 mmol/L)

**CDSS Optimization Targets:**
1. Antibiotic selection and timing
2. Fluid administration balance
3. Vasopressor initiation timing and agent selection

## Complete Workflow Example

```bash
# 1. Extract data (creates Data_All_YYYY-MM-DD folder)
python extract_data.py

# 2. Generate comprehensive summary
python summarize_data.py

# 3. Access data in Python for analysis
python
>>> import pandas as pd
>>> enc_keys = pd.read_parquet('Data_All_2025-10-17/enc_keys.parquet')
>>> med_ab = pd.read_parquet('Data_All_2025-10-17/med_ab.parquet')

# 4. Or access in R
R
> library(arrow)
> enc_keys <- read_parquet('Data_All_2025-10-17/enc_keys.parquet')
```

## File Structure

```
Full_Extraction/
├── README.md                           # This file
├── requirements.txt                    # Python package requirements
├── extract_data.py                     # Data extraction script (Parquet + DuckDB)
├── summarize_data.py                   # Dataset summary and analysis script
├── Sepsis_R01_LabFlow_2023-11-29.xlsx # Lab/flowsheet keys (required)
├── process_data.Rmd                    # Legacy R processing (now obsolete)
├── SepsisR01_DeIDCDW_2_2023-12-27.Rmd # Original SQL-based script (reference)
└── Data_All_YYYY-MM-DD/               # Output folder (created by script)
    ├── *.parquet                       # 20 individual Parquet files
    ├── HTESepsis_UCSFDeIDCDW_All_YYYY-MM-DD.duckdb  # Unified database
    └── summary_stats_YYYYMMDD_HHMMSS.json           # Summary statistics
```

## Notes

- **System requirement**: Runs on local high-memory workstation (503GB RAM, 32 cores)
- **Virtual environment**: Use `.venv` for correct package versions
- **No password required**: Reads directly from Parquet files
- **Column name handling**: Automatically handles case variations from DuckDB/Parquet
- **Date stamping**: Folder names automatically include `YYYY-MM-DD`
- **Excel file**: `Sepsis_R01_LabFlow_2023-11-29.xlsx` must be present
- **Source data**: `/media/ubuntu/HDD Storage/parquet/deid_cdw/`
