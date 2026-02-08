# Files

Extracted data lives in `Data_All_<SITE>_YYYY-MM-DD/`. Filtered cohorts go to `Filtered_Combined_YYYY-MM-DD/`. All files are Parquet (+ CSV mirrors for some). A unified DuckDB file bundles everything per site.

SFDPH does not have: `icu_reg`, `death`, `sdoh_*`.

All files contain `deidlds` (constant, always `deid_uf` or `deid_sfdph`) and `count` (always 1) — ignore both. Many files have columns set to `"DEID"` where PHI was stripped; these are noted below.

## Pipeline Scripts

| Script | Purpose |
|--------|---------|
| `01_extract_data.py` | Extract from raw CDW parquet files into per-site output folders |
| `02_filter_data.py` | Apply sepsis inclusion criteria, produce filtered cohorts |
| `03_analyze_covariates.py` | Rank time-varying covariates by recording frequency |
| `summarize_data.py` | Generate clinical summary report + JSON stats |

## Identifiers

### enc_keys (5 cols)
Encounter keys for qualifying ED visits (adults >=18, site-specific ED).
- **Join keys:** `EncounterKey`, `PatientKey`, `PatientDurableKey`
- `ArrivalDateKeyValue` — ED arrival date
- `HospitalAdmissionKey` — links to `hosp_adm` (if admitted)

### pt_keys (2 cols)
Unique patients from enc_keys.
- `PatientDurableKey`, `PatientKey`

## Demographics & Encounters

### pt (60 cols)
Patient demographics. One row per patient.
- **Join key:** `patientdurablekey`, `patientkey`
- **Useful:** `sex`, `preferredlanguage`, `ethnicity`, `firstrace`–`fifthrace`, `multiracial`, `birthdate`, `deathdate`, `status`, `maritalstatus`, `religion`, `smokingstatus`, `primaryfinancialclass`, `sexassignedatbirth`, `genderidentity`, `sexualorientation`, `ucsfderivedraceethnicity_x`, `mychartstatus`
- **DEID (all constant "DEID"):** `name`, `firstname`, `middlename`, `lastname`, `ssn`, `address`, `city`, `county`, `homephonenumber`, `workphonenumber`, `deathlocation`, `emailaddress`
- **Always null:** `latitude`, `longitude`

### enc (84 cols)
Encounter facts. One row per encounter.
- **Join keys:** `encounterkey`, `patientdurablekey`, `patientkey`
- **Useful:** `primarydiagnosiskey`, `primarydiagnosisname`, `agekeyvalue`, `datekeyvalue` (encounter date), `enddatekeyvalue`, `admissiondatekeyvalue`, `dischargedatekeyvalue`, `departmentname`, `departmentspecialty`, `admissiontype`, `admissionsource`, `dischargedisposition`, `admissioninstant`, `dischargeinstant`, `visittype`, `derivedencounterstatus`, `patientclass`, `ishospitaladmission`, `isedvisit`, `isoutpatientfacetofacevisit`
- **Always null:** all cost columns (`totalcost` through `otherdirectvariablecost`)
- **Always constant:** `type` = "Hospital Encounter", `isedvisit` = 1, `isskillednursingfacilityvisit` = 0

### enc_dx (96 cols)
Same as `enc` plus primary ICD-10-CM diagnosis.
- **Additional useful:** `icd_Value` (ICD-10 code), `icd_diagnosisname`, `icd_displaystring`, `icd_l1_name` through `icd_l9_name` (hierarchical groupings)

### ed (241 cols)
ED visit facts. One row per ED encounter. Many timestamp pairs (datekey + timeofdaykey) for ED milestones.
- **Join keys:** `encounterkey`, `patientdurablekey`, `hospitaladmissionkey`
- **Timing:** `arrivaldatekeyvalue` + `arrivaltimeofdaykeyvalue` (or `arrivalinstant`), `departuredatekeyvalue` + `departuretimeofdaykeyvalue`, `dispositioninstant`, `triagestartinstant`, `triagecompleteinstant`, `roomedinstant`, `firstprovidercontactinstant`, `firstekgcompletedinstant`, `firstctcompletedinstant`, `admissiondecisioninstant`
- **Clinical:** `acuitylevel` (ESI), `arrivalmethod`, `eddisposition`, `dischargedisposition`, `levelofcare`, `financialclass`, `primaryeddiagnosisname`, `primarychiefcomplaintname`, `chiefcomplaintcombokey`
- **Flags:** `underobservation`, `leftwithoutbeingseen`, `leftagainstmedicaladvice`, `behavioralhealth`, `behavioralrestraintsused`, `nonbehavioralrestraintsused`
- **Always null:** all cost columns, trauma/thrombolytic/arterial inflation timestamps (rare events)

### hosp_adm (105 cols)
Hospital admissions (subset of encounters that were admitted).
- **Join keys:** `encounterkey`, `hospitaladmissionkey`, `patientdurablekey`
- **Useful:** `admissiondatekeyvalue` + `admissiontimeofdaykeyvalue`, `dischargedatekeyvalue` + `dischargetimeofdaykeyvalue`, `dischargeinstant`, `lengthofstayindays`, `inpatientlengthofstayindays`, `expectedinpatientlengthofstayindays`, `admittingdepartmentname`, `departmentname`, `hospitalservice`, `admissiontype`, `admissionsource`, `dischargedisposition`, `patientclass`, `encountertype`, `financialclass`, `drgname`, `drgcode`, `principalproblemdiagnosisname`, `primarycodeddiagnosisname`

### icu_reg (160 cols)
ICU stays. UCSF only. Rich summary with min/max/most-recent vitals and labs during the ICU stay.
- **Join keys:** `encounterkey`, `patientdurablekey`
- **Timing:** `icustaystartinstant`, `icustayendinstant`, `iculengthofstay`, `ageaticustaystartvalue`
- **Location:** `departmentkey`, `previousdepartmentkey`, `nextdepartmentkey`
- **Vitals summary:** `minimumpulse`/`maximumpulse`, `minimumsystolicbloodpressure`/`maximumsystolicbloodpressure`, `minimummeanarterialpressure`, `minimumtemperatureincelsius`/`maximumtemperatureincelsius`, `maximumfio2`, `mostrecentfio2`, `minimumpao2fio2ratio`
- **Labs summary:** `maximumlactate`, `minimumcreatinine`/`maximumcreatinine`, `minimumplateletcount`, `minimumwbc`/`maximumwbc`, `maximumbilirubin`, `minimumhematocrit`, `maximuminr`, `maximumprocalcitonin`
- **Scores:** `maximumsofascore`, `maximumsapsiiscore`, `minimumgcsscore`, `firstapacheiiscore`, `epicicuriskofmortality`
- **Interventions:** `ventilatordays`, `intubationdays`, `centrallinedays`, `foleydays`, `methodofventilation`, `minimumpeep`
- **Outcomes:** `expiredinhospital`, `expiredinicu`, `receivedvasopressor`, `receivedhemodialysis`, `onventilator`, `wasextubated`, `wasintubated`, `haddelirium`, `hadpressureulcer`, `receivedantibiotic`, `hadpositiveculture`

## Diagnoses

### dx_enc_icd (40 cols)
All ICD-10-CM diagnosis events. Multiple rows per encounter.
- **Join keys:** `encounterkey`, `patientdurablekey`, `diagnosiskey`
- **Useful:** `diagnosisname`, `icd_value` (ICD-10 code), `icd_type`, `startdatekeyvalue`, `enddatekeyvalue`, `type`, `status`, `presentonadmission`, `hospitaldiagnosis`, `emergencydepartmentdiagnosis`, `chronic`
- **Hierarchy:** `icd_l1_name` through `icd_l9_name`
- Sorted by: `patientdurablekey`, `startdatekeyvalue`, `diagnosiskey`

## Imaging

### img_all_proc (3 cols)
Aggregated counts of all imaging procedures by name and category.
- `firstprocedurename`, `firstprocedurecategory`, `Count`

### img_lung_proc (3 cols)
Same as above, filtered to "chest" or "lung" procedures.

### img_lung (141 cols)
Row-level chest/lung imaging orders.
- **Join keys:** `encounterkey`, `patientdurablekey`
- **Useful:** `firstprocedurename`, `firstprocedurecategory`, `firstprocedurecptcode`, `studystatus`, `orderpriority`, `isabnormal`
- **Timing:** `orderingdatekeyvalue` + `orderingtimeofdaykeyvalue` (or `orderinginstant`), `examstartinstant`, `examendinstant`, `finalizinginstant`
- **Departments:** `orderingdepartmentname`, `performingdepartmentname`

## Medications

### med_ab / med_vp (107 cols each)
Antibiotic (`med_ab`) and IV vasopressor (`med_vp`) administrations. Same schema.
- **Join keys:** `encounterkey`, `patientdurablekey`
- **What was given:** `medicationname`, `medicationgenericname`, `medicationtherapeuticclass`, `medicationpharmaceuticalclass`, `medicationpharmaceuticalsubclass`, `medicationstrength`, `medicationform`, `medicationroute`, `dose`, `doseunit`
- **Timing:** `administrationdatekeyvalue` + `administrationtimeofdaykeyvalue` (or `administrationinstant`), `scheduledadministrationinstant`
- **Administration:** `administrationaction` (Given, Held, etc.), `administrationroute`, `administrationdepartmentname`, `isprn`, `timelyadministrationstatus`
- **Multi-component:** up to 5 components with `primarycomponentname`/`genericname`/`route` through `fifthcomponent*`
- **DEID:** `chargeamount`, `medicationrepresentativecost`, `medicationacquisitioncost` (all "DEID")

## Vital Signs & Flowsheet

### flowsheet_assess / flowsheet_resp (38 cols each)
Vital signs/assessments (`flowsheet_assess`) and respiratory parameters (`flowsheet_resp`). Same schema.
- **Join keys:** `encounterkey`, `patientdurablekey`
- **What was measured:** `flowsheetrowkey` (numeric key), `flowsheetrowname` (e.g., "BLOOD PRESSURE", "TEMPERATURE"), `flowsheetrowdisplayname`, `flowsheetrowunit`
- **Values:** `value` (string, always populated — e.g., "100/51", "98.6", "20"), `numericvalue` (float, populated for single-number values like temp/pulse/resp, NULL for compound values like BP)
- **Timing:** `datekeyvalue` + `timeofdaykeyvalue`, `takeninstant`, `firstdocumentedinstant`
- **Flags:** `abnormal`, `accepted`, `fromdevice`
- **Template:** `templatename`, `templatedisplayname`
- **Always DEID/constant/null:** `portalaccountkey` (DEID), `comment` (DEID), `occurrence` (-1), `datevalue` (>99% null), `timevalue` (>99% null), `linkedtopatiententeredflowsheetepisode` (0)

**flowsheet_assess covariates (UCSF):** RESPIRATIONS, PULSE, BLOOD PRESSURE, TEMPERATURE, R MAP, PULSE - PALPATED OR PLETH, R CPN ADULT GLASGOW COMA SCALE (3 components), R ARTERIAL LINE BLOOD PRESSURE (+ 2), WEIGHT/SCALE, R MAP A-LINE

**flowsheet_resp covariates (UCSF):** R FIO2 (%), R RESP PEEP/CPAP SET, R RT VENT MODE, R RT MODE/BREATH TYPE, R RT VENTILATOR RESPIRATORY RATE TOTAL, RT MAP MEASURED, R RT (ADULT) VENT TYPE, R RESP RR SET, R RT VENTILATOR MINUTE VOLUME MEASURED V2, R RT (ADULT) VENTILATOR TIDAL VOLUME EXHALED, R RT (ADULT) VENTILATOR I:E RATIO MEASURED, R OXYGEN SATURATION INDEX (OSI), R FIO2 2

## Labs

### labs (47 cols)
Lab results for selected components.
- **Join keys:** `encounterkey`, `patientdurablekey`, `labcomponentkey`
- **What was tested:** `componentname` (e.g., "WBC Count", "Creatinine"), `componentloinccode`, `labcomponentkey`, `resultinglablabname`, `procedurename`
- **Values:** `value` (string, the actual result — e.g., "12.5", "1.00"), `unit` (e.g., "g/dL", "mg/dL"), `flag`, `resultstatus`, `abnormal`, `referencevalues`
- **Timing:** `resultdatekeyvalue` + `resulttimeofdaykeyvalue`, `ordereddatekeyvalue` + `orderedtimeofdaykeyvalue`, `collectiondatekeyvalue` + `collectiontimeofdaykeyvalue`
- **DEID/useless:** `numericvalue` (always "DEID" — must parse from `value` instead), `componentdefaultunit` (>99% null)

**Lab components extracted (UCSF):** WBC Count, Hemoglobin, Hematocrit, Platelet Count, Auto Abs Neutrophil/Basophil/Eosinophil/Lymphocyte/Monocyte, Creatinine, Bilirubin Total/Direct, PO2, Lactate (whole blood + plasma), plus rare variants (POCT Hemoglobin, PO2 Venous, Creatinine POCT)

**Lab components extracted (SFDPH):** WBC Count, Hemoglobin, Hematocrit, Platelet Count, Creatinine Serum, Sodium, Potassium, Calcium, BUN, CO2, Chloride, Glucose Non Fasting, Albumin, ALT, Lactate

## Outcomes

### death (15 cols)
Death registry. UCSF only.
- **Join key:** `patientdurablekey`
- **Useful:** `deathdateucsfinternaluseonly`
- **DEID:** `placeofbirth`, `placeofdeathfacilitynamelocation`, `placeofdeathaddressstreetnumber`, `placeofdeathaddressstreetname`, `placeofdeathcity`, `placeofdeathcounty`, `fatherlastname` (all constant "DEID")

### insurance (43 cols)
Primary insurance coverage for hospital admissions.
- **Join key:** `EncounterKey`
- **Useful:** `coveragefinancialclass`, `coveragetype`, `benefitplanproducttype`, `benefitplantype`, `payorfinancialclass`
- **DEID:** almost all subscriber, payer, and address columns (constant "DEID") — `subscribername`, `subscriberssn`, `payorname`, `payorepicid`, `payoraddress`, etc.

## SDOH

### sdoh_fact (23 cols)
Social Determinants of Health screening events. UCSF only.
- **Join keys:** `encounterkey`, `patientdurablekey`, `socialdeterminantkey`
- **Useful:** `socialdeterminantdomain`, `levelofconcern`, `entryinterpretation`, `documentationsource`, `patientreportedstatus`, `socialdeterminantdatekeyvalue`

### sdoh_answer (12 cols)
SDOH screening answers linked to `sdoh_fact` via `socialdeterminantkey`. UCSF only.
- **Useful:** `answertext`, `flowsheetrowkey`

### sdoh_domain (15 cols)
Patient-level SDOH domain summaries. UCSF only.
- **Join key:** `patientdurablekey`
- **Useful:** `socialdeterminantdomain`, `isconcernpresent`, `levelofconcern`, `wantsassistance`

## Dimension Tables

### dur (11 cols)
Duration dimension for temporal calculations. Maps `durationkey` to `days`, `weeks`, `months`, `years` + display strings. Filtered to years in (-99, 99).

## Filtered Cohort Files

Produced by `02_filter_data.py`. Written to each `Data_All_*` folder and combined in `Filtered_Combined_YYYY-MM-DD/`.

### filtered_cohort
Encounters meeting sepsis inclusion criteria: CBC within 2h of ED arrival + abnormal temp (<36C or >38.5C) or WBC (<4K or >12K).
- `encounterkey`, `first_cbc_time`, `first_wbc_value`, `has_abnormal_temp`, `has_abnormal_wbc`, `meets_sirs_criteria`, `has_chest_imaging`, `first_imaging_time`, `has_hypotension`, `first_hypotension_time`, `min_sbp`, `time_zero`, `ed_arrival_time`

### filtered_cohort_hypotensive
Subset of `filtered_cohort` with SBP < 90 within 4h. Same columns.

### filtered_cbc_within_2h
Intermediate: encounters with any CBC result within 2h of ED arrival.

### filtered_abnormal_temp_wbc
Intermediate: encounters with abnormal temp or WBC within 2h.

### filtered_chest_imaging
Intermediate: encounters with chest imaging within 4h.

### filtered_hypotension
Intermediate: encounters with SBP < 90 within 4h.

### filtered_cohort_combined
Multi-site combined cohort (in `Filtered_Combined_YYYY-MM-DD/`). Same columns as `filtered_cohort` plus `source_folder`.

### filtered_cohort_hypotensive_combined
Multi-site combined hypotensive subgroup. Same columns as above.

## Analysis Outputs

### top_50_covariates.csv
Produced by `03_analyze_covariates.py`. Ranks time-varying covariates (labs, vitals, respiratory) by recording frequency.
- `covariate_name`, `data_type`, `SFDPH_records`, `UCSF_records`, `SFDPH_encounters`, `UCSF_encounters`, `total_records`, `total_encounters`, `sites`

## Unified Database

### HTESepsis_\<SITE\>DeIDCDW_All_YYYY-MM-DD.duckdb
Single DuckDB file per site containing all extracted tables plus `lab_keys` and `flowsheet_row_keys` config tables.

```r
library(duckdb)
con <- dbConnect(duckdb(), "HTESepsis_UCSFDeIDCDW_All_2026-02-06.duckdb")
enc_keys <- dbReadTable(con, "enc_keys")
```

```python
import duckdb
con = duckdb.connect("HTESepsis_UCSFDeIDCDW_All_2026-02-06.duckdb")
enc_keys = con.execute("SELECT * FROM enc_keys").df()
```

## Configuration

### Sepsis_R01_LabFlow_2023-11-29.xlsx
Lab component keys (sheet: `lab_keys`) and flowsheet row keys (sheet: `flowsheet_row_keys`) for UCSF. SFDPH uses hardcoded keys in `01_extract_data.py` (`SFDPH_LAB_KEYS`, `SFDPH_FLOWSHEET_KEYS`).
