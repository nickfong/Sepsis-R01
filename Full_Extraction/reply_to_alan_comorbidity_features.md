**Subject: Comorbidity features ready — Elixhauser + CMS-HCC + CCSR, with POA variants**

Hi all,

I've built per-encounter comorbidity features for our 244,135-encounter cohort using four validated systems:

**1. AHRQ Elixhauser Refined (38 categories)**
- Source: HCUP CMR-Reference-File v2026.1 (primary Excel from AHRQ)
- ICD-10-CM exact-match mapping, parsed programmatically from the official file

**2. Quan Elixhauser (31 categories)**
- Source: Quan et al., Med Care 2005;43(11):1130-9
- Prefix-based ICD-10 mapping, verified identical to the R `comorbidity` package

**3. CMS-HCC V28 (115 hierarchical condition categories)**
- Source: CMS 2026 midyear final model files (crosswalk, hierarchy, coefficients)
- Built entirely from official CMS SAS files — no third-party packages
- Includes hierarchy enforcement, interaction terms, and RAF scores

**4. AHRQ CCSR (553 clinical categories across 22 body systems)**
- Source: AHRQ CCSR for ICD-10-CM Diagnoses v2026.1 (primary CSV from HCUP)
- Maps ALL ~75,000 ICD-10-CM codes into 553 clinically meaningful categories
- A single ICD code can map to up to 6 categories (e.g., a diabetes code maps to both END003 "Diabetes with complication" and EYE005 "Retinal conditions")
- Unlike Elixhauser/HCC which only capture selected comorbidities, CCSR classifies every diagnosis — giving a complete picture of the cohort's disease burden

**Two variants per system: All-Dx vs POA-Filtered**

Each system produces two sets of features:
- **All-Dx**: every ICD code counts, regardless of whether it was present on admission
- **POA-Filtered**: only counts diagnoses that were present on admission (POA='Yes', or diagnosis type is Medical History / Problem List). For Elixhauser, conditions classified as "POA-exempt" (chronic by nature, e.g., diabetes, COPD, HIV) always count in both variants. For HCC and CCSR, strict POA filtering is applied to all categories.

The POA variant isolates pre-existing comorbidity burden; the all-dx variant includes hospital-acquired conditions. The difference matters most for acute conditions — see the attached POA impact figure.

**Summary numbers:**

| System | Variant | Mean categories | % with >=1 |
|--------|---------|----------------|------------|
| AHRQ Elixhauser | All-Dx | 3.4 | 79.2% |
| AHRQ Elixhauser | POA | 2.9 | 74.4% |
| Quan Elixhauser | All-Dx | 3.3 | 79.4% |
| Quan Elixhauser | POA | 2.9 | 74.8% |
| CMS-HCC V28 | All-Dx | 3.3 | 80.0% |
| CMS-HCC V28 | POA | 1.7 | 39.6% |
| AHRQ CCSR | All-Dx | 24.3 | 99.9% |
| AHRQ CCSR | POA | 10.6 | 45.0% |

CCSR has much higher counts because it classifies every diagnosis code (not just comorbidities) into one or more of 553 categories. The POA impact is dramatic: 99.9% -> 45.0% with >=1 category, and mean count drops from 24.3 to 10.6. This reflects the large number of hospital-acquired diagnoses (acute injuries, procedure complications, inpatient findings) that are filtered out when restricting to pre-existing conditions.

**Output files:**
- `elixhauser_features.parquet` — 244,135 x 147 columns (AHRQ + Quan, all-dx + POA, VW scores)
- `hcc_features.parquet` — 244,135 x 234 columns (all-dx + POA, RAF scores)
- `ccsr_features.parquet` — 244,135 x 1,109 columns (all-dx + POA, category + body system counts)

All are keyed on `encounterkey` and ready to merge with the filtered cohort. 98.2% of ICD codes in our cohort mapped to CCSR categories (the remaining 1.8% are likely ICD-10-PCS procedure codes or deprecated codes).

Figures attached.

Best,
Nick
