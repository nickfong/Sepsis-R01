#!/usr/bin/env python3
"""Check SFDPH flowsheet data to find correct FlowsheetRowKey values"""
import duckdb

con = duckdb.connect()
con.execute("CREATE VIEW flowsheetvaluefact AS SELECT * FROM read_parquet('/wynton/protected/project/ic/data/parquet/deid_cdw_sfdph/flowsheetvaluefact/*.parquet')")

# Get columns
print("Columns in flowsheetvaluefact:")
cols = con.execute("DESCRIBE flowsheetvaluefact").df()
print(cols['column_name'].tolist())

# Total rows
print("\nTotal rows in flowsheetvaluefact:")
total = con.execute("SELECT COUNT(*) FROM flowsheetvaluefact").fetchone()[0]
print(f"{total:,}")

# Find temperature-related flowsheets
print("\n\nTemperature-related flowsheets:")
result = con.execute("""
SELECT DISTINCT flowsheetrowkey, flowsheetrowdisplayname, COUNT(*) as cnt
FROM flowsheetvaluefact
WHERE LOWER(flowsheetrowdisplayname) LIKE '%temp%'
GROUP BY flowsheetrowkey, flowsheetrowdisplayname
ORDER BY cnt DESC
LIMIT 20
""").df()
print(result.to_string())

# Find BP-related flowsheets
print("\n\nBlood Pressure-related flowsheets:")
result = con.execute("""
SELECT DISTINCT flowsheetrowkey, flowsheetrowdisplayname, COUNT(*) as cnt
FROM flowsheetvaluefact
WHERE LOWER(flowsheetrowdisplayname) LIKE '%blood%press%'
   OR LOWER(flowsheetrowdisplayname) LIKE '%bp%'
   OR LOWER(flowsheetrowdisplayname) LIKE '%systolic%'
   OR LOWER(flowsheetrowdisplayname) LIKE '%diastolic%'
GROUP BY flowsheetrowkey, flowsheetrowdisplayname
ORDER BY cnt DESC
LIMIT 20
""").df()
print(result.to_string())

# Top flowsheet rows by count
print("\n\nTop 30 flowsheet rows by frequency:")
result = con.execute("""
SELECT flowsheetrowkey, flowsheetrowdisplayname, COUNT(*) as cnt
FROM flowsheetvaluefact
GROUP BY flowsheetrowkey, flowsheetrowdisplayname
ORDER BY cnt DESC
LIMIT 30
""").df()
print(result.to_string())
