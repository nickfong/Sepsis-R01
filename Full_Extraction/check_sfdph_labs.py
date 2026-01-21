#!/usr/bin/env python3
"""Check SFDPH lab data to find correct LabComponentKey values"""
import duckdb

# Query raw SFDPH lab data
con = duckdb.connect()
con.execute("CREATE VIEW labcomponentresultfact AS SELECT * FROM read_parquet('/wynton/protected/project/ic/data/parquet/deid_cdw_sfdph/labcomponentresultfact/*.parquet')")

# Get distinct lab component keys and their counts
print("Top 30 LabComponentKey values in SFDPH:")
result = con.execute("""
SELECT LabComponentKey, COUNT(*) as cnt
FROM labcomponentresultfact
GROUP BY LabComponentKey
ORDER BY cnt DESC
LIMIT 30
""").df()
print(result)

print("\nTotal rows in labcomponentresultfact:")
total = con.execute("SELECT COUNT(*) FROM labcomponentresultfact").fetchone()[0]
print(f"{total:,}")

# Also check the column names
print("\nColumns in labcomponentresultfact:")
cols = con.execute("DESCRIBE labcomponentresultfact").df()
print(cols)
