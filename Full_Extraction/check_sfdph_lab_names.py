#!/usr/bin/env python3
"""Check SFDPH lab data to find WBC and other lab component keys by name"""
import duckdb

con = duckdb.connect()
con.execute("CREATE VIEW labcomponentresultfact AS SELECT * FROM read_parquet('/wynton/protected/project/ic/data/parquet/deid_cdw_sfdph/labcomponentresultfact/*.parquet')")

# Find WBC-related labs by component name
print("WBC-related labs in SFDPH:")
result = con.execute("""
SELECT DISTINCT labcomponentkey, componentname, componentloinccode, COUNT(*) as cnt
FROM labcomponentresultfact
WHERE LOWER(componentname) LIKE '%wbc%'
   OR LOWER(componentname) LIKE '%white%blood%'
   OR LOWER(componentname) LIKE '%leukocyte%'
GROUP BY labcomponentkey, componentname, componentloinccode
ORDER BY cnt DESC
LIMIT 20
""").df()
print(result)

# Find CBC-related labs
print("\n\nCBC-related labs in SFDPH:")
result = con.execute("""
SELECT DISTINCT labcomponentkey, componentname, componentloinccode, COUNT(*) as cnt
FROM labcomponentresultfact
WHERE LOWER(componentname) LIKE '%cbc%'
GROUP BY labcomponentkey, componentname, componentloinccode
ORDER BY cnt DESC
LIMIT 20
""").df()
print(result)

# Sample of top lab names
print("\n\nTop 30 lab component names by frequency:")
result = con.execute("""
SELECT labcomponentkey, componentname, componentloinccode, COUNT(*) as cnt
FROM labcomponentresultfact
GROUP BY labcomponentkey, componentname, componentloinccode
ORDER BY cnt DESC
LIMIT 30
""").df()
print(result.to_string())
