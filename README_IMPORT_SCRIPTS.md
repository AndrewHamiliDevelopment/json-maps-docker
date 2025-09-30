# Philippines Administrative Data Import Scripts

This repository contains multiple approaches for importing Philippines administrative boundary data into PostgreSQL/PostGIS.

## 📁 Available Scripts

### 1. Year-Separated Approach
**File:** `import-administrative-hierarchy.sh`
- Creates separate tables for each year and administrative level
- Tables: `regions_2011`, `provinces_2019`, `municipalities_2023`, etc.
- **Best for:** Single-year analysis, maximum performance, data separation

```bash
./import-administrative-hierarchy.sh
```

### 2. Unified Table Approach  
**File:** `import-administrative-single-table.sh`
- Creates one table per administrative level with year as column
- Tables: `regions`, `provinces`, `municipalities`, `barangays` (partitioned by year)
- **Best for:** Time-series analysis, trend tracking, cross-year comparisons

```bash
./import-administrative-single-table.sh
```

### 3. Comprehensive Comparison
**File:** `compare-approaches.sh`
- Interactive script to run both approaches
- Generates detailed comparison reports
- Creates sample queries for testing
- **Best for:** Deciding which approach to use

```bash
./compare-approaches.sh
```

## 🗂️ Data Structure

### Administrative Hierarchy
```
Philippines (NAME_0)
├── Regions (NAME_1)
│   ├── Provinces (NAME_2)
│   │   ├── Municipalities (NAME_2)
│   │   │   └── Barangays (NAME_3)
```

### Available Years
- **2011**: Complete hierarchy (regions, provinces, municipalities, barangays)
- **2019**: Complete hierarchy (regions, provinces, municipalities, barangays)  
- **2023**: Modified structure (regions, provdists, municipalities)

## 🔧 Environment Variables

```bash
DB_HOST=localhost      # PostgreSQL host
DB_PORT=5432          # PostgreSQL port  
DB_NAME=gis           # Database name
DB_USER=postgres      # Database user
DB_PASSWORD=password  # Database password
```

## 📊 Comparison Summary

| Aspect | Year-Separated | Unified Tables |
|--------|----------------|----------------|
| **Performance** | ⭐⭐⭐⭐⭐ Single-year | ⭐⭐⭐⭐⭐ Cross-year |
| **Storage** | ⭐⭐⭐ More tables | ⭐⭐⭐⭐ Fewer tables |
| **Complexity** | ⭐⭐⭐ Complex joins | ⭐⭐⭐⭐⭐ Simple queries |
| **Maintenance** | ⭐⭐ Many tables | ⭐⭐⭐⭐⭐ Few tables |
| **Time Analysis** | ⭐⭐ Complex unions | ⭐⭐⭐⭐⭐ Native support |
| **Data Safety** | ⭐⭐⭐⭐⭐ Isolated | ⭐⭐⭐ Mixed data |

## 🎯 Quick Start

1. **Test both approaches:**
   ```bash
   ./compare-approaches.sh
   # Choose option 3 (Run both approaches)
   ```

2. **Analyze results:**
   - Check generated `comparison_report_*.md`
   - Run queries from `sample_queries_*.sql`
   - Compare performance and usability

3. **Choose your approach:**
   - **Year-separated** for operational/production use
   - **Unified** for analytics/research use
   - **Both** for hybrid approach

## 📈 Sample Queries

### Year-Separated Tables
```sql
-- Count municipalities by region for 2023
SELECT region, COUNT(*) as municipalities
FROM municipalities_2023 
GROUP BY region 
ORDER BY COUNT(*) DESC;

-- Compare regions between years
SELECT 
    r23.name_1 as region,
    COUNT(r23.id) as regions_2023,
    COUNT(r19.id) as regions_2019
FROM regions_2023 r23
FULL OUTER JOIN regions_2019 r19 ON r23.name_1 = r19.name_1
GROUP BY r23.name_1;
```

### Unified Tables
```sql
-- Time series of administrative units
SELECT 
    year,
    COUNT(*) as total_municipalities,
    COUNT(DISTINCT region) as regions
FROM municipalities 
GROUP BY year 
ORDER BY year;

-- Regional growth over time
SELECT 
    region,
    year,
    COUNT(*) as municipalities,
    COUNT(*) - LAG(COUNT(*)) OVER (PARTITION BY region ORDER BY year) as growth
FROM municipalities 
GROUP BY region, year
ORDER BY region, year;
```

## 🛠️ Troubleshooting

### Common Issues
1. **ogr2ogr errors**: Check PostgreSQL connection and PostGIS extension
2. **File not found**: Ensure you're in the repository root directory
3. **Permission denied**: Run `chmod +x *.sh` to make scripts executable
4. **Connection refused**: Start PostgreSQL container with `docker-compose up -d`

### Debug Mode
Add `set -x` to any script for verbose debugging output.

## 📚 Additional Resources
- [PostGIS Documentation](https://postgis.net/docs/)
- [GDAL/OGR Documentation](https://gdal.org/programs/ogr2ogr.html)
- [PostgreSQL Partitioning](https://www.postgresql.org/docs/current/ddl-partitioning.html)

---
**Last Updated:** September 30, 2025