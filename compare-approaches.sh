#!/bin/bash

# Comparison Script - Both Approaches for Philippines Administrative Data
# This script runs both year-separated and unified table approaches
# Then generates comparison reports to help you decide which works better

set -e

# Database connection settings
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-gis}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-password}"

# Get connection string for PostgreSQL
get_connection_string() {
    echo "postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
}

# Function to log with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Function to run SQL and time it
run_timed_sql() {
    local description=$1
    local sql=$2
    local output_file=$3
    
    log "Running: $description"
    local start_time=$(date +%s.%N)
    
    psql "$(get_connection_string)" -c "$sql" > "$output_file" 2>&1
    local exit_code=$?
    
    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc)
    
    if [[ $exit_code -eq 0 ]]; then
        log "✅ Completed in ${duration}s: $description"
    else
        log "❌ Failed after ${duration}s: $description"
    fi
    
    return $exit_code
}

# Function to generate comparison report
generate_comparison_report() {
    log "=== GENERATING COMPARISON REPORT ==="
    
    local report_file="comparison_report_$(date +%Y%m%d_%H%M%S).md"
    
    cat > "$report_file" << 'EOF'
# Philippines Administrative Data Import Comparison Report

## Executive Summary
This report compares two approaches for importing Philippines administrative boundary data:
1. **Year-Separated Tables**: `regions_2011`, `regions_2019`, `regions_2023`, etc.
2. **Unified Tables**: `regions`, `provinces`, etc. with year as a column

## Database Structure Comparison

### Year-Separated Approach
EOF

    # Add year-separated table info
    echo "#### Tables Created:" >> "$report_file"
    psql "$(get_connection_string)" -t -c "
        SELECT 
            schemaname, 
            tablename, 
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
        FROM pg_tables 
        WHERE tablename LIKE '%_20%' 
        ORDER BY tablename;
    " >> "$report_file" 2>/dev/null || echo "Year-separated tables not found" >> "$report_file"

    cat >> "$report_file" << 'EOF'

### Unified Approach
#### Tables Created:
EOF

    # Add unified table info
    psql "$(get_connection_string)" -t -c "
        SELECT 
            schemaname, 
            tablename, 
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
        FROM pg_tables 
        WHERE tablename IN ('regions', 'provinces', 'municipalities', 'barangays')
        AND tablename NOT LIKE '%_20%'
        ORDER BY tablename;
    " >> "$report_file" 2>/dev/null || echo "Unified tables not found" >> "$report_file"

    cat >> "$report_file" << 'EOF'

## Performance Comparison

### Query Performance Tests

#### Test 1: Count Records by Year
EOF

    # Test query performance
    log "Testing query performance..."
    
    # Year-separated approach timing
    if psql "$(get_connection_string)" -c "\d regions_2023" >/dev/null 2>&1; then
        echo "**Year-Separated Approach:**" >> "$report_file"
        run_timed_sql "Count regions by year (separated)" "
            SELECT '2011' as year, COUNT(*) as count FROM regions_2011
            UNION ALL
            SELECT '2019' as year, COUNT(*) as count FROM regions_2019  
            UNION ALL
            SELECT '2023' as year, COUNT(*) as count FROM regions_2023
            ORDER BY year;
        " "/tmp/year_separated_count.txt"
        
        if [[ $? -eq 0 ]]; then
            echo '```' >> "$report_file"
            cat "/tmp/year_separated_count.txt" >> "$report_file"
            echo '```' >> "$report_file"
        fi
    else
        echo "Year-separated tables not available for testing" >> "$report_file"
    fi

    # Unified approach timing
    if psql "$(get_connection_string)" -c "\d regions" >/dev/null 2>&1; then
        echo "**Unified Approach:**" >> "$report_file"
        run_timed_sql "Count regions by year (unified)" "
            SELECT year, COUNT(*) as count 
            FROM regions 
            GROUP BY year 
            ORDER BY year;
        " "/tmp/unified_count.txt"
        
        if [[ $? -eq 0 ]]; then
            echo '```' >> "$report_file"
            cat "/tmp/unified_count.txt" >> "$report_file"
            echo '```' >> "$report_file"
        fi
    else
        echo "Unified tables not available for testing" >> "$report_file"
    fi

    cat >> "$report_file" << 'EOF'

#### Test 2: Regional Analysis
EOF

    # Regional analysis test
    if psql "$(get_connection_string)" -c "\d regions_2023" >/dev/null 2>&1; then
        echo "**Year-Separated - Metro Manila 2023:**" >> "$report_file"
        run_timed_sql "Metro Manila analysis (separated)" "
            SELECT name_1, COUNT(*) as count, pg_size_pretty(SUM(ST_MemSize(geom))) as geom_size
            FROM regions_2023 
            WHERE name_1 ILIKE '%metro%' OR name_1 ILIKE '%manila%'
            GROUP BY name_1;
        " "/tmp/year_separated_manila.txt"
        
        if [[ $? -eq 0 ]]; then
            echo '```' >> "$report_file"
            cat "/tmp/year_separated_manila.txt" >> "$report_file"
            echo '```' >> "$report_file"
        fi
    fi

    if psql "$(get_connection_string)" -c "\d regions" >/dev/null 2>&1; then
        echo "**Unified - Metro Manila All Years:**" >> "$report_file"
        run_timed_sql "Metro Manila analysis (unified)" "
            SELECT year, name_1, COUNT(*) as count
            FROM regions 
            WHERE name_1 ILIKE '%metro%' OR name_1 ILIKE '%manila%'
            GROUP BY year, name_1
            ORDER BY year;
        " "/tmp/unified_manila.txt"
        
        if [[ $? -eq 0 ]]; then
            echo '```' >> "$report_file"
            cat "/tmp/unified_manila.txt" >> "$report_file"
            echo '```' >> "$report_file"
        fi
    fi

    cat >> "$report_file" << 'EOF'

## Data Quality Assessment

### Record Counts by Administrative Level
EOF

    # Data quality assessment
    log "Assessing data quality..."
    
    echo "#### Year-Separated Tables:" >> "$report_file"
    for year in 2011 2019 2023; do
        echo "**Year $year:**" >> "$report_file"
        psql "$(get_connection_string)" -t -c "
            SELECT 
                'Regions' as level, 
                COUNT(*) as count,
                COUNT(DISTINCT name_1) as unique_names
            FROM regions_$year
            UNION ALL
            SELECT 
                'Provinces' as level, 
                COUNT(*) as count,
                COUNT(DISTINCT name_2) as unique_names
            FROM provinces_$year
            UNION ALL
            SELECT 
                'Municipalities' as level, 
                COUNT(*) as count,
                COUNT(DISTINCT name_2) as unique_names
            FROM municipalities_$year
            UNION ALL
            SELECT 
                'Barangays' as level, 
                COUNT(*) as count,
                COUNT(DISTINCT name_3) as unique_names
            FROM barangays_$year;
        " >> "$report_file" 2>/dev/null || echo "No data for $year" >> "$report_file"
        echo "" >> "$report_file"
    done

    echo "#### Unified Tables:" >> "$report_file"
    psql "$(get_connection_string)" -t -c "
        SELECT 
            year,
            'Regions' as level, 
            COUNT(*) as count,
            COUNT(DISTINCT name_1) as unique_names
        FROM regions
        GROUP BY year
        UNION ALL
        SELECT 
            year,
            'Provinces' as level, 
            COUNT(*) as count,
            COUNT(DISTINCT name_2) as unique_names
        FROM provinces
        GROUP BY year
        ORDER BY year, level;
    " >> "$report_file" 2>/dev/null || echo "Unified tables not available" >> "$report_file"

    cat >> "$report_file" << 'EOF'

## Storage Analysis

### Database Size Comparison
EOF

    echo "#### Total Database Size:" >> "$report_file"
    psql "$(get_connection_string)" -t -c "
        SELECT pg_size_pretty(pg_database_size('$DB_NAME')) as total_size;
    " >> "$report_file"

    echo "#### Table Sizes:" >> "$report_file"
    psql "$(get_connection_string)" -t -c "
        SELECT 
            tablename,
            pg_size_pretty(pg_total_relation_size(tablename)) as size,
            pg_size_pretty(pg_relation_size(tablename)) as table_size,
            pg_size_pretty(pg_total_relation_size(tablename) - pg_relation_size(tablename)) as index_size
        FROM pg_tables 
        WHERE schemaname = 'public' 
        AND (tablename LIKE '%_20%' OR tablename IN ('regions', 'provinces', 'municipalities', 'barangays'))
        ORDER BY pg_total_relation_size(tablename) DESC;
    " >> "$report_file"

    cat >> "$report_file" << 'EOF'

## Recommendations

### Use Year-Separated Tables If:
- ✅ You primarily analyze one year at a time
- ✅ You need maximum query performance for single-year operations
- ✅ You want to avoid any risk of data contamination between years
- ✅ Different years have significantly different data structures
- ✅ You plan to archive or drop old year data regularly

### Use Unified Tables If:
- ✅ You frequently perform time-series analysis
- ✅ You need to track changes over time (boundary evolution)
- ✅ You want simpler database maintenance
- ✅ You prefer fewer tables to manage
- ✅ You need to JOIN data across years regularly

### Hybrid Approach:
Consider using both:
1. **Unified tables** for analysis and reporting
2. **Year-separated tables** for operational queries
3. **Views** that abstract the complexity

## Sample Queries

### Year-Separated Approach
```sql
-- Single year analysis (fast)
SELECT region, COUNT(*) as municipalities
FROM municipalities_2023 
GROUP BY region;

-- Cross-year comparison (complex)
SELECT 
    '2023' as year, region, COUNT(*) as count
FROM municipalities_2023 GROUP BY region
UNION ALL
SELECT 
    '2019' as year, region, COUNT(*) as count  
FROM municipalities_2019 GROUP BY region
ORDER BY region, year;
```

### Unified Approach
```sql
-- Time series analysis (easy)
SELECT year, region, COUNT(*) as municipalities
FROM municipalities 
GROUP BY year, region
ORDER BY region, year;

-- Single year analysis (requires filter)
SELECT region, COUNT(*) as municipalities
FROM municipalities 
WHERE year = 2023
GROUP BY region;
```

EOF

    log "📊 Comparison report generated: $report_file"
    echo "Report saved to: $report_file"
}

# Function to create sample analysis queries
create_sample_queries() {
    local query_file="sample_queries_$(date +%Y%m%d_%H%M%S).sql"
    
    cat > "$query_file" << 'EOF'
-- Sample Analysis Queries for Philippines Administrative Data
-- Run these to test both approaches

-- =====================================================
-- YEAR-SEPARATED TABLE QUERIES
-- =====================================================

-- 1. Count administrative units by year
SELECT '2011' as year, 
       (SELECT COUNT(*) FROM regions_2011) as regions,
       (SELECT COUNT(*) FROM provinces_2011) as provinces,
       (SELECT COUNT(*) FROM municipalities_2011) as municipalities,
       (SELECT COUNT(*) FROM barangays_2011) as barangays
UNION ALL
SELECT '2019' as year,
       (SELECT COUNT(*) FROM regions_2019) as regions,
       (SELECT COUNT(*) FROM provinces_2019) as provinces,
       (SELECT COUNT(*) FROM municipalities_2019) as municipalities,
       (SELECT COUNT(*) FROM barangays_2019) as barangays
UNION ALL
SELECT '2023' as year,
       (SELECT COUNT(*) FROM regions_2023) as regions,
       (SELECT COUNT(*) FROM provinces_2023) as provinces,
       (SELECT COUNT(*) FROM municipalities_2023) as municipalities,
       (SELECT COUNT(*) FROM barangays_2023) as barangays;

-- 2. Regional analysis for 2023
SELECT 
    name_1 as region,
    COUNT(*) as municipalities,
    pg_size_pretty(SUM(ST_MemSize(geom))) as total_geom_size
FROM municipalities_2023 
GROUP BY name_1 
ORDER BY COUNT(*) DESC;

-- 3. Province comparison between years
SELECT 
    p2023.name_2 as province,
    ST_Area(p2023.geom) as area_2023,
    ST_Area(p2019.geom) as area_2019,
    ST_Area(p2023.geom) - ST_Area(p2019.geom) as area_change
FROM provinces_2023 p2023
JOIN provinces_2019 p2019 ON p2023.name_2 = p2019.name_2
WHERE ST_Area(p2023.geom) - ST_Area(p2019.geom) != 0
ORDER BY ABS(ST_Area(p2023.geom) - ST_Area(p2019.geom)) DESC;

-- =====================================================
-- UNIFIED TABLE QUERIES  
-- =====================================================

-- 1. Count administrative units by year (unified)
SELECT 
    year,
    (SELECT COUNT(*) FROM regions r WHERE r.year = t.year) as regions,
    (SELECT COUNT(*) FROM provinces p WHERE p.year = t.year) as provinces,
    (SELECT COUNT(*) FROM municipalities m WHERE m.year = t.year) as municipalities,
    (SELECT COUNT(*) FROM barangays b WHERE b.year = t.year) as barangays
FROM (SELECT DISTINCT year FROM regions) t
ORDER BY year;

-- 2. Time series analysis
SELECT 
    year,
    name_1 as region,
    COUNT(*) as municipalities,
    AVG(ST_Area(geom)) as avg_municipality_area
FROM municipalities 
GROUP BY year, name_1
ORDER BY name_1, year;

-- 3. Administrative evolution
WITH province_evolution AS (
    SELECT 
        name_2 as province,
        year,
        ST_Area(geom) as area,
        LAG(ST_Area(geom)) OVER (PARTITION BY name_2 ORDER BY year) as prev_area
    FROM provinces
    WHERE name_2 IS NOT NULL
)
SELECT 
    province,
    year,
    area,
    CASE 
        WHEN prev_area IS NOT NULL 
        THEN ((area - prev_area) / prev_area) * 100 
        ELSE NULL 
    END as area_change_percent
FROM province_evolution
WHERE prev_area IS NOT NULL
AND ABS(((area - prev_area) / prev_area) * 100) > 1
ORDER BY ABS(area_change_percent) DESC;

-- =====================================================
-- PERFORMANCE COMPARISON QUERIES
-- =====================================================

-- Test 1: Single year query performance
\timing on

-- Year-separated (should be faster)
EXPLAIN ANALYZE 
SELECT region, COUNT(*) 
FROM municipalities_2023 
WHERE region LIKE '%Luzon%'
GROUP BY region;

-- Unified (requires year filter)
EXPLAIN ANALYZE 
SELECT region, COUNT(*) 
FROM municipalities 
WHERE year = 2023 AND region LIKE '%Luzon%'
GROUP BY region;

-- Test 2: Cross-year analysis
-- Year-separated (more complex query)
EXPLAIN ANALYZE 
SELECT 
    region,
    (SELECT COUNT(*) FROM municipalities_2023 m1 WHERE m1.region = m2019.region) as count_2023,
    (SELECT COUNT(*) FROM municipalities_2019 m2 WHERE m2.region = m2019.region) as count_2019
FROM (SELECT DISTINCT region FROM municipalities_2019) m2019;

-- Unified (simpler query)
EXPLAIN ANALYZE 
SELECT 
    region,
    SUM(CASE WHEN year = 2023 THEN 1 ELSE 0 END) as count_2023,
    SUM(CASE WHEN year = 2019 THEN 1 ELSE 0 END) as count_2019
FROM municipalities 
WHERE year IN (2019, 2023)
GROUP BY region;

-- =====================================================
-- SPATIAL ANALYSIS EXAMPLES
-- =====================================================

-- Find overlapping boundaries between years (unified tables only)
SELECT 
    r1.name_1 as region,
    r1.year as year1,
    r2.year as year2,
    ST_Area(ST_Intersection(r1.geom, r2.geom)) as overlap_area
FROM regions r1
JOIN regions r2 ON r1.name_1 = r2.name_1 
WHERE r1.year < r2.year
AND ST_Intersects(r1.geom, r2.geom)
AND ST_Area(ST_Intersection(r1.geom, r2.geom)) > 0;

-- Regional coverage analysis
SELECT 
    year,
    COUNT(*) as total_regions,
    SUM(ST_Area(geom)) as total_area,
    AVG(ST_Area(geom)) as avg_region_area,
    pg_size_pretty(SUM(ST_MemSize(geom))) as geom_memory_usage
FROM regions
GROUP BY year
ORDER BY year;

\timing off
EOF

    log "📝 Sample queries generated: $query_file"
    echo "Query file saved to: $query_file"
}

# Main function
main() {
    log "=== COMPREHENSIVE IMPORT AND COMPARISON ==="
    log "This script will run both import approaches and generate comparison reports"
    
    echo "Choose import approach:"
    echo "1. Run year-separated import only"
    echo "2. Run unified table import only"  
    echo "3. Run both approaches (recommended)"
    echo "4. Generate comparison report only (requires existing data)"
    echo "5. Generate sample queries only"
    
    read -p "Enter your choice (1-5): " choice
    
    case $choice in
        1)
            log "Running year-separated import..."
            if [[ -f "import-administrative-hierarchy.sh" ]]; then
                bash import-administrative-hierarchy.sh
            else
                log "ERROR: import-administrative-hierarchy.sh not found"
                exit 1
            fi
            ;;
        2)
            log "Running unified table import..."
            if [[ -f "import-administrative-single-table.sh" ]]; then
                bash import-administrative-single-table.sh
            else
                log "ERROR: import-administrative-single-table.sh not found"
                exit 1
            fi
            ;;
        3)
            log "Running both approaches..."
            log "Step 1: Year-separated import"
            if [[ -f "import-administrative-hierarchy.sh" ]]; then
                bash import-administrative-hierarchy.sh
            else
                log "ERROR: import-administrative-hierarchy.sh not found"
                exit 1
            fi
            
            log "Step 2: Unified table import"
            if [[ -f "import-administrative-single-table.sh" ]]; then
                bash import-administrative-single-table.sh
            else
                log "ERROR: import-administrative-single-table.sh not found"
                exit 1
            fi
            
            log "Step 3: Generating comparison report"
            generate_comparison_report
            create_sample_queries
            ;;
        4)
            generate_comparison_report
            create_sample_queries
            ;;
        5)
            create_sample_queries
            ;;
        *)
            log "Invalid choice. Exiting."
            exit 1
            ;;
    esac
    
    log "=== COMPLETED ==="
    log "Check the generated reports and run the sample queries to compare approaches!"
}

# Help function
show_help() {
    cat << EOF
Comprehensive Import and Comparison Script

USAGE:
    $0 [--help]

DESCRIPTION:
    This script helps you test both approaches for importing Philippines administrative data:
    1. Year-separated tables (regions_2011, regions_2019, etc.)
    2. Unified tables with partitioning (regions with year column)
    
    It generates detailed comparison reports including:
    - Performance benchmarks
    - Storage analysis
    - Data quality assessment
    - Sample queries for both approaches

OPTIONS:
    --help    Show this help message

GENERATED FILES:
    comparison_report_YYYYMMDD_HHMMSS.md  - Detailed comparison report
    sample_queries_YYYYMMDD_HHMMSS.sql   - Test queries for both approaches

REQUIREMENTS:
    - Both import scripts must be present
    - PostgreSQL with PostGIS
    - bc calculator for timing

EOF
}

# Parse command line arguments
case "${1:-}" in
    -h|--help)
        show_help
        exit 0
        ;;
    *)
        main "$@"
        ;;
esac