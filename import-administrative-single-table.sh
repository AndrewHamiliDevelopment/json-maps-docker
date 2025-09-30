#!/bin/bash

# Single-Table Administrative Import Script for Philippines GeoJSON Data
# This version creates one table per administrative level with year as a column

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

# Function to create single table for all years
create_unified_table() {
    local table_name=$1
    
    log "Creating unified table: ${table_name}"
    
    psql "$(get_connection_string)" -c "
    DROP TABLE IF EXISTS ${table_name} CASCADE;
    CREATE TABLE ${table_name} (
        id SERIAL,
        year INTEGER NOT NULL,
        -- Administrative identifiers
        id_0 INTEGER,
        iso VARCHAR(10),
        name_0 VARCHAR(100),
        id_1 INTEGER,
        name_1 VARCHAR(100),
        id_2 INTEGER,
        name_2 VARCHAR(100),
        id_3 INTEGER,
        name_3 VARCHAR(100),
        -- Additional name fields
        nl_name_1 VARCHAR(100),
        nl_name_2 VARCHAR(100),
        nl_name_3 VARCHAR(100),
        varname_1 VARCHAR(200),
        varname_2 VARCHAR(200),
        varname_3 VARCHAR(200),
        -- Type information
        type_1 VARCHAR(100),
        type_2 VARCHAR(100),
        type_3 VARCHAR(100),
        engtype_1 VARCHAR(100),
        engtype_2 VARCHAR(100),
        engtype_3 VARCHAR(100),
        -- Additional Philippines-specific fields
        province VARCHAR(100),
        region VARCHAR(100),
        -- Geometry
        geom GEOMETRY(MULTIPOLYGON, 4326),
        -- Metadata
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id, year)
    ) PARTITION BY LIST (year);
    
    -- Create partitions for each year
    CREATE TABLE ${table_name}_2011 PARTITION OF ${table_name} FOR VALUES IN (2011);
    CREATE TABLE ${table_name}_2019 PARTITION OF ${table_name} FOR VALUES IN (2019);
    CREATE TABLE ${table_name}_2023 PARTITION OF ${table_name} FOR VALUES IN (2023);
    
    -- Create indexes on parent table
    CREATE INDEX ${table_name}_geom_idx ON ${table_name} USING GIST (geom);
    CREATE INDEX ${table_name}_year_idx ON ${table_name} (year);
    CREATE INDEX ${table_name}_name_1_idx ON ${table_name} (name_1);
    CREATE INDEX ${table_name}_name_2_idx ON ${table_name} (name_2);
    CREATE INDEX ${table_name}_name_3_idx ON ${table_name} (name_3);
    CREATE INDEX ${table_name}_region_idx ON ${table_name} (region);
    CREATE INDEX ${table_name}_province_idx ON ${table_name} (province);
    
    -- Composite indexes for common queries
    CREATE INDEX ${table_name}_year_region_idx ON ${table_name} (year, region);
    CREATE INDEX ${table_name}_year_name_idx ON ${table_name} (year, name_1, name_2, name_3);
    "
}

# Function to import GeoJSON files using ogr2ogr
import_geojson() {
    local file_path=$1
    local table_name=$2
    local year=$3
    local admin_level=$4
    
    if [[ ! -f "$file_path" ]]; then
        log "WARNING: File not found: $file_path"
        return 1
    fi
    
    log "Importing $admin_level data: $(basename "$file_path") to $table_name"
    
    # Import with ogr2ogr
    ogr2ogr \
        -f "PostgreSQL" \
        "$(get_connection_string)" \
        "$file_path" \
        -nln "$table_name" \
        -append \
        -skipfailures \
        -lco GEOMETRY_NAME=geom \
        -lco PRECISION=NO \
        -a_srs "EPSG:4326" \
        -nlt PROMOTE_TO_MULTI
    
    if [[ $? -eq 0 ]]; then
        log "Successfully imported: $(basename "$file_path")"
        # Update year field after import
        psql "$(get_connection_string)" -c "UPDATE $table_name SET year = $year WHERE year IS NULL;" >/dev/null 2>&1
        return 0
    else
        log "ERROR: Failed to import: $(basename "$file_path")"
        return 1
    fi
}

# Function to import all files for a specific year and admin level
import_admin_level() {
    local year=$1
    local admin_level=$2
    local pattern=$3
    local table_name=$4
    
    log "=== Importing $admin_level for year $year ==="
    
    # Determine the correct subdirectory based on admin level
    local subdir=""
    case $admin_level in
        "Regions")
            subdir="regions"
            ;;
        "Provinces")
            subdir="provinces"
            ;;
        "Provincial Districts")
            subdir="provdists"
            ;;
        "Municipalities")
            subdir="municties"
            if [[ ! -d "maps/$year/geojson/$subdir" ]]; then
                subdir="municities"
            fi
            ;;
        "Barangays")
            subdir="barangays"
            ;;
    esac
    
    if [[ ! -d "maps/$year/geojson/$subdir" ]]; then
        log "Directory not found: maps/$year/geojson/$subdir - skipping $admin_level"
        return 0
    fi
    
    local import_count=0
    local error_count=0
    local files_found=0
    
    # For 2023, check if files are in resolution subdirectories
    local search_path="maps/$year/geojson/$subdir"
    local max_depth=1
    
    # For 2011/2019 Provinces and Municipalities: use medres/hires if available
    if [[ ("$year" == "2011" || "$year" == "2019") && ("$admin_level" == "Provinces" || "$admin_level" == "Municipalities") ]]; then
        if [[ -d "$search_path/medres" ]]; then
            search_path="$search_path/medres"
        elif [[ -d "$search_path/hires" ]]; then
            search_path="$search_path/hires"
        fi
    fi
    
    # For 2023, most files are in hires/lowres/medres subdirectories
    if [[ "$year" == "2023" && "$admin_level" != "Municipalities" ]]; then
        if [[ -d "$search_path/medres" ]]; then
            search_path="$search_path/medres"
        elif [[ -d "$search_path/hires" ]]; then
            search_path="$search_path/hires"
        fi
    fi
    
    # Only search for '*.json' files
    local found_files=()
    while IFS= read -r -d '' file; do
        found_files+=("$file")
    done < <(find "$search_path" -maxdepth $max_depth -name "*.json" -type f -print0 2>/dev/null)
    files_found=${#found_files[@]}

    if [[ $files_found -eq 0 ]]; then
        log "ERROR: No files found for $admin_level ($year) in $search_path with pattern: *.json"
        echo "$year,$admin_level,0,0,0" >> import_summary.log
        return 0
    fi

    log "Found $files_found files to import for $admin_level ($year):"
    for f in "${found_files[@]}"; do
        echo "  $f"
    done

    # Import all matching files
    for file in "${found_files[@]}"; do
        if import_geojson "$file" "$table_name" "$year" "$admin_level"; then
            ((import_count++))
        else
            ((error_count++))
        fi
    done

    log "$admin_level import summary for $year: $import_count successful, $error_count errors (out of $files_found files)"
    echo "$year,$admin_level,$files_found,$import_count,$error_count" >> import_summary.log

    # Update statistics
    if [[ $import_count -gt 0 ]]; then
        log "Updating table statistics for $table_name"
        psql "$(get_connection_string)" -c "ANALYZE $table_name;" >/dev/null 2>&1
    fi
}

# Function to create comprehensive views
create_unified_views() {
    log "Creating unified analysis views"
    
    psql "$(get_connection_string)" -c "
    -- Time-series analysis view
    CREATE OR REPLACE VIEW administrative_timeline AS
    SELECT 
        year,
        'Regions' as admin_level,
        COUNT(*) as feature_count,
        ST_Union(geom) as total_geom
    FROM regions
    GROUP BY year
    UNION ALL
    SELECT 
        year,
        'Provinces' as admin_level,
        COUNT(*) as feature_count,
        ST_Union(geom) as total_geom
    FROM provinces
    GROUP BY year
    UNION ALL
    SELECT 
        year,
        'Municipalities' as admin_level,
        COUNT(*) as feature_count,
        ST_Union(geom) as total_geom
    FROM municipalities
    GROUP BY year
    UNION ALL
    SELECT 
        year,
        'Barangays' as admin_level,
        COUNT(*) as feature_count,
        ST_Union(geom) as total_geom
    FROM barangays
    GROUP BY year
    ORDER BY admin_level, year;
    
    -- Administrative changes over time
    CREATE OR REPLACE VIEW boundary_evolution AS
    WITH region_changes AS (
        SELECT 
            name_1,
            year,
            ST_Area(geom) as area,
            LAG(ST_Area(geom)) OVER (PARTITION BY name_1 ORDER BY year) as prev_area
        FROM regions
        WHERE name_1 IS NOT NULL
    )
    SELECT 
        name_1 as region_name,
        year,
        area,
        prev_area,
        CASE 
            WHEN prev_area IS NOT NULL 
            THEN ((area - prev_area) / prev_area) * 100 
            ELSE NULL 
        END as area_change_percent
    FROM region_changes
    ORDER BY name_1, year;
    
    -- Current vs historical comparison
    CREATE OR REPLACE VIEW admin_comparison AS
    SELECT 
        r2023.name_1 as region_name,
        COUNT(DISTINCT r2023.id) as regions_2023,
        COUNT(DISTINCT r2019.id) as regions_2019,
        COUNT(DISTINCT r2011.id) as regions_2011,
        COUNT(DISTINCT p2023.id) as provinces_2023,
        COUNT(DISTINCT p2019.id) as provinces_2019,
        COUNT(DISTINCT p2011.id) as provinces_2011
    FROM regions r2023
    LEFT JOIN regions r2019 ON r2023.name_1 = r2019.name_1 AND r2019.year = 2019
    LEFT JOIN regions r2011 ON r2023.name_1 = r2011.name_1 AND r2011.year = 2011
    LEFT JOIN provinces p2023 ON r2023.name_1 = p2023.region AND p2023.year = 2023
    LEFT JOIN provinces p2019 ON r2023.name_1 = p2019.region AND p2019.year = 2019
    LEFT JOIN provinces p2011 ON r2023.name_1 = p2011.region AND p2011.year = 2011
    WHERE r2023.year = 2023
    GROUP BY r2023.name_1
    ORDER BY r2023.name_1;
    "
}

# Main execution function
main() {
    log "Starting unified administrative import for Philippines GeoJSON data"
    log "Database: $DB_NAME on $DB_HOST:$DB_PORT"
    
    # Check if maps directory exists
    if [[ ! -d "maps" ]]; then
        log "ERROR: maps directory not found. Please run this script from the repository root."
        exit 1
    fi
    
    # Check if database exists, create if missing
    if ! psql -h "$DB_HOST" -U "$DB_USER" -p "$DB_PORT" -lqt | cut -d \| -f 1 | grep -qw "$DB_NAME"; then
        log "Database $DB_NAME does not exist. Creating..."
        createdb -h "$DB_HOST" -U "$DB_USER" -p "$DB_PORT" "$DB_NAME"
        if [[ $? -ne 0 ]]; then
            log "ERROR: Failed to create database $DB_NAME. Please check your permissions."
            exit 1
        fi
    fi

    # Check database connection
    if ! psql "$(get_connection_string)" -c "SELECT 1;" >/dev/null 2>&1; then
        log "ERROR: Cannot connect to database. Please check your connection settings."
        exit 1
    fi
    
    # Enable PostGIS if not already enabled
    log "Ensuring PostGIS extension is enabled"
    psql "$(get_connection_string)" -c "CREATE EXTENSION IF NOT EXISTS postgis;"
    
    # Create unified tables
    create_unified_table "regions"
    create_unified_table "provinces"
    create_unified_table "municipalities"
    create_unified_table "barangays"
    
    # Remove previous summary log
    rm -f import_summary.log

    # Process each year and administrative level
    for year in 2011 2019 2023; do
        log "Processing year: $year"

        # Check if year directory exists
        if [[ ! -d "maps/$year/geojson" ]]; then
            log "WARNING: Directory maps/$year/geojson not found, skipping year $year"
            continue
        fi

        # Import all levels into unified tables
        if [[ "$year" == "2023" ]]; then
            # 2023 has different file patterns and structure
            import_admin_level "$year" "Regions" "provdists-region-*.json" "regions"
            import_admin_level "$year" "Provincial Districts" "municities-provdist-*.json" "provinces"
            import_admin_level "$year" "Municipalities" "bgysubmuns-municity-*.json" "municipalities"
        else
            # 2011 and 2019 use consistent patterns, but regions.json is inside medres/
            import_admin_level "$year" "Regions" "*.json" "regions" # pattern is ignored, now uses all common JSON extensions
            import_admin_level "$year" "Provinces" "provinces-region-*.json" "provinces"
            import_admin_level "$year" "Municipalities" "municities-province-*.json" "municipalities"
            import_admin_level "$year" "Barangays" "barangays-municity-*.json" "barangays"
        fi
    done

    # Print import summary
    log "=== IMPORT SUMMARY ==="
    if [[ -f import_summary.log ]]; then
        echo "Year,AdminLevel,FilesFound,Imported,Errors"
        cat import_summary.log
    else
        echo "No import summary log found."
    fi

    # Create unified analysis views
    create_unified_views

    log "=== UNIFIED IMPORT COMPLETED ==="
    log "Tables created: regions, provinces, municipalities, barangays (partitioned by year)"
    log "Analysis views: administrative_timeline, boundary_evolution, admin_comparison"
}

# Help function
show_help() {
    cat << EOF
Unified Administrative Import Script for Philippines GeoJSON Data

DESCRIPTION:
    This script creates unified tables (one per admin level) with year as a column.
    Uses PostgreSQL partitioning for performance while enabling time-series analysis.

TABLES CREATED:
    regions        - All regional boundaries (partitioned by year)
    provinces      - All provincial boundaries (partitioned by year)
    municipalities - All municipal boundaries (partitioned by year)
    barangays      - All barangay boundaries (partitioned by year)

VIEWS CREATED:
    administrative_timeline - Feature counts by year and admin level
    boundary_evolution     - Area changes over time by region
    admin_comparison       - Cross-year administrative comparisons

BENEFITS:
    ✅ Time-series analysis in single queries
    ✅ PostgreSQL partitioning for performance
    ✅ Easier cross-year comparisons
    ✅ Simplified table structure

TRADE-OFFS:
    ❌ Larger table sizes
    ❌ More complex queries for single-year analysis
    ❌ Potential data conflicts between years

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