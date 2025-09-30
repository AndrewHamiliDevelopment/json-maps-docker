#!/bin/bash

# Comprehensive Administrative-Level Import Script for Philippines GeoJSON Data
# This script imports all levels: Regions, Provinces, Municipalities, and Barangays
# It creates separate tables for each administrative level with proper spatial indexes

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

# Function to create table with proper structure
create_table() {
    local table_name=$1
    local year=$2
    
    log "Creating table: ${table_name}"
    
    psql "$(get_connection_string)" -c "
    DROP TABLE IF EXISTS ${table_name};
    CREATE TABLE ${table_name} (
        id SERIAL PRIMARY KEY,
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
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Create indexes
    CREATE INDEX ${table_name}_geom_idx ON ${table_name} USING GIST (geom);
    CREATE INDEX ${table_name}_year_idx ON ${table_name} (year);
    CREATE INDEX ${table_name}_name_0_idx ON ${table_name} (name_0);
    CREATE INDEX ${table_name}_name_1_idx ON ${table_name} (name_1);
    CREATE INDEX ${table_name}_name_2_idx ON ${table_name} (name_2);
    CREATE INDEX ${table_name}_name_3_idx ON ${table_name} (name_3);
    CREATE INDEX ${table_name}_region_idx ON ${table_name} (region);
    CREATE INDEX ${table_name}_province_idx ON ${table_name} (province);
    
    -- Create composite indexes for common queries
    CREATE INDEX ${table_name}_admin_hierarchy_idx ON ${table_name} (name_0, name_1, name_2, name_3);
    CREATE INDEX ${table_name}_year_region_idx ON ${table_name} (year, region);
    CREATE INDEX ${table_name}_year_province_idx ON ${table_name} (year, province);
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
        -lco FID=id \
        -sql "SELECT *, $year AS year FROM OGRGeoJSON" \
        -a_srs "EPSG:4326" \
        -nlt PROMOTE_TO_MULTI \
        -overwrite
    
    if [[ $? -eq 0 ]]; then
        log "Successfully imported: $(basename "$file_path")"
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
    
    # Create table if it doesn't exist
    create_table "$table_name" "$year"
    
    local import_count=0
    local error_count=0
    
    # Find and import all matching files
    while IFS= read -r -d '' file; do
        if import_geojson "$file" "$table_name" "$year" "$admin_level"; then
            ((import_count++))
        else
            ((error_count++))
        fi
    done < <(find "maps/$year/geojson" -name "$pattern" -type f -print0 2>/dev/null)
    
    log "$admin_level import summary for $year: $import_count successful, $error_count errors"
    
    # Update statistics
    log "Updating table statistics for $table_name"
    psql "$(get_connection_string)" -c "ANALYZE $table_name;"
}

# Function to create summary views
create_summary_views() {
    local year=$1
    
    log "Creating summary views for year $year"
    
    psql "$(get_connection_string)" -c "
    -- Create view for administrative hierarchy summary
    CREATE OR REPLACE VIEW admin_hierarchy_summary_$year AS
    SELECT 
        r.name_1 as region_name,
        COUNT(DISTINCT p.name_2) as province_count,
        COUNT(DISTINCT m.name_2) as municipality_count,
        COUNT(DISTINCT b.name_3) as barangay_count,
        ST_Union(r.geom) as region_geom
    FROM regions_$year r
    LEFT JOIN provinces_$year p ON r.name_1 = p.region
    LEFT JOIN municipalities_$year m ON r.name_1 = m.region  
    LEFT JOIN barangays_$year b ON r.name_1 = b.region
    GROUP BY r.name_1
    ORDER BY r.name_1;
    
    -- Create view for geographic coverage
    CREATE OR REPLACE VIEW geographic_coverage_$year AS
    SELECT 
        'Regions' as admin_level,
        COUNT(*) as feature_count,
        ST_Union(geom) as total_area
    FROM regions_$year
    UNION ALL
    SELECT 
        'Provinces' as admin_level,
        COUNT(*) as feature_count,
        ST_Union(geom) as total_area
    FROM provinces_$year
    UNION ALL
    SELECT 
        'Municipalities' as admin_level,
        COUNT(*) as feature_count,
        ST_Union(geom) as total_area
    FROM municipalities_$year
    UNION ALL
    SELECT 
        'Barangays' as admin_level,
        COUNT(*) as feature_count,
        ST_Union(geom) as total_area
    FROM barangays_$year;
    "
}

# Function to generate import report
generate_report() {
    log "=== IMPORT REPORT ==="
    
    for year in 2011 2019 2023; do
        log "--- Year $year ---"
        
        # Check which tables exist and their record counts
        for table in regions_$year provinces_$year municipalities_$year barangays_$year; do
            local count=$(psql "$(get_connection_string)" -t -c "
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = '$table';
            " 2>/dev/null | tr -d ' ')
            
            if [[ "$count" == "1" ]]; then
                local records=$(psql "$(get_connection_string)" -t -c "SELECT COUNT(*) FROM $table;" 2>/dev/null | tr -d ' ')
                log "$table: $records records"
            else
                log "$table: Table not found"
            fi
        done
    done
    
    # Show database size
    local db_size=$(psql "$(get_connection_string)" -t -c "
        SELECT pg_size_pretty(pg_database_size('$DB_NAME'));
    " 2>/dev/null | tr -d ' ')
    log "Total database size: $db_size"
}

# Main execution function
main() {
    log "Starting comprehensive administrative-level import for Philippines GeoJSON data"
    log "Database: $DB_NAME on $DB_HOST:$DB_PORT"
    
    # Check if maps directory exists
    if [[ ! -d "maps" ]]; then
        log "ERROR: maps directory not found. Please run this script from the repository root."
        exit 1
    fi
    
    # Check database connection
    if ! psql "$(get_connection_string)" -c "SELECT 1;" >/dev/null 2>&1; then
        log "ERROR: Cannot connect to database. Please check your connection settings."
        exit 1
    fi
    
    # Enable PostGIS if not already enabled
    log "Ensuring PostGIS extension is enabled"
    psql "$(get_connection_string)" -c "CREATE EXTENSION IF NOT EXISTS postgis;"
    
    # Process each year and administrative level
    for year in 2011 2019 2023; do
        log "Processing year: $year"
        
        # Check if year directory exists
        if [[ ! -d "maps/$year/geojson" ]]; then
            log "WARNING: Directory maps/$year/geojson not found, skipping year $year"
            continue
        fi
        
        # Import regions
        if [[ -d "maps/$year/geojson/regions" ]]; then
            import_admin_level "$year" "Regions" "*.json" "regions_$year"
        fi
        
        # Import provinces (2011 and 2019 have provinces, 2023 has provdists)
        if [[ -d "maps/$year/geojson/provinces" ]]; then
            import_admin_level "$year" "Provinces" "*.json" "provinces_$year"
        elif [[ -d "maps/$year/geojson/provdists" ]]; then
            import_admin_level "$year" "Provincial Districts" "*.json" "provinces_$year"
        fi
        
        # Import municipalities
        if [[ -d "maps/$year/geojson/municties" ]]; then
            # Import main municipality files (not in resolution subfolders)
            import_admin_level "$year" "Municipalities" "municities-*.json" "municipalities_$year"
        elif [[ -d "maps/$year/geojson/municities" ]]; then
            import_admin_level "$year" "Municipalities" "*.json" "municipalities_$year"
        fi
        
        # Import barangays
        if [[ -d "maps/$year/geojson/barangays" ]]; then
            import_admin_level "$year" "Barangays" "barangays-*.json" "barangays_$year"
        fi
        
        # Create summary views for this year
        create_summary_views "$year"
    done
    
    # Create cross-year comparison views
    log "Creating cross-year comparison views"
    psql "$(get_connection_string)" -c "
    -- Administrative boundary changes over time
    CREATE OR REPLACE VIEW boundary_changes AS
    SELECT 
        'Regions' as admin_level,
        2011 as year,
        COUNT(*) as count
    FROM regions_2011
    UNION ALL
    SELECT 
        'Regions' as admin_level,
        2019 as year,
        COUNT(*) as count
    FROM regions_2019
    UNION ALL
    SELECT 
        'Regions' as admin_level,
        2023 as year,
        COUNT(*) as count
    FROM regions_2023
    ORDER BY admin_level, year;
    "
    
    # Generate final report
    generate_report
    
    log "=== IMPORT COMPLETED ==="
    log "Tables created with administrative hierarchy data for multiple years"
    log "You can now query the data using tables: regions_YYYY, provinces_YYYY, municipalities_YYYY, barangays_YYYY"
    log "Summary views available: admin_hierarchy_summary_YYYY, geographic_coverage_YYYY, boundary_changes"
}

# Help function
show_help() {
    cat << EOF
Comprehensive Administrative-Level Import Script for Philippines GeoJSON Data

USAGE:
    $0 [OPTIONS]

DESCRIPTION:
    This script imports Philippines administrative boundary data (regions, provinces, 
    municipalities, and barangays) from GeoJSON files into PostgreSQL/PostGIS tables.
    
    Data is organized by year (2011, 2019, 2023) and administrative level.

ENVIRONMENT VARIABLES:
    DB_HOST     PostgreSQL host (default: localhost)
    DB_PORT     PostgreSQL port (default: 5432)
    DB_NAME     Database name (default: gis)
    DB_USER     Database user (default: postgres)
    DB_PASSWORD Database password (default: password)

TABLES CREATED:
    regions_YYYY        - Regional boundaries
    provinces_YYYY      - Provincial boundaries
    municipalities_YYYY - Municipal/city boundaries  
    barangays_YYYY      - Barangay boundaries

VIEWS CREATED:
    admin_hierarchy_summary_YYYY - Administrative hierarchy summary
    geographic_coverage_YYYY     - Geographic coverage statistics
    boundary_changes             - Changes in boundary counts over time

EXAMPLES:
    # Use default settings
    $0
    
    # Use custom database settings
    DB_NAME=philippines DB_USER=gis_user $0

REQUIREMENTS:
    - PostgreSQL with PostGIS extension
    - ogr2ogr (GDAL tools)
    - psql command-line tool
    - GeoJSON files in maps/ directory structure

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