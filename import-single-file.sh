#!/bin/bash

# Single File Import Script for Philippines GeoJSON Data
# Usage: ./import-single-file.sh <path-to-geojson-file>
# Imports a single GeoJSON file and determines the appropriate table based on the file path

set -e

# Check if file argument is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <path-to-geojson-file>"
    echo "Example: $0 maps/2011/geojson/regions/regions-region-1-ilocos.json"
    exit 1
fi

FILE_PATH="$1"

# Check if file exists
if [ ! -f "$FILE_PATH" ]; then
    echo "ERROR: File '$FILE_PATH' does not exist."
    exit 1
fi

DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-gis}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-password}"
export PGPASSWORD="$DB_PASSWORD"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

create_table() {
    local table_name=$1
    log "Creating table: $table_name"
    PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -U "$DB_USER" -p "$DB_PORT" "$DB_NAME" -c "\
    DROP TABLE IF EXISTS $table_name CASCADE;
    CREATE TABLE $table_name (
        id SERIAL PRIMARY KEY,
        admin_level VARCHAR(20),
        source_path TEXT,
        id_0 INTEGER,
        iso VARCHAR(10),
        name_0 VARCHAR(100),
        id_1 INTEGER,
        name_1 VARCHAR(100),
        id_2 INTEGER,
        name_2 VARCHAR(100),
        id_3 INTEGER,
        name_3 VARCHAR(100),
        id_4 INTEGER,
        name_4 VARCHAR(100),
        nl_name_1 VARCHAR(100),
        nl_name_2 VARCHAR(100),
        nl_name_3 VARCHAR(100),
        nl_name_4 VARCHAR(100),
        varname_1 VARCHAR(200),
        varname_2 VARCHAR(200),
        varname_3 VARCHAR(200),
        varname_4 VARCHAR(200),
        type_1 VARCHAR(100),
        type_2 VARCHAR(100),
        type_3 VARCHAR(100),
        type_4 VARCHAR(100),
        engtype_1 VARCHAR(100),
        engtype_2 VARCHAR(100),
        engtype_3 VARCHAR(100),
        engtype_4 VARCHAR(100),
        province VARCHAR(100),
        region VARCHAR(100),
        geom GEOMETRY(MULTIPOLYGON, 4326),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX ${table_name}_geom_idx ON $table_name USING GIST (geom);
    "
}

import_geojson() {
    local file_path=$1
    local table_name=$2
    local admin_level=$3
    log "Importing $(basename "$file_path") as $admin_level to $table_name"
    PGPASSWORD="$DB_PASSWORD" ogr2ogr \
        -f "PostgreSQL" \
        PG:"host=$DB_HOST port=$DB_PORT dbname=$DB_NAME user=$DB_USER password=$DB_PASSWORD" \
        "$file_path" \
        -nln "$table_name" \
        -append \
        -skipfailures \
        -lco GEOMETRY_NAME=geom \
        -lco PRECISION=NO \
        -a_srs "EPSG:4326" \
        -nlt PROMOTE_TO_MULTI
    # Add admin_level and source_path after import
    PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -U "$DB_USER" -p "$DB_PORT" "$DB_NAME" -c "UPDATE $table_name SET admin_level = '$admin_level', source_path = '$file_path' WHERE admin_level IS NULL;"
}

main() {
    log "Starting single file import for: $FILE_PATH"
    log "Database: $DB_NAME on $DB_HOST:$DB_PORT"

    # Ensure PGPASSWORD is set before any database command
    export PGPASSWORD="$DB_PASSWORD"

    # Check connection
    if ! PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -U "$DB_USER" -p "$DB_PORT" "$DB_NAME" -c "SELECT 1;" >/dev/null 2>&1; then
        log "ERROR: Cannot connect to database. Please check your connection settings."
        exit 1
    fi

    # Enable PostGIS
    log "Ensuring PostGIS extension is enabled"
    PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -U "$DB_USER" -p "$DB_PORT" "$DB_NAME" -c "CREATE EXTENSION IF NOT EXISTS postgis;"

    # Determine admin level and table name by file path
    case "$FILE_PATH" in
        */regions/*)
            TABLE_NAME="regions"
            ADMIN_LEVEL="region"
            ;;
        */provinces/*)
            TABLE_NAME="provinces"
            ADMIN_LEVEL="province"
            ;;
        */municties/*|*/municities/*)
            TABLE_NAME="municipalities"
            ADMIN_LEVEL="municipality"
            ;;
        */barangays/*)
            TABLE_NAME="barangays"
            ADMIN_LEVEL="barangay"
            ;;
        *)
            log "ERROR: Cannot determine admin level from file path: $FILE_PATH"
            log "Expected path to contain one of: regions, provinces, municities/municties, barangays"
            exit 1
            ;;
    esac

    # Check if table exists, create if it doesn't
    if ! PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -U "$DB_USER" -p "$DB_PORT" "$DB_NAME" -c "\dt $TABLE_NAME" >/dev/null 2>&1; then
        log "Table $TABLE_NAME does not exist. Creating it..."
        create_table "$TABLE_NAME"
    else
        log "Table $TABLE_NAME already exists. Appending data..."
    fi

    # Import the file
    import_geojson "$FILE_PATH" "$TABLE_NAME" "$ADMIN_LEVEL"

    log "=== SINGLE FILE IMPORT COMPLETED ==="
    log "File: $FILE_PATH"
    log "Table: $TABLE_NAME"
    log "Admin Level: $ADMIN_LEVEL"
}

main "$@"