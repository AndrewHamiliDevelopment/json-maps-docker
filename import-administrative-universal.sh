#!/bin/bash

# Universal Administrative Import Script for Philippines GeoJSON Data
# Imports all .json files from all folders/subfolders under maps/
# Segregates by admin level (regions, provinces, municipalities, barangays) into separate tables
# Each table has an 'admin_level' column for easy joins and queries

set -e

DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-gis}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-password}"
export PGPASSWORD="$DB_PASSWORD"

get_connection_string() {
    echo "postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
}

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
        "$(get_connection_string)" \
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
    log "Starting universal administrative import for Philippines GeoJSON data"
    log "Database: $DB_NAME on $DB_HOST:$DB_PORT"

    # Create database if missing
    if ! PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -U "$DB_USER" -p "$DB_PORT" -lqt | cut -d \| -f 1 | grep -qw "$DB_NAME"; then
        log "Database $DB_NAME does not exist. Creating..."
        PGPASSWORD="$DB_PASSWORD" createdb -h "$DB_HOST" -U "$DB_USER" -p "$DB_PORT" "$DB_NAME"
        if [[ $? -ne 0 ]]; then
            log "ERROR: Failed to create database $DB_NAME. Please check your permissions."
            exit 1
        fi
    fi

    # Check connection
    if ! PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -U "$DB_USER" -p "$DB_PORT" "$DB_NAME" -c "SELECT 1;" >/dev/null 2>&1; then
        log "ERROR: Cannot connect to database. Please check your connection settings."
        exit 1
    fi

    # Enable PostGIS
    log "Ensuring PostGIS extension is enabled"
    PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -U "$DB_USER" -p "$DB_PORT" "$DB_NAME" -c "CREATE EXTENSION IF NOT EXISTS postgis;"

    # Create tables for each level
    create_table "regions"
    create_table "provinces"
    create_table "municipalities"
    create_table "barangays"

    # Import all .json files from all subfolders
    find maps -type f -name "*.json" | while read -r file; do
        # Determine admin level by path
        case "$file" in
            */regions/*)
                import_geojson "$file" "regions" "region"
                ;;
            */provinces/*)
                import_geojson "$file" "provinces" "province"
                ;;
            */municties/*|*/municities/*)
                import_geojson "$file" "municipalities" "municipality"
                ;;
            */barangays/*)
                import_geojson "$file" "barangays" "barangay"
                ;;
            *)
                log "Skipping (unknown level): $file"
                ;;
        esac
    done

    log "=== UNIVERSAL IMPORT COMPLETED ==="
    log "Tables created: regions, provinces, municipalities, barangays"
    log "You can join tables using province, region, etc. columns."
}

main "$@"
