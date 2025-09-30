#!/bin/bash

# Simple Administrative Import Script for Philippines GeoJSON Data
# Imports all administrative levels 1 to 4 into separate tables, no year partitioning

set -e

# Database connection settings
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
    local geom_type=${2:-MULTIPOLYGON}
    log "Creating table: $table_name"
    psql -h "$DB_HOST" -U "$DB_USER" -p "$DB_PORT" "$DB_NAME" -c "\
    DROP TABLE IF EXISTS $table_name CASCADE;
    CREATE TABLE $table_name (
        id SERIAL PRIMARY KEY,
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
        geom GEOMETRY($geom_type, 4326),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX ${table_name}_geom_idx ON $table_name USING GIST (geom);
    "
}

import_geojson() {
    local file_path=$1
    local table_name=$2
    if [[ ! -f "$file_path" ]]; then
        log "WARNING: File not found: $file_path"
        return 1
    fi
    log "Importing $(basename "$file_path") to $table_name"
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
        return 0
    else
        log "ERROR: Failed to import: $(basename "$file_path")"
        return 1
    fi
}

main() {
    log "Starting simple administrative import for Philippines GeoJSON data"
    log "Database: $DB_NAME on $DB_HOST:$DB_PORT"

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
    if ! psql -h "$DB_HOST" -U "$DB_USER" -p "$DB_PORT" "$DB_NAME" -c "SELECT 1;" >/dev/null 2>&1; then
        log "ERROR: Cannot connect to database. Please check your connection settings."
        exit 1
    fi

    # Enable PostGIS if not already enabled
    log "Ensuring PostGIS extension is enabled"
    psql -h "$DB_HOST" -U "$DB_USER" -p "$DB_PORT" "$DB_NAME" -c "CREATE EXTENSION IF NOT EXISTS postgis;"

    # Create tables for each level
    create_table "regions"
    create_table "provinces"
    create_table "municipalities"
    create_table "barangays"

    # Import files for each level (search all years)
    # Regions (level 1)
    for file in $(find maps/*/geojson/regions -type f -name "*.json"); do
        import_geojson "$file" "regions"
    done
    # Provinces (level 2)
    for file in $(find maps/*/geojson/provinces -type f -name "*.json"); do
        import_geojson "$file" "provinces"
    done
    # Municipalities (level 3)
    for file in $(find maps/*/geojson/municties -type f -name "*.json"); do
        import_geojson "$file" "municipalities"
    done
    for file in $(find maps/*/geojson/municities -type f -name "*.json"); do
        import_geojson "$file" "municipalities"
    done
    # Barangays (level 4)
    for file in $(find maps/*/geojson/barangays -type f -name "*.json"); do
        import_geojson "$file" "barangays"
    done

    log "=== SIMPLE IMPORT COMPLETED ==="
    log "Tables created: regions, provinces, municipalities, barangays"
}

main "$@"
