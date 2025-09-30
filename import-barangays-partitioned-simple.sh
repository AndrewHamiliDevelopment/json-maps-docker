#!/bin/bash

# Database connection details
PG_HOST="localhost"
PG_PORT="5432"
PG_DB="gis"
PG_USER="postgres"
PG_PASS="postgres"

# Export password for ogr2ogr
export PGPASSWORD="$PG_PASS"

# Create partitioned table structure
create_partitioned_table() {
    echo "Creating partitioned barangay table structure..."
    
    psql -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DB" -U "$PG_USER" << 'EOF'
-- Drop existing table if it exists
DROP TABLE IF EXISTS barangays_partitioned CASCADE;

-- Create partitioned parent table
CREATE TABLE barangays_partitioned (
    gid SERIAL,
    id_0 INTEGER,
    iso VARCHAR(3),
    name_0 VARCHAR(100),
    id_1 INTEGER,
    name_1 VARCHAR(100),
    id_2 INTEGER,
    name_2 VARCHAR(100),
    id_3 INTEGER,
    name_3 VARCHAR(100),
    nl_name_3 VARCHAR(100),
    varname_3 VARCHAR(100),
    type_3 VARCHAR(50),
    engtype_3 VARCHAR(50),
    province VARCHAR(100),
    region VARCHAR(100),
    data_year VARCHAR(4),
    geom GEOMETRY(MULTIPOLYGON, 4326),
    PRIMARY KEY (gid, name_3)
) PARTITION BY LIST (name_3);

-- Create spatial index on parent table
CREATE INDEX idx_barangays_partitioned_geom ON barangays_partitioned USING GIST (geom);
CREATE INDEX idx_barangays_partitioned_year ON barangays_partitioned (data_year);

EOF
}

# Function to create partition for specific barangay
create_partition() {
    local BARANGAY_NAME="$1"
    local CLEAN_NAME=$(echo "$BARANGAY_NAME" | sed 's/ /_/g' | sed 's/[^a-zA-Z0-9_]//g' | tr '[:upper:]' '[:lower:]')
    
    echo "Creating partition for barangay: $BARANGAY_NAME"
    
    psql -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DB" -U "$PG_USER" << EOF
-- Create partition table for specific barangay
CREATE TABLE IF NOT EXISTS barangays_${CLEAN_NAME} 
PARTITION OF barangays_partitioned 
FOR VALUES IN ('${BARANGAY_NAME}');
EOF
}

# Extract unique barangay names from a JSON file
get_barangay_names() {
    local FILE="$1"
    if command -v jq &> /dev/null; then
        jq -r '.features[].properties.NAME_3' "$FILE" 2>/dev/null | sort | uniq | grep -v "^null$" | grep -v "^$"
    else
        echo "Warning: jq not found. Will create partitions after import."
        return 1
    fi
}

# Simplified import with direct partitioning
import_with_direct_partitioning() {
    # First create the parent table
    create_partitioned_table
    
    for YEAR in 2011 2019 2023; do
        echo "Processing barangays for year $YEAR..."
        
        find "maps/$YEAR/geojson/barangays" -name "*.json" | head -2 | while read FILE; do  # Limit to 2 files for testing
            if [ -f "$FILE" ]; then
                echo "Processing file: $FILE"
                
                # First, extract barangay names and create partitions
                echo "Creating partitions for barangays in this file..."
                get_barangay_names "$FILE" | while read BARANGAY_NAME; do
                    if [ -n "$BARANGAY_NAME" ]; then
                        create_partition "$BARANGAY_NAME"
                    fi
                done
                
                # Import each barangay separately
                get_barangay_names "$FILE" | while read BARANGAY_NAME; do
                    if [ -n "$BARANGAY_NAME" ]; then
                        echo "Importing barangay: $BARANGAY_NAME"
                        
                        # Create a unique table name for this barangay import
                        CLEAN_NAME=$(echo "$BARANGAY_NAME" | sed 's/ /_/g' | sed 's/[^a-zA-Z0-9_]//g' | tr '[:upper:]' '[:lower:]')
                        STAGING_TABLE="staging_${CLEAN_NAME}_${YEAR}"
                        
                        # Import to staging table with filter
                        ogr2ogr \
                            -f "PostgreSQL" \
                            PG:"host=$PG_HOST port=$PG_PORT dbname=$PG_DB user=$PG_USER password=$PG_PASS" \
                            "$FILE" \
                            -nln "$STAGING_TABLE" \
                            -overwrite \
                            -lco GEOMETRY_NAME=geom \
                            -lco FID=gid \
                            -nlt PROMOTE_TO_MULTI \
                            -where "NAME_3 = '$BARANGAY_NAME'" \
                            -skipfailures
                        
                        if [ $? -eq 0 ]; then
                            # Move data to partitioned table
                            psql -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DB" -U "$PG_USER" << EOSTAGING
-- Add year column and insert into partitioned table
ALTER TABLE $STAGING_TABLE ADD COLUMN IF NOT EXISTS data_year VARCHAR(4);
UPDATE $STAGING_TABLE SET data_year = '$YEAR';

INSERT INTO barangays_partitioned (
    id_0, iso, name_0, id_1, name_1, id_2, name_2, id_3, name_3,
    nl_name_3, varname_3, type_3, engtype_3, province, region, data_year, geom
)
SELECT 
    id_0, iso, name_0, id_1, name_1, id_2, name_2, id_3, name_3,
    nl_name_3, varname_3, type_3, engtype_3, province, region, data_year, geom
FROM $STAGING_TABLE 
WHERE name_3 = '$BARANGAY_NAME';

-- Clean up staging table
DROP TABLE IF EXISTS $STAGING_TABLE;
EOSTAGING
                            echo "✓ Successfully imported barangay: $BARANGAY_NAME"
                        else
                            echo "✗ Failed to import barangay: $BARANGAY_NAME"
                        fi
                    fi
                done
            fi
        done
    done
    
    # Show statistics
    echo "Showing partition statistics..."
    psql -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DB" -U "$PG_USER" << 'EOF'
-- Show partition information
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables 
WHERE tablename LIKE 'barangays_%' 
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
LIMIT 20;

-- Show total count by year
SELECT 
    data_year,
    COUNT(*) as total_barangays,
    COUNT(DISTINCT name_3) as unique_barangay_names
FROM barangays_partitioned 
GROUP BY data_year 
ORDER BY data_year;

-- Show sample barangays
SELECT name_3, data_year, COUNT(*) as count
FROM barangays_partitioned 
GROUP BY name_3, data_year
ORDER BY name_3, data_year
LIMIT 10;

EOF
}

echo "Starting simplified partitioned barangay import..."
import_with_direct_partitioning
echo "Partitioned barangay import completed!"