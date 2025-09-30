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

# Import with automatic partition creation
import_with_partitioning() {
    # First create the parent table
    create_partitioned_table
    
    # Temporary table for initial import
    local TEMP_TABLE="temp_barangay_import"
    
    for YEAR in 2011 2019 2023; do
        echo "Processing barangays for year $YEAR..."
        
        find "maps/$YEAR/geojson/barangays" -name "*.json" | while read FILE; do
            if [ -f "$FILE" ]; then
                echo "Importing $FILE into temporary table"
                
                # Import to temporary table first
                ogr2ogr \
                    -f "PostgreSQL" \
                    PG:"host=$PG_HOST port=$PG_PORT dbname=$PG_DB user=$PG_USER password=$PG_PASS" \
                    "$FILE" \
                    -nln "$TEMP_TABLE" \
                    -overwrite \
                    -lco GEOMETRY_NAME=geom \
                    -lco FID=gid \
                    -nlt PROMOTE_TO_MULTI \
                    -sql "SELECT *, '$YEAR' as data_year FROM OGRGeoJSON" \
                    -progress
                
                # Get unique barangay names and create partitions
                psql -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DB" -U "$PG_USER" -t -c "SELECT DISTINCT name_3 FROM $TEMP_TABLE WHERE name_3 IS NOT NULL;" | while read BARANGAY_NAME; do
                    if [ -n "$BARANGAY_NAME" ]; then
                        # Remove leading/trailing whitespace
                        BARANGAY_NAME=$(echo "$BARANGAY_NAME" | xargs)
                        create_partition "$BARANGAY_NAME"
                    fi
                done
                
                # Move data from temp table to partitioned table
                echo "Moving data to partitioned table..."
                psql -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DB" -U "$PG_USER" << EOF
INSERT INTO barangays_partitioned 
SELECT * FROM $TEMP_TABLE 
WHERE name_3 IS NOT NULL;

DROP TABLE $TEMP_TABLE;
EOF
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
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Show total count by year
SELECT 
    data_year,
    COUNT(*) as total_barangays,
    COUNT(DISTINCT name_3) as unique_barangay_names
FROM barangays_partitioned 
GROUP BY data_year 
ORDER BY data_year;

EOF
}

echo "Starting partitioned barangay import..."
import_with_partitioning
echo "Partitioned barangay import completed!"