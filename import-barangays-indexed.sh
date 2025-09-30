#!/bin/bash

# Database connection details
PG_HOST="localhost"
PG_PORT="5432"
PG_DB="gis"
PG_USER="postgres"
PG_PASS="postgres"

# Export password for ogr2ogr
export PGPASSWORD="$PG_PASS"

# Import all barangay data into a single table with NAME_3 indexing
import_all_barangays() {
    local TABLE_NAME="all_barangays"
    
    echo "Creating indexed barangay table: $TABLE_NAME"
    
    # Loop through each year
    for YEAR in 2011 2019 2023; do
        echo "Processing barangays for year $YEAR..."
        
        # Add year column to track data source
        find "maps/$YEAR/geojson/barangays" -name "*.json" | while read FILE; do
            if [ -f "$FILE" ]; then
                echo "Importing $FILE into table $TABLE_NAME"
                
                # Add year as additional field and import
                ogr2ogr \
                    -f "PostgreSQL" \
                    PG:"host=$PG_HOST port=$PG_PORT dbname=$PG_DB user=$PG_USER password=$PG_PASS" \
                    "$FILE" \
                    -nln "$TABLE_NAME" \
                    -append \
                    -lco GEOMETRY_NAME=geom \
                    -lco FID=gid \
                    -nlt PROMOTE_TO_MULTI \
                    -sql "SELECT *, '$YEAR' as data_year FROM OGRGeoJSON" \
                    -progress
            fi
        done
    done
    
    # Create indexes for efficient querying by NAME_3
    echo "Creating database indexes..."
    psql -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DB" -U "$PG_USER" << EOF
-- Create index on NAME_3 for fast barangay lookup
CREATE INDEX IF NOT EXISTS idx_${TABLE_NAME}_name3 ON $TABLE_NAME (name_3);

-- Create index on data_year for year-based filtering
CREATE INDEX IF NOT EXISTS idx_${TABLE_NAME}_year ON $TABLE_NAME (data_year);

-- Create composite index for NAME_3 + year combination
CREATE INDEX IF NOT EXISTS idx_${TABLE_NAME}_name3_year ON $TABLE_NAME (name_3, data_year);

-- Create spatial index on geometry
CREATE INDEX IF NOT EXISTS idx_${TABLE_NAME}_geom ON $TABLE_NAME USING GIST (geom);

-- Show table statistics
SELECT 
    data_year,
    COUNT(*) as barangay_count,
    COUNT(DISTINCT name_3) as unique_barangays
FROM $TABLE_NAME 
GROUP BY data_year 
ORDER BY data_year;

EOF
}

echo "Starting indexed barangay import..."
import_all_barangays
echo "Indexed barangay import completed!"