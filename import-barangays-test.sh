#!/bin/bash

# Database connection details
PG_HOST="localhost"
PG_PORT="5432"
PG_DB="gis"
PG_USER="postgres"
PG_PASS="postgres"

# Export password for ogr2ogr
export PGPASSWORD="$PG_PASS"

# Test individual file import
test_single_file() {
    local TEST_FILE="maps/2011/geojson/barangays/barangays-municity-297-plaridel.json"
    local TABLE_NAME="test_barangays"
    
    echo "Testing import of single file: $TEST_FILE"
    
    if [ ! -f "$TEST_FILE" ]; then
        echo "Test file not found: $TEST_FILE"
        echo "Available files:"
        find maps/2011/geojson/barangays -name "*.json" | head -5
        return 1
    fi
    
    echo "File exists. Checking content..."
    echo "First few lines of the file:"
    head -10 "$TEST_FILE"
    
    echo "Testing ogr2ogr import..."
    ogr2ogr \
        -f "PostgreSQL" \
        PG:"host=$PG_HOST port=$PG_PORT dbname=$PG_DB user=$PG_USER password=$PG_PASS" \
        -s_srs EPSG:4326 \
        -t_srs EPSG:4326 \
        "$TEST_FILE" \
        -nln "$TABLE_NAME" \
        -overwrite \
        -lco GEOMETRY_NAME=geom \
        -lco FID=gid \
        -nlt PROMOTE_TO_MULTI \
        -skipfailures \
        -progress \
        -verbose
    
    if [ $? -eq 0 ]; then
        echo "Test import successful!"
        
        # Check the imported data
        psql -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DB" -U "$PG_USER" << EOF
-- Show table info
\d $TABLE_NAME

-- Show sample data
SELECT name_3, COUNT(*) 
FROM $TABLE_NAME 
GROUP BY name_3 
ORDER BY name_3 
LIMIT 10;
EOF
    else
        echo "Test import failed!"
        return 1
    fi
}

# Import all barangay data into a single table with NAME_3 indexing
import_all_barangays() {
    local TABLE_NAME="all_barangays"
    
    echo "Creating indexed barangay table: $TABLE_NAME"
    
    # Loop through each year
    for YEAR in 2011 2019 2023; do
        echo "Processing barangays for year $YEAR..."
        
        # Check if directory exists
        if [ ! -d "maps/$YEAR/geojson/barangays" ]; then
            echo "Directory not found: maps/$YEAR/geojson/barangays"
            continue
        fi
        
        # Count files first
        FILE_COUNT=$(find "maps/$YEAR/geojson/barangays" -name "*.json" | wc -l)
        echo "Found $FILE_COUNT JSON files for year $YEAR"
        
        if [ "$FILE_COUNT" -eq 0 ]; then
            echo "No JSON files found for year $YEAR"
            continue
        fi
        
        find "maps/$YEAR/geojson/barangays" -name "*.json" | while read FILE; do
            if [ -f "$FILE" ]; then
                echo "Importing $FILE into table $TABLE_NAME"
                
                # Import the file - use simpler approach
                ogr2ogr \
                    -f "PostgreSQL" \
                    PG:"host=$PG_HOST port=$PG_PORT dbname=$PG_DB user=$PG_USER password=$PG_PASS" \
                    "$FILE" \
                    -nln "$TABLE_NAME" \
                    -append \
                    -lco GEOMETRY_NAME=geom \
                    -lco FID=gid \
                    -nlt PROMOTE_TO_MULTI \
                    -skipfailures
                
                if [ $? -eq 0 ]; then
                    echo "✓ Successfully imported $FILE"
                    
                    # Update the newly imported rows with the year
                    echo "Adding year information for $YEAR..."
                    psql -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DB" -U "$PG_USER" << EOSQL
-- Add data_year column if it doesn't exist
ALTER TABLE $TABLE_NAME ADD COLUMN IF NOT EXISTS data_year VARCHAR(4);

-- Update rows that don't have year set
UPDATE $TABLE_NAME SET data_year = '$YEAR' WHERE data_year IS NULL;
EOSQL
                else
                    echo "✗ Failed to import $FILE"
                fi
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

# Main execution
echo "=== Barangay Import Tool ==="
echo "1. Running single file test first..."
test_single_file

if [ $? -eq 0 ]; then
    echo "2. Test successful! Proceeding with full import..."
    import_all_barangays
    echo "Indexed barangay import completed!"
else
    echo "2. Test failed! Please check the issues above."
    exit 1
fi