#!/bin/bash

# Database connection details
PG_HOST="localhost"
PG_PORT="5432"
PG_DB="gis"
PG_USER="postgres"
PG_PASS="postgres"

# Export password for ogr2ogr
export PGPASSWORD="$PG_PASS"

# Function to clean table name (remove spaces, special characters)
clean_table_name() {
    echo "$1" | sed 's/ /_/g' | sed 's/[^a-zA-Z0-9_]//g' | tr '[:upper:]' '[:lower:]'
}

# Extract unique NAME_3 values from a JSON file using jq
extract_barangay_names() {
    local file="$1"
    if command -v jq &> /dev/null; then
        jq -r '.features[].properties.NAME_3' "$file" | sort | uniq
    else
        echo "jq not found. Please install jq or use alternative method."
        exit 1
    fi
}

# Import by NAME_3 for a specific year
import_by_name3() {
    local YEAR=$1
    echo "Processing barangays for year $YEAR..."
    
    # Find all barangay files for this year
    find "maps/$YEAR/geojson/barangays" -name "*.json" | while read FILE; do
        if [ -f "$FILE" ]; then
            echo "Processing file: $FILE"
            
            # Extract barangay names from this file
            extract_barangay_names "$FILE" | while read BARANGAY_NAME; do
                if [ -n "$BARANGAY_NAME" ] && [ "$BARANGAY_NAME" != "null" ]; then
                    # Clean the barangay name for table naming
                    CLEAN_NAME=$(clean_table_name "$BARANGAY_NAME")
                    TABLE_NAME="barangay_${CLEAN_NAME}_${YEAR}"
                    
                    echo "Importing barangay '$BARANGAY_NAME' into table '$TABLE_NAME'"
                    
                    # Use ogr2ogr with SQL WHERE clause to filter by NAME_3
                    ogr2ogr \
                        -f "PostgreSQL" \
                        PG:"host=$PG_HOST port=$PG_PORT dbname=$PG_DB user=$PG_USER password=$PG_PASS" \
                        "$FILE" \
                        -nln "$TABLE_NAME" \
                        -overwrite \
                        -lco GEOMETRY_NAME=geom \
                        -lco FID=gid \
                        -nlt PROMOTE_TO_MULTI \
                        -where "NAME_3 = '$BARANGAY_NAME'" \
                        -progress
                fi
            done
        fi
    done
}

# Main execution
echo "Starting barangay-specific import..."

# Process each year
for YEAR in 2011 2019 2023; do
    import_by_name3 $YEAR
done

echo "Barangay-specific import completed!"