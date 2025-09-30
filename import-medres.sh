#!/bin/bash

# Database connection details
PG_HOST="localhost"
PG_PORT="5432"
PG_DB="gis"
PG_USER="postgres"
PG_PASS="postgres"

# Export password for ogr2ogr
export PGPASSWORD="$PG_PASS"

# Import medium resolution data from all years
echo "Importing MEDIUM RESOLUTION data from all years..."

for YEAR in 2011 2019 2023; do
  echo "Processing year $YEAR - Medium Resolution"
  find "maps/$YEAR" -type f -path "*/medres/*" -name "*.json" | while read FILE; do
    if [ -f "$FILE" ]; then
      echo "Importing $FILE into table geojson_medres"
      ogr2ogr \
        -f "PostgreSQL" \
        PG:"host=$PG_HOST port=$PG_PORT dbname=$PG_DB user=$PG_USER password=$PG_PASS" \
        "$FILE" \
        -nln "geojson_medres" \
        -append \
        -lco GEOMETRY_NAME=geom \
        -lco FID=gid \
        -nlt PROMOTE_TO_MULTI \
        -progress
    fi
  done
done

echo "Medium resolution import completed!"