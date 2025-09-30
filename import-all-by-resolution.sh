#!/bin/bash

# Database connection details
PG_HOST="localhost"
PG_PORT="5432"
PG_DB="gis"
PG_USER="postgres"
PG_PASS="postgres"

# Export password for ogr2ogr
export PGPASSWORD="$PG_PASS"

# Master script to import all data organized by resolution across all years
echo "Starting comprehensive resolution-based import..."

echo "================================================"
echo "PHASE 1: LOW RESOLUTION DATA (All Years)"
echo "================================================"
for YEAR in 2011 2019 2023; do
  echo "Processing $YEAR low resolution data..."
  find "maps/$YEAR" -type f -path "*/lowres/*" -name "*.json" | while read FILE; do
    if [ -f "$FILE" ]; then
      echo "Importing $FILE into table geojson_lowres"
      ogr2ogr \
        -f "PostgreSQL" \
        PG:"host=$PG_HOST port=$PG_PORT dbname=$PG_DB user=$PG_USER password=$PG_PASS" \
        "$FILE" \
        -nln "geojson_lowres" \
        -append \
        -lco GEOMETRY_NAME=geom \
        -lco FID=gid \
        -nlt PROMOTE_TO_MULTI \
        -progress
    fi
  done
done

echo "================================================"
echo "PHASE 2: MEDIUM RESOLUTION DATA (All Years)"
echo "================================================"
for YEAR in 2011 2019 2023; do
  echo "Processing $YEAR medium resolution data..."
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

echo "================================================"
echo "PHASE 3: HIGH RESOLUTION DATA (All Years)"
echo "================================================"
for YEAR in 2011 2019 2023; do
  echo "Processing $YEAR high resolution data..."
  find "maps/$YEAR" -type f -path "*/hires/*" -name "*.json" | while read FILE; do
    if [ -f "$FILE" ]; then
      echo "Importing $FILE into table geojson_hires"
      ogr2ogr \
        -f "PostgreSQL" \
        PG:"host=$PG_HOST port=$PG_PORT dbname=$PG_DB user=$PG_USER password=$PG_PASS" \
        "$FILE" \
        -nln "geojson_hires" \
        -append \
        -lco GEOMETRY_NAME=geom \
        -lco FID=gid \
        -nlt PROMOTE_TO_MULTI \
        -progress
    fi
  done
done

echo "================================================"
echo "ALL RESOLUTION-BASED IMPORTS COMPLETED!"
echo "================================================"
echo "Data has been imported into:"
echo "- geojson_lowres (Low resolution data from all years)"
echo "- geojson_medres (Medium resolution data from all years)"
echo "- geojson_hires (High resolution data from all years)"