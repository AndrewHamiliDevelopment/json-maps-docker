#!/bin/bash

# Database connection details
PG_HOST="localhost"
PG_PORT="5432"
PG_DB="gis"
PG_USER="postgres"
PG_PASS="postgres"

# Export password for ogr2ogr
export PGPASSWORD="$PG_PASS"

# Import 2019 data by resolution
YEAR=2019

echo "Importing 2019 LOW RESOLUTION data..."
find "maps/$YEAR" -type f -path "*/lowres/*" -name "*.json" | while read FILE; do
  if [ -f "$FILE" ]; then
    echo "Importing $FILE into table geojson_2019_lowres"
    ogr2ogr \
      -f "PostgreSQL" \
      PG:"host=$PG_HOST port=$PG_PORT dbname=$PG_DB user=$PG_USER password=$PG_PASS" \
      "$FILE" \
      -nln "geojson_2019_lowres" \
      -append \
      -lco GEOMETRY_NAME=geom \
      -lco FID=gid \
      -nlt PROMOTE_TO_MULTI \
      -progress
  fi
done

echo "Importing 2019 MEDIUM RESOLUTION data..."
find "maps/$YEAR" -type f -path "*/medres/*" -name "*.json" | while read FILE; do
  if [ -f "$FILE" ]; then
    echo "Importing $FILE into table geojson_2019_medres"
    ogr2ogr \
      -f "PostgreSQL" \
      PG:"host=$PG_HOST port=$PG_PORT dbname=$PG_DB user=$PG_USER password=$PG_PASS" \
      "$FILE" \
      -nln "geojson_2019_medres" \
      -append \
      -lco GEOMETRY_NAME=geom \
      -lco FID=gid \
      -nlt PROMOTE_TO_MULTI \
      -progress
  fi
done

echo "Importing 2019 HIGH RESOLUTION data..."
find "maps/$YEAR" -type f -path "*/hires/*" -name "*.json" | while read FILE; do
  if [ -f "$FILE" ]; then
    echo "Importing $FILE into table geojson_2019_hires"
    ogr2ogr \
      -f "PostgreSQL" \
      PG:"host=$PG_HOST port=$PG_PORT dbname=$PG_DB user=$PG_USER password=$PG_PASS" \
      "$FILE" \
      -nln "geojson_2019_hires" \
      -append \
      -lco GEOMETRY_NAME=geom \
      -lco FID=gid \
      -nlt PROMOTE_TO_MULTI \
      -progress
  fi
done

echo "2019 resolution-based import completed!"