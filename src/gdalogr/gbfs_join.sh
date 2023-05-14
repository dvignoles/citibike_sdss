#!/bin/bash

# work from this dir for relative paths
cwd=$(dirname "${BASH_SOURCE[0]}")
cd $cwd

gbfs="../../data/prepared/gbfs.gpkg"
output="../../data/processed/gbfs_summary.gpkg"

# copy stations
if [ -f "$output" ]; then
    rm $output
fi
ogr2ogr -overwrite $output $gbfs station 


for PEAK in "_" "_morning_peak_" "_evening_peak_" "_offpeak_" "_peak_"
do
    status_table="status${PEAK}summary"
    echo $status_table
    
    sql=$(cat << EOF
    SELECT ${status_table}.*, station.geom
    FROM station JOIN ${status_table} ON station.station_id = ${status_table}.station_id
EOF
)

    ogr2ogr -append -nln $status_table -sql "$sql" $output $gbfs 
done
