#!/bin/bash

# work from this dir for relative paths
#cwd=$(dirname "${BASH_SOURCE[0]}")
#cd $cwd

gbfs="../../data/prepared/gbfs.gpkg"
output="../../data/prepared/citibike_trips_summary.gpkg"

# copy stations
if [ -f $output ]; then
    rm $output
fi

echo "[GDAL/OGR]: Citi Bike Trip"
echo "[GDAL/OGR]: Citi Bike Trip -- Writing Stations"
ogr2ogr -overwrite $output $gbfs station 

prelude=$(cat << EOF
ATTACH DATABASE "$gbfs" AS gbfs;
EOF
)

# 2021 is weird, citibike switched their ID format in february 2021
input_gpkg="../../data/prepared/citibike_trips_2021.gpkg"
table="trips_summary_2021"

sql=$(cat << EOF
WITH stations AS (
    SELECT station_id, legacy_id, short_name as modern_id, capacity, geom
    FROM gbfs.station
),
start1 AS (
    SELECT start_station_id as station_id, COUNT(*) as start_trips
    FROM trips 
    WHERE start_station_id != end_station_id
    AND CAST(strftime('%m', started_at) as int) = 1
    GROUP BY start_station_id
),
end1 AS (
    SELECT end_station_id as station_id, COUNT(*) as end_trips
    FROM trips 
    WHERE start_station_id != end_station_id
    AND CAST(strftime('%m', started_at) as int) = 1
    GROUP BY end_station_id
),
totals1 AS (
    SELECT
    stations.station_id as station_id,
    start1.start_trips + end1.end_trips as total_trips
    FROM stations
    LEFT JOIN start1 on stations.legacy_id = start1.station_id
    LEFT JOIN end1 on stations.legacy_id = end1.station_id
),
start2 AS (
    SELECT start_station_id as station_id, COUNT(*) as start_trips
    FROM trips 
    WHERE start_station_id != end_station_id
    AND CAST(strftime('%m', started_at) as int) > 1
    GROUP BY start_station_id
),
end2 AS (
    SELECT end_station_id as station_id, COUNT(*) as end_trips
    FROM trips 
    WHERE start_station_id != end_station_id
    AND CAST(strftime('%m', started_at) as int) > 1
    GROUP BY end_station_id
),
totals2 AS (
    SELECT
    stations.station_id as station_id,
    start2.start_trips + end2.end_trips as total_trips
    FROM stations
    LEFT JOIN start2 on stations.modern_id = start2.station_id
    LEFT JOIN end2 on stations.modern_id = end2.station_id
),
totals AS (
    SELECT 
    totals1.station_id as station_id,
    totals1.total_trips + totals2.total_trips as total_trips
    FROM totals1 LEFT JOIN totals2 on totals1.station_id = totals2.station_id
)
    SELECT 
    totals.station_id,
    stations.capacity,
    totals.total_trips,
    totals.total_trips*1.0 / stations.capacity*1.0 as trips_per_dock,
    totals.total_trips*1.0 / 365 as trips_per_day,
    totals.total_trips*1.0 / (365 * stations.capacity*1.0) as trips_per_day_per_dock,
    stations.geom
    FROM stations
    LEFT JOIN totals on stations.station_id = totals.station_id

EOF
)

echo "[GDAL/OGR]: Citi Bike Trip -- Aggregating ${table}"
ogr2ogr -oo PRELUDE_STATEMENTS="$prelude" -append -nln "$table" -sql "$sql" $output $input_gpkg

# all other years are internally consistent with their station_ids
for year in 2019 2020 2022 2023
do
    input_gpkg="../../data/prepared/citibike_trips_${year}.gpkg"
    join_id=$( [[ $year -lt 2021 ]] && echo "legacy_id" || echo "modern_id" )
    table="trips_summary_${year}"

    sql=$(cat << EOF
    WITH stations AS (
        SELECT station_id, legacy_id, short_name as modern_id, capacity, geom
        FROM gbfs.station
    ),
    days AS(
        SELECT COUNT(DISTINCT strftime('%Y%m%d', started_at)) as count FROM trips
    ),
    start AS (
        SELECT start_station_id as station_id, COUNT(*) as start_trips
        FROM trips 
        WHERE start_station_id != end_station_id
        GROUP BY start_station_id
    ),
    end AS (
        SELECT end_station_id as station_id, COUNT(*) as end_trips
        FROM trips 
        WHERE start_station_id != end_station_id
        GROUP BY end_station_id
    ),
    totals AS (
        SELECT
        stations.station_id as station_id,
        stations.capacity as capacity,
        start.start_trips + end.end_trips as total_trips,
        stations.geom
        FROM stations
        LEFT JOIN start on stations.${join_id} = start.station_id
        LEFT JOIN end on stations.${join_id} = end.station_id
    )
    SELECT 
    totals.station_id,
    totals.capacity,
    totals.total_trips,
    total_trips*1.0 / totals.capacity*1.0 as trips_per_dock,
    total_trips*1.0 / days.count*1.0 as trips_per_day,
    total_trips*1.0 / (days.count*1.0 * totals.capacity*1.0) as trips_per_day_per_dock,

    totals.geom
    FROM totals,days
EOF
)

    echo "[GDAL/OGR]: Citi Bike Trip -- Aggregating ${table}"
    ogr2ogr -oo PRELUDE_STATEMENTS="$prelude" -append -nln "$table" -sql "$sql" $output $input_gpkg
done

# final summary

sql=$(cat << EOF
WITH all_summary AS(
    SELECT station_id, total_trips, trips_per_day, trips_per_day_per_dock FROM trips_summary_2019
    UNION ALL
    SELECT station_id, total_trips, trips_per_day, trips_per_day_per_dock FROM trips_summary_2020
    UNION ALL
    SELECT station_id, total_trips, trips_per_day, trips_per_day_per_dock FROM trips_summary_2021
    UNION ALL
    SELECT station_id, total_trips, trips_per_day, trips_per_day_per_dock FROM trips_summary_2022
    UNION ALL
    SELECT station_id, total_trips, trips_per_day, trips_per_day_per_dock FROM trips_summary_2023
),
totals AS(
    SELECT
    station_id,
    SUM(total_trips) as total_trips,
    AVG(trips_per_day) as trips_per_day,
    AVG(trips_per_day_per_dock) as trips_per_day_per_dock
    FROM all_summary
    GROUP BY station_id
)
SELECT 
totals.*, 
totals.total_trips / station.capacity as trips_per_dock,
station.capacity,
station.geom
FROM station LEFT JOIN totals ON station.station_id = totals.station_id

EOF
)

echo "[GDAL/OGR]: Citi Bike Trip -- Aggregating trips_summary"
ogr2ogr -append -nln "trips_summary" -sql "$sql" $output $output

echo $output