#!/bin/bash

mta2019="../../data/prepared/mta_2019.gpkg"
mta2020="../../data/prepared/mta_2020.gpkg"
mta2021="../../data/prepared/mta_2021.gpkg"
mta2022="../../data/prepared/mta_2022.gpkg"
mta2023="../../data/prepared/mta_2023.gpkg"
output="../../data/processed/mta_allyears.gpkg"

if [ -f "$output" ]; then
    echo "replacing existing $output"
    rm $output
fi

prelude=$(cat << EOF
ATTACH DATABASE "$mta2019" AS m19;
ATTACH DATABASE "$mta2020" AS m20;
ATTACH DATABASE "$mta2021" AS m21;
ATTACH DATABASE "$mta2022" AS m22;
EOF
)

for PEAK in "_" "_morning_peak_" "_evening_peak_" "_offpeak_" "_peak_"
do
    annual_table="annual${PEAK}complex"
    
    sql=$(cat << EOF
    WITH g23 AS (
        SELECT stations.complex_id as complex_id, 
            AVG(m23_ac.mean_daily_entries) as mean_daily_entries, 
            SUM(m23_ac.total_entries) as total_entries,
            AVG(m23_ac.mean_daily_exits) as mean_daily_exits,
            SUM(m23_ac.total_exits) as total_exits,
            stations.geom
        FROM stations
        JOIN ${annual_table} m23_ac ON stations.complex_id = m23_ac.complex_id
        GROUP BY stations.complex_id
    ),
    g22 AS (
        SELECT stations.complex_id as complex_id, 
            AVG(m22_ac.mean_daily_entries) as mean_daily_entries, 
            SUM(m22_ac.total_entries) as total_entries,
            AVG(m22_ac.mean_daily_exits) as mean_daily_exits,
            SUM(m22_ac.total_exits) as total_exits
        FROM stations
        JOIN m22.${annual_table} m22_ac ON stations.complex_id = m22_ac.complex_id
        GROUP BY stations.complex_id
    ),
    g21 AS (
        SELECT stations.complex_id as complex_id, 
            AVG(m21_ac.mean_daily_entries) as mean_daily_entries, 
            SUM(m21_ac.total_entries) as total_entries,
            AVG(m21_ac.mean_daily_exits) as mean_daily_exits,
            SUM(m21_ac.total_exits) as total_exits
        FROM stations
        JOIN m21.${annual_table} m21_ac ON stations.complex_id = m21_ac.complex_id
        GROUP BY stations.complex_id
    ),
    g20 AS (
        SELECT stations.complex_id as complex_id, 
            AVG(m20_ac.mean_daily_entries) as mean_daily_entries, 
            SUM(m20_ac.total_entries) as total_entries,
            AVG(m20_ac.mean_daily_exits) as mean_daily_exits,
            SUM(m20_ac.total_exits) as total_exits
        FROM stations
        JOIN m20.${annual_table} m20_ac ON stations.complex_id = m20_ac.complex_id
        GROUP BY stations.complex_id
    ),
    g19 AS (
        SELECT stations.complex_id as complex_id, 
            AVG(m19_ac.mean_daily_entries) as mean_daily_entries, 
            SUM(m19_ac.total_entries) as total_entries,
            AVG(m19_ac.mean_daily_exits) as mean_daily_exits,
            SUM(m19_ac.total_exits) as total_exits
        FROM stations
        JOIN m19.${annual_table} m19_ac ON stations.complex_id = m19_ac.complex_id
        GROUP BY stations.complex_id
    ),
    complexes AS(
        SELECT 
            complex_id,
            ST_Centroid(ST_Collect(geom)) as geom
        FROM stations
        GROUP BY complex_id
    )
    SELECT
    g23.complex_id,

    g19.mean_daily_entries as mean_daily_entries_2019,
    g20.mean_daily_entries as mean_daily_entries_2020,
    g21.mean_daily_entries as mean_daily_entries_2021,
    g22.mean_daily_entries as mean_daily_entries_2022,
    g23.mean_daily_entries as mean_daily_entries_2023,
    (g19.mean_daily_entries + g20.mean_daily_entries + g21.mean_daily_entries + g22.mean_daily_entries + g23.mean_daily_entries) / 5 as mean_daily_entries_all,

    g19.mean_daily_exits as mean_daily_exits_2019,
    g20.mean_daily_exits as mean_daily_exits_2020,
    g21.mean_daily_exits as mean_daily_exits_2021,
    g22.mean_daily_exits as mean_daily_exits_2022,
    g23.mean_daily_exits as mean_daily_exits_2023,
    (g19.mean_daily_exits + g20.mean_daily_exits + g21.mean_daily_exits + g22.mean_daily_exits + g23.mean_daily_exits) / 5 as mean_daily_exits_all,

    g19.total_entries as total_entries_2019,
    g20.total_entries as total_entries_2020,
    g21.total_entries as total_entries_2021,
    g22.total_entries as total_entries_2022,
    g23.total_entries as total_entries_2023,
    (g19.total_entries + g20.total_entries + g21.total_entries + g22.total_entries + g23.total_entries) as total_entries_all,

    g19.total_exits as total_exits_2019,
    g20.total_exits as total_exits_2020,
    g21.total_exits as total_exits_2021,
    g22.total_exits as total_exits_2022,
    g23.total_exits as total_exits_2023,
    (g19.total_exits + g20.total_exits + g21.total_exits + g22.total_exits + g23.total_exits) as total_exits_all,
    complexes.geom as geom

    FROM g23
    LEFT JOIN g22 ON g23.complex_id = g22.complex_id
    LEFT JOIN g21 ON g23.complex_id = g21.complex_id
    LEFT JOIN g20 ON g23.complex_id = g20.complex_id
    LEFT JOIN g19 ON g23.complex_id = g19.complex_id
    LEFT JOIN complexes ON g23.complex_id = complexes.complex_id
EOF
)

    ogr2ogr -oo PRELUDE_STATEMENTS="$prelude" -append -nln $annual_table -sql "$sql" $output $mta2023 
done
