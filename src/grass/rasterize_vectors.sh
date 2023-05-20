#! /usr/bin/bash

######################
# CLI Interface Start#
######################

function PrintUsage () {
    echo "Usage ${0##*/} [options] <output_directory>"
    echo "      -w,  --walkradi  <feet>   [OPTIONAL] DEFAULT=2640"
    echo "      -m,  --streetmax <feet>   [OPTIONAL] DEFAULT=60"
    echo "      -c,  --cbmin     <feet>   [OPTIONAL] DEFAULT=100"
    exit 1
}

OUTPUT_DIR=""

# units are feet
WALK_RADIUS=2640
MAX_DIST_CB_TO_STREET=60
MIN_DIST_CB_TO_CB=100

if [ "${1}" == "" ]; then 
    PrintUsage
fi

while [ "${1}" != "" ]
do
    case "${1}" in 
    (-w|--walkradi)
        shift
        if [ "${1}" == "" ]; then PrintUsage; fi
        if [[ "${1}" =~ ^[0-9]+$ ]]
        then
            WALK_RADIUS="${1}"
        else
            PrintUsage
        fi
        shift
    ;;
    (-m|--streetmax)
        shift
        if [ "${1}" == "" ]; then PrintUsage; fi
        if [[ "${1}" =~ ^[0-9]+$ ]]
        then
            MAX_DIST_CB_TO_STREET="${1}"
        else
            PrintUsage
        fi
        shift
    ;;
    (-c|--cbmin)
        shift
        if [ "${1}" == "" ]; then PrintUsage; fi
        if [[ "${1}" =~ ^[0-9]+$ ]]
        then
            MIN_DIST_CB_TO_CB="${1}"
        else
            PrintUsage
        fi
        shift
    ;;
    (-)
        OUTPUT_DIR="${1}"
        shift
    ;;
    (*)
        OUTPUT_DIR="${1}"
        shift
    ;;
    esac
done

if [ "${OUTPUT_DIR}" == "" ]; then 
	OUTPUT_DIR=$(pwd)
fi;

######################
# CLI Interface End  #
######################

PROJECT_DIR="."
DATA_DIR=$PROJECT_DIR"/data/prepared"
SRC_DIR=$PROJECT_DIR"/src/grass"

if [ ! -d "$OUTPUT_DIR" ]; then
    mkdir -p $OUTPUT_DIR
fi

. "${SRC_DIR}/set_grass_constants.sh"
. "${SRC_DIR}/define_sdss_util.sh"

##################
# Import vectors #
##################
# ACS population
load_layer acs.gpkg acs V_acs

# NYC Open Data
load_layer open_data.gpkg bike_routes V_bike_routes
load_layer open_data.gpkg boroughs V_boroughs
load_layer open_data.gpkg motor_vehicle_crashes V_crashes
load_layer open_data.gpkg streets V_streets

# GBFS
load_layer gbfs_summary.gpkg status_peak_summary V_gbfs_peak
load_layer gbfs_summary.gpkg status_offpeak_summary V_gbfs_offpeak

# Suggest a Station
load_layer sas.gpkg sas V_SAS

# MTA turnstile data
load_layer mta_allyears.gpkg annual_complex V_mta

# Trips
load_layer citibike_trips_summary.gpkg trips_summary_2023 V_trips
	
################################
# Calculate constraint layer #
################################
# This fine-resolution raster identifies possible locations for new Citi Bike stations

### Fine-resolution rasters for calculating constraint
g.region vector=V_boroughs res=$FINE_RES

# Boroughs
convert_char_to_int V_boroughs boro_code
rasterize_vector V_boroughs area boro_code R_boroughs_fine

r.mask raster=R_boroughs_fine

# Streets
convert_char_to_int V_streets rw_type
rasterize_vector V_streets line rw_type R_streets
save_raster R_streets

# Reclassify streets raster to exclude highways, bridges, tunnels, etc.
# See streets metadata doc in citbike_sdss/references for all codes
echo "1 3 5 6 7 10 11 13 = 1
2 4 8 9 12 14 = NULL" | 
    r.reclass input=R_streets output=R_bikeable_streets rules=- --overwrite

# Calculate raster and constraint for distance to bikeable streets

r.grow.distance input=R_bikeable_streets distance=R_dist_bikeable_street metric=euclidean --overwrite

reclass_dist_constraint R_dist_bikeable_street \
			0 \
			$MAX_DIST_CB_TO_STREET \
			R_bikeable_street_constraint

load_layer gbfs_summary.gpkg station V_cb_stations

v.to.rast input=V_cb_stations type=point output=R_cb_stations \
	  use=val value=1 --overwrite

# Calculate raster and constraint for distance to Citi Bike stations
r.grow.distance input=R_cb_stations distance=R_dist_cb metric=euclidean --overwrite

reclass_dist_constraint R_dist_cb \
			$MIN_DIST_CB_TO_CB \
			$WALK_RADIUS \
			R_cb_constraint

r.mapcalc "R_constraint = R_cb_constraint * R_bikeable_street_constraint"

save_raster R_constraint constraint.gtiff

###########################
# Service area mask layer #
###########################
# This medium-resolution raster defines the service area of the current system
# WALK_RADIUS buffer around all existing stations, clipped by boroughs

# Resample boroughs raster to medium resolution
g.region vector=V_boroughs res=$MEDIUM_RES
r.resample input=R_boroughs_fine output=R_boroughs_medium
save_raster R_boroughs_medium

# Reclassify R distance constraint, removing min. CB distance constraint
# (to calculate continuous service area without "holes" by stations)
reclass_dist_constraint R_dist_cb \
			0 \
			$WALK_RADIUS \
			R_service_area_fine

# Resample fine service area to medium resolution
r.resample input=R_service_area_fine output=R_service_area

g.copy raster=R_service_area,R_service_area_mask
r.mask -r
r.mask raster=R_boroughs_medium

r.null R_service_area_mask setnull=0
save_raster R_service_area_mask

###########################
# Rasterize vector layers #
###########################

### Medium-resolution rasters for calculating suitability layers
g.region vector=V_boroughs res=$MEDIUM_RES

# Bike routes
convert_char_to_int V_bike_routes lanecount
rasterize_vector V_bike_routes line lanecount R_bike_routes
save_raster R_bike_routes

### Exploratory layers covering all boroughs
r.mask -r
r.mask R_boroughs_medium

# ACS population
v.db.addcolumn map=V_acs column='pop_per_area real'
v.db.addcolumn map=V_acs column='cell_area real'

v.db.update map=V_acs column=cell_area value=$MEDIUM_CELL_AREA
v.db.update map=V_acs column=pop_per_area query_column="population * cell_area / area"

rasterize_vector V_acs area pop_per_area R_acs
save_raster R_acs

sum_rast_in_walk_radius R_acs R_acs_sum
save_raster R_acs_sum

# Crashes
bin_vect_to_rast V_crashes \
		 number_of_cyclist_injured \
		 R_crashes \
		 sum

sum_rast_in_walk_radius R_crashes R_crashes_sum
save_raster R_crashes_sum

# Docks / population within radius
bin_vect_to_rast V_cb_stations \
		 capacity \
		 R_cb_capacity \
		 sum

sum_rast_in_walk_radius R_cb_capacity R_cb_capacity_sum
save_raster R_cb_capacity_sum

r.mapcalc "R_docks_per_person = R_cb_capacity_sum / R_acs_sum"
# Real values of docks/person should range 0--.1, but data issues
# introduce some values > 10
save_raster R_docks_per_person
r.mapcalc "R_docks_per_person_capped = min(R_docks_per_person * 1000, 100)"
save_raster R_docks_per_person_capped

# Distance to bike routes
r.grow.distance input=R_bike_routes distance=R_bike_route_dist metric=euclidean --overwrite
save_raster R_bike_route_dist

# MTA connectivity
bin_vect_to_rast V_mta \
		 mean_daily_entries_2023 \
		 R_mta_daily_entries \
		 sum

bin_vect_to_rast V_mta \
		 mean_daily_exits_2023 \
		 R_mta_daily_exits \
		 sum

sum_rast_in_walk_radius R_mta_daily_entries R_mta_mean_daily_entries_sum
sum_rast_in_walk_radius R_mta_daily_exits R_mta_mean_daily_exits_sum
# Impose a ceiling value to preserve meaingful variation in the index
r.mapcalc "R_mta_mean_daily_exits_sum_capped = min(R_mta_mean_daily_exits_sum, 500000)"
r.mapcalc "R_mta_mean_daily_entries_sum_capped = min(R_mta_mean_daily_entries_sum, 300000)"

save_raster R_mta_mean_daily_entries_sum
save_raster R_mta_mean_daily_exits_sum

# Distance to complex
rasterize_vector V_mta point total_exits_all R_mta_complexes
save_raster R_mta_complexes
r.null R_mta_complexes setnull=0
r.grow.distance input=R_mta_complexes distance=R_mta_complex_dist metric=euclidean --overwrite
save_raster R_mta_complex_dist

### Analytical layers covering current CB system
r.mask -r
r.mask R_service_area_mask

# GBFS peak
convert_char_to_real V_gbfs_peak bikes_available_eq0
convert_char_to_real V_gbfs_peak docks_available_eq0
v.voronoi input=V_gbfs_peak output=V_gbfs_peak_voronoi

rasterize_vector V_gbfs_peak_voronoi area bikes_available_eq0 R_gbfs_peak_bikes_eq0
rasterize_vector V_gbfs_peak_voronoi area docks_available_eq0 R_gbfs_peak_docks_eq0
save_raster R_gbfs_peak_bikes_eq0
save_raster R_gbfs_peak_docks_eq0

# GBFS off-peak
convert_char_to_real V_gbfs_offpeak bikes_available_eq0
convert_char_to_real V_gbfs_offpeak docks_available_eq0
v.voronoi input=V_gbfs_offpeak output=V_gbfs_offpeak_voronoi

rasterize_vector V_gbfs_offpeak_voronoi area bikes_available_eq0 R_gbfs_offpeak_bikes_eq0
rasterize_vector V_gbfs_offpeak_voronoi area docks_available_eq0 R_gbfs_offpeak_docks_eq0
save_raster R_gbfs_offpeak_bikes_eq0
save_raster R_gbfs_offpeak_docks_eq0

# Profitability (trips per dock)
v.voronoi input=V_trips output=V_trips_voronoi
# Approximately five stations have a data entry issue where their capacity is missing a zero; this correct those stations
# (20 trips per day per dock is implausibly high and above only occurs with the incorrectly entered stations)
v.db.addcolumn V_trips_voronoi column="adjustment real"
v.db.update V_trips_voronoi column=adjustment value=10
v.db.update V_trips_voronoi column=trips_per_day_per_dock where="trips_per_day_per_dock > 20" query_column="trips_per_day_per_dock / adjustment"
rasterize_vector V_trips_voronoi area trips_per_day_per_dock R_trips_per_day_per_dock

save_raster R_trips_per_day_per_dock

# Potential users
r.mask -r
echo "0 = 1
1 = 0" | r.reclass input=R_service_area rules=- output=R_service_area_inverse
save_raster R_service_area_inverse

r.mapcalc "R_potential_pop = R_acs * R_service_area_inverse"
r.mapcalc "R_potential_area = ${MEDIUM_CELL_AREA} * R_service_area_inverse"
sum_rast_in_walk_radius R_potential_pop R_potential_pop_sum
sum_rast_in_walk_radius R_potential_area R_potential_area_sum

save_raster R_potential_area

r.mask raster=R_service_area_mask

r.mapcalc "R_potential_pop_service_area = R_potential_pop_sum"
r.mapcalc "R_potential_area_service_area = R_potential_area_sum"
save_raster R_potential_area_service_area
save_raster R_potential_pop_service_area

###########
# Indices #
###########
# Service area mask remains active from section above
# Note that normalize_raster creates a new raster with same name but "_norm" appended
# (see define_sdss_util)

# Transit
normalize_raster R_mta_mean_daily_exits_sum_capped
normalize_raster R_mta_mean_daily_entries_sum_capped
normalize_raster R_mta_complex_dist

# Find inverse squared distance; implement one cell-length as floor value for
# distance from MTA complex to avoid division by 0.
r.mapcalc "R_mta_complex_dist_floor = max(R_mta_complex_dist, ${MEDIUM_RES})"
save_raster R_mta_complex_dist_floor
r.mapcalc "R_mta_complex_inv_dist = (R_mta_complex_dist_floor ^ -.5) * 10000"
save_raster R_mta_complex_inv_dist

normalize_raster R_mta_complex_inv_dist
save_raster R_mta_complex_inv_dist_norm

r.mapcalc <<EOF 
transit_index = (R_mta_mean_daily_entries_sum_capped_norm * .25) + \ 
	      	(R_mta_mean_daily_entries_sum_capped_norm * .25) + \
		(R_mta_complex_inv_dist_norm * .50)
EOF

normalize_raster transit_index
save_raster transit_index_norm

# Profitability
normalize_raster R_trips_per_day_per_dock
normalize_raster R_acs_sum
r.mapcalc "profitability_index = (R_trips_per_day_per_dock_norm * .75) + (R_acs_sum_norm * .25)"

normalize_raster profitability_index
save_raster profitability_index_norm

# Service expansion
normalize_raster R_potential_pop_service_area
normalize_raster R_potential_area_service_area
r.mapcalc "expansion_index = (R_potential_pop_service_area_norm * .5) + (R_potential_area_service_area_norm * .5)"

normalize_raster expansion_index
save_raster expansion_index_norm

# Safety
normalize_raster R_bike_route_dist
normalize_raster R_crashes_sum
r.mapcalc "danger_index = (R_bike_route_dist_norm *.5) + (R_crashes_sum_norm * .5)"
normalize_raster danger_index
# Calculate inverse of danger index so that high values = high suitability
r.mapcalc "safety_index_norm = 101 - danger_index_norm"
save_raster safety_index_norm

# Service improvement

# r.rescale doesn't work with very small values;
# manually scaling by * 100 first allows r.rescale to
# function as expected
r.mapcalc --overwrite <<EOF
R_gbfs_peak_bikes_eq0 = R_gbfs_peak_bikes_eq0 * 100
R_gbfs_offpeak_bikes_eq0 = R_gbfs_offpeak_bikes_eq0 * 100
R_gbfs_peak_docks_eq0 = R_gbfs_peak_docks_eq0 * 100
R_gbfs_offpeak_docks_eq0 = R_gbfs_offpeak_docks_eq0 * 100
EOF

normalize_raster R_gbfs_peak_bikes_eq0
save_raster R_gbfs_peak_bikes_eq0_norm

normalize_raster R_gbfs_peak_docks_eq0
save_raster R_gbfs_peak_docks_eq0_norm

normalize_raster R_gbfs_offpeak_bikes_eq0
save_raster R_gbfs_offpeak_bikes_eq0_norm

normalize_raster R_gbfs_offpeak_docks_eq0
save_raster R_gbfs_offpeak_docks_eq0_norm

normalize_raster R_docks_per_person_capped
save_raster R_docks_per_person_capped_norm

r.mapcalc <<EOF
service_improvement_index = (101 - R_docks_per_person_capped_norm) * .4 + \
R_acs_sum_norm * .10 + \			
R_gbfs_peak_bikes_eq0_norm * .125 + \
R_gbfs_offpeak_bikes_eq0_norm * .125 + \
R_gbfs_peak_docks_eq0_norm * .125 + \
R_gbfs_offpeak_docks_eq0_norm * .125
EOF

normalize_raster service_improvement_index
save_raster service_improvement_index_norm
