#! /usr/bin/bash

######################
# CLI Interface Start#
######################

function PrintUsage () {
    echo "Usage ${0##*/} [weights] <output_directory>"
    echo "      -t,  --transit   <weight> [REQUIRED]"
    echo "      -s,  --safety    <weight> [REQUIRED]"
    echo "      -i,  --service   <weight> [REQUIRED]"
    echo "      -p,  --profit    <weight> [REQUIRED]"
    echo "      -e,  --expansion <weight> [REQUIRED]"
    echo "      -w,  --walkradi  <feet>   [OPTIONAL] DEFAULT=2640"
    echo "      -m,  --streetmax <feet>   [OPTIONAL] DEFAULT=60"
    echo "      -c,  --cbmin     <feet>   [OPTIONAL] DEFAULT=100"
    echo "      -u,  --userpref  <tif>    [OPTIONAL]"
    exit 1
}

TRANSIT=""
SAFETY=""
SERVICE=""
PROFIT=""
EXPANSION=""
OUTPUT_DIR=""
USER_PREF=""

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
    (-t|--transit)
        shift
        if [ "${1}" == "" ]; then PrintUsage; fi
        if [[ "${1}" =~ ^[0-9]+(\.[0-9]+)?$ ]]
        then
            TRANSIT="${1}"
        else
            PrintUsage
        fi
        shift
    ;;
    (-s|--safety)
        shift
        if [ "${1}" == "" ]; then PrintUsage; fi
        if [[ "${1}" =~ ^[0-9]+(\.[0-9]+)?$ ]]
        then
            SAFETY="${1}"
        else
            PrintUsage
        fi
        shift
    ;;
    (-i|--service)
        shift
        if [ "${1}" == "" ]; then PrintUsage; fi
        if [[ "${1}" =~ ^[0-9]+(\.[0-9]+)?$ ]]
        then
            SERVICE="${1}"
        else
            PrintUsage
        fi
        shift
    ;;
    (-e|--expansion)
        shift
        if [ "${1}" == "" ]; then PrintUsage; fi
        if [[ "${1}" =~ ^[0-9]+(\.[0-9]+)?$ ]]
        then
            EXPANSION="${1}"
        else
            PrintUsage
        fi
        shift
    ;;
    (-p|--profit)
        shift
        if [ "${1}" == "" ]; then PrintUsage; fi
        if [[ "${1}" =~ ^[0-9]+(\.[0-9]+)?$ ]]
        then
            PROFIT="${1}"
        else
            PrintUsage
        fi
        shift
    ;;
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
    (-u|--userpref)
        shift
        if [ "${1}" == "" ]; then PrintUsage; fi
        USER_PREF="${1}"
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

if [[ -z $TRANSIT ]] || [[ -z $SERVICE ]] || [[ -z $SAFETY ]] || [[ -z $EXPANSION ]] || [[ -z $PROFIT ]]; then
	echo "Must supply all weights"
	PrintUsage
	exit 1
fi

if [ "${OUTPUT_DIR}" == "" ]; then 
	OUTPUT_DIR=$(pwd)
fi;

if [ $(( $(( $TRANSIT + $SAFETY + $SERVICE + $EXPANSION + $PROFIT )) % 10 )) != 0 ]; then
	echo "Weights must be integers adding up to multiple of 10"
	exit 1
fi

SCENARIO="index_tra${TRANSIT}_saf${SAFETY}_ser${SERVICE}_exp${EXPANSION}_pro${PROFIT}"
SCENARIO_NORM="${SCENARIO}_norm"
SCENARIO_CONS="${SCENARIO}_constrained"
SCENARIO_CONS_NORM="${SCENARIO}_constrained_norm"

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

load_intermediate service_improvement_index_norm.tif service_improvement_index_norm
load_intermediate transit_index_norm.tif transit_index_norm
load_intermediate expansion_index_norm.tif expansion_index_norm
load_intermediate profitability_index_norm.tif profitability_index_norm
load_intermediate safety_index_norm.tif safety_index_norm
load_intermediate R_constraint.tif R_constraint

INDEX_WEIGHT_EXPR=$(cat <<EOF
${SCENARIO} = \
service_improvement_index_norm * 0.${SERVICE} + \
transit_index_norm * 0.${TRANSIT} + \
expansion_index_norm * 0.${EXPANSION} + \
profitability_index_norm * 0.${PROFIT} + \
safety_index_norm * 0.${SAFETY}
EOF
)

r.mapcalc "${INDEX_WEIGHT_EXPR}"

save_raster $SCENARIO
normalize_raster $SCENARIO
save_raster $SCENARIO_NORM

r.mapcalc "${SCENARIO_CONS} = ${SCENARIO_NORM} * R_constraint"

save_raster $SCENARIO_CONS
normalize_raster $SCENARIO_CONS
save_raster $SCENARIO_CONS_NORM

if [ ! -z "$USER_PREF" ]; then
    echo "modifying with user pref"
    PREF_NAME=$(basename ${USER_PREF} .tif)

    load_raster $USER_PREF user_pref_mod

    r.mapcalc "${SCENARIO_NORM}+${PREF_NAME}} = ${SCENARIO_NORM} + user_pref_mod"
    save_raster "${SCENARIO_NORM}+${PREF_NAME}"

    r.mapcalc "${SCENARIO_CONS_NORM}+${PREF_NAME} = ${SCENARIO_CONS_NORM} + user_pref_mod"
    save_raster "${SCENARIO_CONS_NORM}+${PREF_NAME}"
fi