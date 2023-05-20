#! /usr/bin/bash

function PrintUsage () {
    echo "Usage ${0##*/} [weights] <output_directory>"
    echo "      -t,  --transit   <weight> [REQUIRED]"
    echo "      -s,  --safety    <weight> [REQUIRED]"
    echo "      -i,  --service   <weight> [REQUIRED]"
    echo "      -p,  --profit    <weight> [REQUIRED]"
    echo "      -e,  --expansion <weight> [REQUIRED]"
    echo "      -u,  --userpref  <tif>    [OPTIONAL]"
    echo "      -r,  --userwght  <weight> [OPTIONAL] DEFAULT=100"
    echo "           --help"
    exit 1
}

TRANSIT=""
SAFETY=""
SERVICE=""
PROFIT=""
EXPANSION=""
OUTPUT_DIR=""
USER_PREF=""
USER_WEIGHT=100

if [ "${1}" == "" ]; then 
    PrintUsage
fi

while [ "${1}" != "" ]
do
    case "${1}" in 
    (--help)
        PrintUsage
        shift
    ;;
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
    (-u|--userpref)
        shift
        if [ "${1}" == "" ]; then PrintUsage; fi
        USER_PREF="${1}"
        shift
    ;;
    (-r|--userwght)
        shift
        if [ "${1}" == "" ]; then PrintUsage; fi
        if [[ "${1}" =~ ^[0-9]+$ ]]
        then
            USER_WEIGHT="${1}"
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

r.mapcalc --overwrite <<EOF
${SCENARIO} = \
service_improvement_index_norm * 0.${SERVICE} + \
transit_index_norm * 0.${TRANSIT} + \
expansion_index_norm * 0.${EXPANSION} + \
profitability_index_norm * 0.${PROFIT} + \
safety_index_norm * 0.${SAFETY}
EOF

save_raster $SCENARIO
normalize_raster $SCENARIO
save_raster "${SCENARIO}_norm"

r.mapcalc --overwrite "${SCENARIO}_cons = ${SCENARIO}_norm * R_constraint"

save_raster "${SCENARIO}_cons"
normalize_raster "${SCENARIO}_cons"
save_raster "${SCENARIO}_cons_norm"

# Adjust with weighted user preference
# adjustments to below zero are set to the floor of zero, then the final output is re-normalized

if [ ! -z "$USER_PREF" ]; then
    echo "modifying with user pref"
    PREF_NAME="$(basename ${USER_PREF} .tif)$USER_WEIGHT"

    echo $USER_PREF $USER_WEIGHT 
    load_raster $USER_PREF user_pref_mod

    INDEX_NORM="${SCENARIO}_norm_${PREF_NAME}"
    INDEX_NORM_S1="${INDEX_NORM}_step1"

r.mapcalc --overwrite <<EOF 
    ${INDEX_NORM_S1} = ${SCENARIO}_norm + (${USER_WEIGHT}.0 / 100.0) * user_pref_mod
    ${INDEX_NORM} = if( ${INDEX_NORM_S1} <= 0, 0, ${INDEX_NORM_S1} )
EOF
    normalize_raster "${INDEX_NORM}"
    save_raster "${INDEX_NORM}_norm"

    INDEX_CONS_NORM="${SCENARIO}_cons_norm_${PREF_NAME}"
    INDEX_CONS_NORM_S1="${INDEX_CONS_NORM}_step1"

r.mapcalc --overwrite <<EOF
    ${INDEX_CONS_NORM_S1} = ${SCENARIO}_cons_norm + (${USER_WEIGHT} / 100) * user_pref_mod
    ${INDEX_CONS_NORM} = if ( ${INDEX_CONS_NORM_S1} <= 0, 0, ${INDEX_CONS_NORM_S1} )
EOF

    normalize_raster "${INDEX_CONS_NORM}"
    save_raster "${INDEX_CONS_NORM}_norm"
fi