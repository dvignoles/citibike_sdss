#!/bin/bash

function PrintUsage () {
    echo "Usage ${0##*/} [weights] <output>.tif"
    echo "      -t,  --transit <weight>"
    echo "      -s,  --safety <weight>"
    echo "      -i,  --service <weight>"
    echo "      -p,  --profit <weight>"
    echo "      -e,  --expansion <weight>"
    exit 1
}

TRANSIT=""
SAFETY=""
SERVICE=""
PROFIT=""
EXPANSION=""
OUTPUT_TIF=""

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
    (-)
        OUTPUT_TIF="${1}"
        shift
    ;;
    (*)
        OUTPUT_TIF="${1}"
        shift
    ;;
    esac
done

if [ "${OUTPUT_TIF}" == "" ]; then PrintUsage; fi

mc=$(cat <<EOF
equal_weight_suitability = \
service_improvement_index_norm * $SERVICE + \
transit_index_norm * $TRANSIT + \
expansion_index_norm * $EXPANSION + \
profitability_index_norm * $PROFIT + \
safety_index_norm * $SAFETY
EOF
)

echo $mc