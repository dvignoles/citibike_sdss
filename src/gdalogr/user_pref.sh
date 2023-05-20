#!/usr/bin/bash

# create example user preference rasters

PROJECT_DIR="."
DATA_DIR=$PROJECT_DIR"/data/prepared"
OPENDATA="${DATA_DIR}/open_data.gpkg"

EMPTY=/tmp/empty.tif

# template raster tif in with appropriate origin, resolution, extent to copy
TEMPLATE="${1}"
OUTPUT_DIR="${2}"

if [ ! -d $OUTPUT_DIR ]; then
    mkdir -p $OUTPUT_DIR
fi

create_empty () {
    empty_tmp="/tmp/empty_tmp.tif"
    gdal_calc.py -A "${TEMPLATE}" --type=Int16 --calc="A*nan" --outfile="${empty_tmp}"
    gdal_translate -a_srs EPSG:2263 $empty_tmp $EMPTY
    rm $empty_tmp
}

not_manhattan () {
    create_empty
    gdal_rasterize -b 1 -burn "-50" -l nta $OPENDATA -where "borocode=1" $EMPTY
    gdal_rasterize -b 1 -burn "0" -l nta $OPENDATA -where "borocode=2" $EMPTY
    gdal_rasterize -b 1 -burn "0" -l nta $OPENDATA -where "borocode=3" $EMPTY
    gdal_rasterize -b 1 -burn "0" -l nta $OPENDATA -where "borocode=4" $EMPTY
    gdal_rasterize -b 1 -burn "0" -l nta $OPENDATA -where "borocode=5" $EMPTY
    mv $EMPTY "${OUTPUT_DIR}/userpref_not_manhattan.tif"
}

prefer_bq () {
    create_empty
    gdal_rasterize -b 1 -burn "0" -l nta $OPENDATA -where "borocode=1" $EMPTY
    gdal_rasterize -b 1 -burn "0" -l nta $OPENDATA -where "borocode=2" $EMPTY
    gdal_rasterize -b 1 -burn "50" -l nta $OPENDATA -where "borocode=3" $EMPTY
    gdal_rasterize -b 1 -burn "50" -l nta $OPENDATA -where "borocode=4" $EMPTY
    gdal_rasterize -b 1 -burn "0" -l nta $OPENDATA -where "borocode=5" $EMPTY
    mv $EMPTY "${OUTPUT_DIR}/userpref_prefer_bq.tif"
}

not_manhattan
prefer_bq