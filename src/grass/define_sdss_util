#! /usr/bin/bash

load_layer () {
    filename=$1
    layer_to_import=$2
    new_layer_name=$3
    full_path="${DATA_DIR}${filename}"
    v.import -o $full_path snap=1e-05 layer=$layer_to_import output=$new_layer_name
}

save_vector () {
    input_v=$1
    output_path="${OUTPUT_DIR}${input_v}.gpkg"
    v.out.ogr input=$1 output=$output_path --overwrite
    }

save_raster () {
    input_raster=$1
    output_path="${OUTPUT_DIR}${input_raster}.tif"
    r.out.gdal input=$input_raster output=$output_path --overwrite
}

rasterize_vector () {
    # Create a new raster layer from a vector.
    input_v_name=$1
    input_v_type=$2
    attribute_column=$3
    output_r_name=$4

    v.to.rast input=$input_v_name type=$input_v_type output=$output_r_name \
	      use=attr attribute_column=$attribute_column --overwrite
}

bin_vect_to_rast () {

    input_v=$1
    input_v_column=$2
    output_r=$3
    my_method=$4

    v.out.ascii input=$input_v column=$input_v_column | \
	r.in.xyz input=- z=4 output=$output_r method=$my_method
}

convert_char_to_int () {
    # Replace attribute table char column with int column with same values.
    
    vector_map=$1
    char_column=$2
    int_column="${char_column}_num"
    # Add a new int column and update its values from the char column
    v.db.addcolumn $vector_map column="${int_column} integer"
    v.db.update $vector_map column=$int_column query_col=$char_column
    # Drop the char column and rename the int column to the original name
    v.db.dropcolumn map=$vector_map columns=$char_column
    v.db.renamecolumn map=$vector_map column=$int_column,$char_column
    }

convert_char_to_real () {
    # Replace attribute table char column with int column with same values.
    
    vector_map=$1
    char_column=$2
    int_column="${char_column}_num"
    # Add a new int column and update its values from the char column
    v.db.addcolumn $vector_map column="${int_column} real"
    v.db.update $vector_map column=$int_column query_col=$char_column
    # Drop the char column and rename the int column to the original name
    v.db.dropcolumn map=$vector_map columns=$char_column
    v.db.renamecolumn map=$vector_map column=$int_column,$char_column
    }

reclass_dist_constraint () {
    input_raster=$1
    min_dist=$2
    max_dist=$3
    output_raster=$4
    if (( $min_dist==0 )); then
       echo "Zero"
       rules="0 thru $((max_dist - 1)) = 1
       ${max_dist} thru 9999999 = 0"
    else
       echo "Not zero"
       rules="0 thru $((min_dist - 1)) = 0
       ${min_dist} thru ${max_dist} = 1
       $((max_dist + 1)) thru 9999999 = 0"
    fi
    echo "Rules: ${rules}"
    echo "$rules" | r.reclass input=$input_raster output=$output_raster rules=- --overwrite
}

sum_rast_in_walk_radius () {
    input_r=$1
    output_r=$2
    neighborhood_size=$((($WALK_RADIUS * 2) / $MEDIUM_RES))
    # Neighborhood size must be odd
    if [ $((neighborhood_size%2)) -eq 0 ]
    then
	neighborhood_size="$((neighborhood_size + 1))"
    fi
    r.neighbors -c input=$input_r method=sum output=$output_r size=$neighborhood_size
    }

normalize_raster () {
    input_r=$1
    r.rescale input=$input_r to=1,100 output="${input_r}_norm"
}
