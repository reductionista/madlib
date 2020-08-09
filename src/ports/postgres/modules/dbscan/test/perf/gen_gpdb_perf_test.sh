#!/bin/bash
DB="dbscan"
CWD=$(pwd)
echo "\timing on"
for blobfile in blobs/*.csv
do
    tbl_name="$(basename $blobfile .csv)"
    out_tbl_name=${tbl_name/input_/output_}
    #psql $DB -c "SELECT madlib.dbscan('$tbl_name', '$out_tbl_name', 'id', 'point', 0.3, 10, 'squared_dist_norm2', 'kd_tree', 3)"
    #psql $DB -c "COPY $tbl_name TO '$CWD/${out_tbl_name}.csv' CSV HEADER"
    echo "SELECT madlib.dbscan('$tbl_name', '$out_tbl_name', 'id', 'point', 0.3, 10, 'squared_dist_norm2', 'kd_tree', 3)"
#    psql $DB -c "SELECT madlib.dbscan('$tbl_name', '$out_tbl_name', 'id', 'point', 0.3, 10, 'squared_dist_norm2', 'kd_tree', 3)"
    echo "COPY $out_tbl_name TO '$CWD/blobs/${out_tbl_name}.csv' CSV HEADER"
#    psql $DB -c "COPY $out_tbl_name TO '$CWD/blobs/${out_tbl_name}.csv' CSV HEADER"
done
