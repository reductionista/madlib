#!/bin/bash
DB="dbscan"
CWD=$(pwd)
psql $DB -c "DROP TABLE IF EXISTS input_blobs_template; CREATE TABLE blobs_template AS (id INTEGER, point DOUBLE PRECISION[], madlib_cluster, sklearn_cluster, true_cluster)"
for blobfile in blobs/*.csv
do
    tbl_name=$(basename $blobfile .csv)
    psql $DB -c "CREATE TABLE $tbl_name LIKE blobs_template"
    psql $DB -c "COPY $tbl_name FROM $CWD/blobs/$blobfile CSV HEADER"
done
