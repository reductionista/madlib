            CREATE TEMP TABLE __madlib_temp_normalized84899197_1574454277_4520195__ AS
            SELECT x::REAL[] AS x_norm,
                ARRAY[(y) IS NOT DISTINCT FROM 0, (y) IS NOT DISTINCT FROM 1, (y) IS NOT DISTINCT FROM 2, (y) IS NOT DISTINCT FROM 3, (y) IS NOT DISTINCT FROM 4, (y) IS NOT DISTINCT FROM 5, (y) IS NOT DISTINCT FROM 6, (y) IS NOT DISTINCT FROM 7, (y) IS NOT DISTINCT FROM 8, (y) IS NOT DISTINCT FROM 9]::INTEGER[]::SMALLINT[] AS y
            FROM cifar_demo_train  ORDER BY RANDOM();

                    CREATE TEMP TABLE __madlib_temp_series60699754_1574454279_15966240__ AS
                        SELECT generate_series(0, 999999) __dist_key__
                        DISTRIBUTED BY (__dist_key__);
         
                    CREATE TEMP TABLE __madlib_temp_dist_key54013556_1574454279_49608103__ AS
                        SELECT __dist_key__ AS buffer_id, ROW_NUMBER() OVER(PARTITION BY gp_segment_id) AS slot_id
                        FROM __madlib_temp_series60699754_1574454279_15966240__ DISTRIBUTED BY (buffer_id);
SET optimizer=off;
--SET enable_hashagg=off;
SET gp_enable_multiphase_agg=off;
EXPLAIN     CREATE TABLE cifar_demo_batched AS
            SELECT madlib.convert_array_to_bytea(independent_var) AS independent_var,
                   madlib.convert_array_to_bytea(dependent_var) AS dependent_var,
                   ARRAY[count,32,32,3]::SMALLINT[] AS independent_var_shape,
                   ARRAY[count,10]::SMALLINT[] AS dependent_var_shape,
                   buffer_id
            FROM
            (
                SELECT
                    madlib.gpdb_agg_array_concat(
                        ARRAY[n.x_norm::REAL[]]) AS independent_var,
                    madlib.gpdb_agg_array_concat(
                        ARRAY[n.y]) AS dependent_var,
                    d.buffer_id AS buffer_id,
                    count(*) AS count
                FROM (
                    SELECT (ROW_NUMBER() OVER())::INTEGER % 3.0 AS buffer_index,
                        x_norm, y FROM __madlib_temp_normalized84899197_1574454277_4520195__
                ) n JOIN __madlib_temp_dist_key54013556_1574454279_49608103__ d
                ON n.buffer_index = 3 * d.slot_id + d.gp_segment_id
                GROUP BY buffer_id
            ) b
             DISTRIBUTED BY (buffer_id);
