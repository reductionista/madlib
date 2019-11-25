EXPLAIN CREATE TABLE places10_train_batched_2slices AS
            SELECT madlib.convert_array_to_bytea(independent_var) AS independent_var,
                   madlib.convert_array_to_bytea(dependent_var) AS dependent_var,
                   ARRAY[count,256,256,3]::SMALLINT[] AS independent_var_shape,
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
                    SELECT ROW_NUMBER() OVER() % 131 AS buffer_index,
                        x_norm, y FROM madlib_places10_normalized
                ) n JOIN madlib_perm_dist_key d
                ON n.buffer_index = 20 * d.slot_id + d.gp_segment_id
                GROUP BY buffer_id
            ) b DISTRIBUTED BY (buffer_id);
