--    DROP TABLE IF EXISTS places100_train_gpdbagg_out;
SET optimizer=off;
--SET gp_enable_multiphase_agg=off;  -- uncomment for single phase HashAgg (only affects Planner)
SET enable_hashagg=off;  -- uncomment for GroupAgg (only affects Planner)
           CREATE TABLE places100_train_gpdbagg_out AS
            SELECT __dist_key__ ,
                   madlib.convert_array_to_bytea(independent_var) AS independent_var,
                   madlib.convert_array_to_bytea(dependent_var) AS dependent_var,
                   ARRAY[count,256,256,3]::SMALLINT[] AS independent_var_shape,
                   ARRAY[count,100]::SMALLINT[] AS dependent_var_shape,
                   buffer_id
            FROM
            (
                SELECT
                    madlib.gpdb_agg_array_concat(
                        ARRAY[madlib_places100_normalized.x_norm::REAL[]]) AS independent_var,
                    madlib.gpdb_agg_array_concat(
                        ARRAY[madlib_places100_normalized.y]) AS dependent_var,
                    floor(madlib_places100_normalized.row_id/381)::smallint AS buffer_id,
                    -- shouldn't need floor as long as row_id is BIGINT.  We do need if it's NUMERIC
                    count(*) AS count
                FROM madlib_places100_normalized
                GROUP BY buffer_id
            ) b
            JOIN madlib_perm_dist_key ON (b.buffer_id%20)= madlib_perm_dist_key.id
             DISTRIBUTED BY (__dist_key__)
