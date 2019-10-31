        set optimizer=off;
        set gp_enable_multiphase_agg=off;
        set gp_autostats_mode_in_functions='ON_NO_STATS';

            CREATE TABLE __madlib_temp_normalized__debug__ AS
            SELECT madlib.array_scalar_mult(
                x::REAL[],
                (1/1.0)::REAL) AS x_norm,
                ARRAY[(y) IS NOT DISTINCT FROM 'airfield', (y) IS NOT DISTINCT FROM 'airplane_cabin', (y) IS NOT DISTINCT FROM 'airport_terminal', (y) IS NOT DISTINCT FROM 'alcove', (y) IS NOT DISTINCT FROM 'alley', (y) IS NOT DISTINCT FROM 'amphitheater', (y) IS NOT DISTINCT FROM 'amusement_arcade', (y) IS NOT DISTINCT FROM 'amusement_park', (y) IS NOT DISTINCT FROM 'apartment_building-outdoor', (y) IS NOT DISTINCT FROM 'aquarium']::INTEGER[]::SMALLINT[] AS y,
                row_number() over() AS row_id
            FROM places10_train ORDER BY RANDOM();

            CREATE TABLE __madlib_temp_series__debug__
                AS
                SELECT generate_series(0, 999999) __dist_key__
                DISTRIBUTED BY (__dist_key__);

            CREATE TABLE __madlib_temp_dist_key__debug__ AS
                    SELECT gp_segment_id AS id, min(__dist_key__) AS __dist_key__
                    FROM __madlib_temp_series__debug__
                    GROUP BY gp_segment_id;

            CREATE TABLE places10_train_myagg_out AS
            SELECT __dist_key__ ,
                   madlib.convert_array_to_bytea(independent_var) AS independent_var,
                   madlib.convert_array_to_bytea(dependent_var) AS dependent_var,
                   ARRAY[count,256,256,3]::SMALLINT[] AS independent_var_shape,
                   ARRAY[count,10]::SMALLINT[] AS dependent_var_shape,
                   buffer_id
            FROM
            (
                SELECT
                    madlib.my_agg_array_concat(
                        ARRAY[__madlib_temp_normalized__debug__.x_norm::REAL[]]) AS independent_var,
                    madlib.my_agg_array_concat(
                        ARRAY[__madlib_temp_normalized__debug__.y]) AS dependent_var,
                    (__madlib_temp_normalized__debug__.row_id%131.0)::smallint AS buffer_id,
                    count(*) AS count
                FROM __madlib_temp_normalized__debug__
                GROUP BY buffer_id
            ) b
            JOIN __madlib_temp_dist_key__debug__ ON (b.buffer_id%20)= __madlib_temp_dist_key__debug__.id
             DISTRIBUTED BY (__dist_key__);
