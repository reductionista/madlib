drop table if exists places100_train_gpdbagg_out;
drop table if exists places100_train_gpdbagg_out_summary ;
select madlib.training_preprocessor_dl('places100_train', 'places100_train_gpdbagg_out', 'y', 'x');

EXPLAIN     CREATE TABLE madlib_places100_normalized AS
            SELECT x::REAL[] AS x_norm,
                ARRAY[(y) IS NOT DISTINCT FROM 'airfield', (y) IS NOT DISTINCT FROM 'airplane_cabin', (y) IS NOT DISTINCT FROM 'airport_terminal', (y) IS NOT DISTINCT FROM 'alcove', (y) IS NOT DISTINCT FROM 'alley', (y) IS NOT DISTINCT FROM 'amphitheater', (y) IS NOT DISTINCT FROM 'amusement_arcade', (y) IS NOT DISTINCT FROM 'amusement_park', (y) IS NOT DISTINCT FROM 'apartment_building-outdoor', (y) IS NOT DISTINCT FROM 'aquarium']::INTEGER[]::SMALLINT[] AS y,
                row_number() over() AS row_id
            FROM places10_train  ORDER BY RANDOM();

-- SET optimizer='off';
SET gp_enable_multiphase_agg='off';
SET optimizer_force_multistage_agg='false';

--EXPLAIN CREATE TABLE places100_train_gpdbagg_out AS
        CREATE TABLE places100_train_gpdbagg_out AS
        SELECT __dist_key__ ,
               madlib.convert_array_to_bytea(independent_var) AS independent_var,
               madlib.convert_array_to_bytea(dependent_var) AS dependent_var,
               ARRAY[count,256,256,3]::SMALLINT[] AS independent_var_shape,
               ARRAY[count,10]::SMALLINT[] AS dependent_var_shape,
               buffer_id
        FROM
        (
            SELECT
                madlib.gpdb_agg_array_concat(
                    ARRAY[madlib_places100_normalized.x_norm::REAL[]]) AS independent_var,
                madlib.gpdb_agg_array_concat(
                    ARRAY[madlib_places100_normalized.y]) AS dependent_var,
                (madlib_places100_normalized.row_id%131.0)::smallint AS buffer_id,
                count(*) AS count
            FROM madlib_places100_normalized
            GROUP BY buffer_id
        ) b
        JOIN madlib_perm_dist_key ON (b.buffer_id % 20) = madlib_perm_dist_key.id
         DISTRIBUTED BY (__dist_key__);
