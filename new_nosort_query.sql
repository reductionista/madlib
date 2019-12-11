DROP TABLE IF EXISTS madlib_perm_normalized;
EXPLAIN CREATE TABLE madlib_perm_normalized AS
            SELECT id,
            d.__dist_key__ AS row_dist_key,
            x::REAL[] AS x_norm,
                ARRAY[(y) IS NOT DISTINCT FROM 0, (y) IS NOT DISTINCT FROM 1, (y) IS NOT DISTINCT FROM 2, (y) IS NOT DISTINCT FROM 3, (y) IS NOT DISTINCT FROM 4, (y) IS NOT DISTINCT FROM 5, (y) IS NOT DISTINCT FROM 6, (y) IS NOT DISTINCT FROM 7, (y) IS NOT DISTINCT FROM 8, (y) IS NOT DISTINCT FROM 9]::INTEGER[]::SMALLINT[] AS y,
            ((ROW_NUMBER() OVER(PARTITION BY d.__dist_key__ ORDER BY RANDOM())) - 1)::INTEGER as slot_id
            FROM cifar_demo_train JOIN perm_dist_key AS d ON id % 20 = d.gp_segment_id
                DISTRIBUTED BY (row_dist_key);

DROP TABLE IF EXISTS madlib_perm_normalized_redist_buffer_id;
CREATE TABLE madlib_perm_normalized_redist_buffer_id AS
    SELECT row_dist_key, x_norm, y, ((slot_id * 20 + gp_segment_id)::INTEGER / 2500) AS buffer_id
        FROM madlib_perm_normalized_redist ORDER BY buffer_id
            DISTRIBUTED BY (row_dist_key);

SET optimizer=off;
SET enable_hashagg=off;

DROP TABLE IF EXISTS madlib_perm_batched_table;

CREATE TABLE madlib_perm_batched_table AS
    SELECT d.__dist_key__, a.* FROM
                    (SELECT n.buffer_id,
                        madlib.agg_array_concat(
                            ARRAY[n.x_norm::REAL[]]) AS independent_var,
                        madlib.agg_array_concat(
                            ARRAY[n.y]) AS dependent_var,
                        COUNT(*) AS count
                            FROM madlib_perm_normalized_redist_buffer_id AS n
                                GROUP BY buffer_id) a
                                    JOIN perm_dist_key AS d
                                        ON (a.buffer_id % 20) = d.gp_segment_id
                                            DISTRIBUTED BY (__dist_key__);
