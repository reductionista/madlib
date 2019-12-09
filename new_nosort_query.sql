DROP TABLE IF EXISTS madlib_perm_normalized;
CREATE TABLE madlib_perm_normalized AS
            SELECT x::REAL[] AS x_norm,
                ARRAY[(y) IS NOT DISTINCT FROM 0, (y) IS NOT DISTINCT FROM 1, (y) IS NOT DISTINCT FROM 2, (y) IS NOT DISTINCT FROM 3, (y) IS NOT DISTINCT FROM 4, (y) IS NOT DISTINCT FROM 5, (y) IS NOT DISTINCT FROM 6, (y) IS NOT DISTINCT FROM 7, (y) IS NOT DISTINCT FROM 8, (y) IS NOT DISTINCT FROM 9]::INTEGER[]::SMALLINT[] AS y,
                id
            FROM cifar_demo_train;

DROP TABLE IF EXISTS madlib_perm_normalized_redist;
CREATE TABLE madlib_perm_normalized_redist AS SELECT n.x_norm, n.y, d.__dist_key__,
                (ROW_NUMBER() OVER(PARTITION BY d.__dist_key__))::INTEGER as slot_id FROM
                    madlib_perm_normalized AS n JOIN perm_dist_key AS d
                        ON (n.id % 3) = d.gp_segment_id
                             ORDER BY RANDOM()   DISTRIBUTED BY (__dist_key__);

SET optimizer=off;
SET enable_hashagg=off;

EXPLAIN ANALYZE CREATE TABLE madlib_perm_batched_table AS
                    SELECT (gp_segment_id * 3 + slot_id % 1.0) AS buffer_id,
                        __dist_key__,
                        madlib.agg_array_concat(
                            ARRAY[n.x_norm::REAL[]]) AS independent_var,
                        madlib.agg_array_concat(
                            ARRAY[n.y]) AS dependent_var,
                        COUNT(*) AS count
                            FROM madlib_perm_normalized_redist AS n
                                GROUP BY __dist_key__, buffer_id
                                     DISTRIBUTED BY (__dist_key__);
