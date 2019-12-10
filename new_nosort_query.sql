DROP TABLE IF EXISTS madlib_perm_normalized;
CREATE TABLE madlib_perm_normalized AS
            SELECT x::REAL[] AS x_norm,
                ARRAY[(y) IS NOT DISTINCT FROM 0, (y) IS NOT DISTINCT FROM 1, (y) IS NOT DISTINCT FROM 2, (y) IS NOT DISTINCT FROM 3, (y) IS NOT DISTINCT FROM 4, (y) IS NOT DISTINCT FROM 5, (y) IS NOT DISTINCT FROM 6, (y) IS NOT DISTINCT FROM 7, (y) IS NOT DISTINCT FROM 8, (y) IS NOT DISTINCT FROM 9]::INTEGER[]::SMALLINT[] AS y
            FROM cifar_demo_train ORDER BY RANDOM()
                DISTRIBUTED RANDOMLY;

DROP TABLE IF EXISTS madlib_perm_normalized_redist;
CREATE TABLE madlib_perm_normalized_redist AS SELECT n.x_norm, n.y,
        d.__dist_key__ AS row_dist_key,
        ((ROW_NUMBER() OVER(PARTITION BY d.__dist_key__)) - 1)::SMALLINT as slot_id FROM
                madlib_perm_normalized AS n JOIN perm_dist_key d
                    ON n.gp_segment_id = d.gp_segment_id
                    DISTRIBUTED BY (row_dist_key);

DROP TABLE IF EXISTS madlib_perm_normalized_redist_buffer_id;
CREATE TABLE madlib_perm_normalized_redist_buffer_id AS
    SELECT row_dist_key, x_norm, y, ((slot_id * 20 + gp_segment_id) / 2500)::SMALLINT AS buffer_id
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
