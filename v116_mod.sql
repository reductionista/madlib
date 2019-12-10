SET optimizer=off;
SET enable_hashagg=off;
DROP TABLE IF EXISTS cifar_demo_train_perf_output;
EXPLAIN ANALYZE CREATE TABLE cifar_demo_train_perf_output AS
      SELECT * FROM
      (
              SELECT madlib.agg_array_concat(
                              ARRAY[madlib_perm_normalized_redist_row_id.x_norm::REAL[]]) AS independent_var,
                     madlib.agg_array_concat(
                              ARRAY[madlib_perm_normalized_redist_row_id.y]) AS dependent_var,
                     (madlib_perm_normalized_redist_row_id.row_id/2500)::smallint AS buffer_id
              FROM madlib_perm_normalized_redist_row_id
              GROUP BY buffer_id
          ) b
      DISTRIBUTED BY (buffer_id)
