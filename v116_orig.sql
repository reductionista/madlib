SET optimizer=off;
SET enable_hashagg=off;
DROP TABLE cifar_demo_train_perf_output;
EXPLAIN CREATE TABLE cifar_demo_train_perf_output AS
      SELECT * FROM
      (
              SELECT madlib.agg_array_concat(
                              ARRAY[__madlib_temp_normalized11244264_1575933768_15466608__.x_norm::REAL[]]) AS independent_var,
                     madlib.agg_array_concat(
                              ARRAY[__madlib_temp_normalized11244264_1575933768_15466608__.y]) AS dependent_var,
                     (__madlib_temp_normalized11244264_1575933768_15466608__.row_id%20.0)::smallint AS buffer_id
              FROM __madlib_temp_normalized11244264_1575933768_15466608__
              GROUP BY buffer_id
          ) b
      DISTRIBUTED BY (buffer_id)
