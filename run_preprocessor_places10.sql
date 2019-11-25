DROP TABLE IF EXISTS places10_train_perf_output, places10_train_perf_output_summary;
SET enable_hashagg=off;
SET optimizer=off;
SELECT madlib.training_preprocessor_dl('places10_train', 'places10_train_perf_output', 'y', 'x');  -- default buf size 382
SELECT madlib.training_preprocessor_dl('places10_train', 'places10_train_perf_output', 'y', 'x', 194); -- half buf size
SELECT madlib.training_preprocessor_dl('places10_train', 'places10_train_perf_output', 'y', 'x', 129); -- 1/3 buf size
