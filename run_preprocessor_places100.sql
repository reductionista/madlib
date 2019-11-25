DROP TABLE IF EXISTS places100_train_perf_output, places100_train_perf_output_summary;
SET enable_hashagg=off;
SET optimizer=off; 
--SELECT madlib.training_preprocessor_dl('places100_train', 'places100_train_perf_output', 'y', 'x'); -- default buf size 388
SELECT madlib.training_preprocessor_dl('places100_train', 'places100_train_perf_output', 'y', 'x', 194);  -- Half buffer size
SELECT madlib.training_preprocessor_dl('places100_train', 'places100_train_perf_output', 'y', 'x', 129);  -- 1/3 buffer size
