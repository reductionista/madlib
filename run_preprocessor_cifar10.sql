DROP TABLE IF EXISTS cifar_demo_train_perf_output, cifar_demo_train_perf_output_summary;
SET enable_hashagg=off;
SET optimizer=off;
SELECT madlib.training_preprocessor_dl('cifar_demo_train', 'cifar_demo_train_perf_output', 'y', 'x');
