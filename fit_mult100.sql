set search_path=udf_output, gpdb6_input, madlib;
SELECT version() from gp_dist_random('gp_id');
SELECT madlib.version() from gp_dist_random('gp_id');
DROP TABLE if exists places100_train_mult_model, places100_train_mult_model_summary, places100_train_mult_model_info;
SELECT madlib_keras_fit_multiple_model(
    'places100_train_batched',
    'places100_train_mult_model',
    'mst_table',
    1,
    TRUE
);
