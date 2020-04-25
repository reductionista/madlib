SELECT version() from gp_dist_random('gp_id');
SELECT madlib.version() from gp_dist_random('gp_id');
DROP TABLE if exists places10_train_mult_model, places10_train_mult_model_summary, places10_train_mult_model_info;
SELECT madlib_keras_fit_multiple_model(
    'places10_train_batched',
    'places10_train_mult_model',
    'mst_table',
    1,
    TRUE
);
