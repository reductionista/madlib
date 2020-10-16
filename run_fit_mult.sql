SELECT version() from gp_dist_random('gp_id');
SELECT madlib.version() from gp_dist_random('gp_id');
DROP TABLE if exists places10_train_mult_model_fromscratch, places10_train_mult_model_fromscratch_summary, places10_train_mult_model_fromscratch_info;
SELECT madlib_keras_fit_multiple_model(
    'places10_train_batched',
    'places10_train_mult_model_fromscratch',
    'mst_table_places10',
    1,
    TRUE -- use_gpus
);
