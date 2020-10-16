set search_path=udf_output, gpdb6_input, madlib;
SELECT version() from gp_dist_random('gp_id');
SELECT madlib.version() from gp_dist_random('gp_id');
SELECT madlib_keras_fit_multiple_model(
    'places10_train_batched',
    'places10_train_mult_model',
    'mst_table_places10',
    1,
    TRUE, -- use_gpus
    NULL,  -- validation table
    NULL,  -- metrics compute freq
    TRUE -- warm start
);
