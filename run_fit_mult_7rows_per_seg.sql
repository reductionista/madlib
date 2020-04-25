set search_path=manual, gpdb6_input, madlib;
SELECT version() from gp_dist_random('gp_id');
SELECT madlib.version() from gp_dist_random('gp_id');
DROP TABLE if exists places10_7rows_per_seg_mult_model, places10_7rows_per_seg_mult_model_summary, places10_7rows_per_seg_mult_model_info;
SELECT madlib_keras_fit_multiple_model(
    'places10_batched_7rows_per_seg',
    'places10_7rows_per_seg_mult_model',
    'mst_table',
    3,
    TRUE
);
