set search_path=public, madlib;
SELECT version() from gp_dist_random('gp_id');
SELECT madlib.version() from gp_dist_random('gp_id');
SELECT madlib_keras_fit_multiple_model(
    'cifar10_train_batched',
    'cifar10_model',
    'mst_table_cifar10',
    300,
    TRUE, -- use_gpus
    'cifar10_val_subset_of_train_batched',  -- validation table
    1  -- metrics compute freq
);
