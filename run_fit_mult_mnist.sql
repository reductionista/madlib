DROP TABLE IF EXISTS mnist_model, mnist_model_summary, mnist_model_info;
SELECT madlib.madlib_keras_fit_multiple_model('mnist_train_batched', 'mnist_model', 'mst_table_mnist', 1, FALSE);
