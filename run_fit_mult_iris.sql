DROP TABLE IF EXISTS iris_model, iris_model_summary, iris_model_info; SELECT madlib.madlib_keras_fit_multiple_model('iris_data_packed', 'iris_model', 'mst_table', 1, FALSE);
