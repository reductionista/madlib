SELECT madlib.madlib_keras_fit('iris_train_packed', -- source table
	'iris_model', -- model output table
	 'model_arch_library',  -- model arch table
	1, -- model arch id
    $$ loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'] $$,  -- compile_params
    $$ batch_size=5, epochs=3 $$,  -- fit_params
    10     -- num_iterations 
);
