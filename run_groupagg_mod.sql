set statement_mem='2GB' ; set optimizer to off; set enable_hashagg to off; select array_dims(madlib.my_agg_array_concat(ARRAY[x])), id % 75 as buffer_id from places10_train group by buffer_id;
set statement_mem='2GB' ; set optimizer to off; set enable_hashagg to off; select array_dims(madlib.my_agg_array_concat(ARRAY[x])), id % 750 as buffer_id from places100_train group by buffer_id;
\c places
set statement_mem='2GB' ; set optimizer to off; set enable_hashagg to off; select array_dims(madlib.my_agg_array_concat(ARRAY[x])), id / 270 as buffer_id from places365 group by buffer_id;
