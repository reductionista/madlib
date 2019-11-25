select length(string_agg(array_to_string(x,','))) from cifar_demo_train group by (id % 2)
