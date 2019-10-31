DROP TABLE IF EXISTS normalized_out;
--CREATE TABLE normalized_out AS
--    SELECT madlib.array_scalar_mult( x, 1.0::REAL) FROM cifar_demo_train;

CREATE TABLE normalized_out_rows AS
    SELECT row_number() over() % 6 as buffer_id FROM cifar_demo_train;
    
--CREATE TABLE normalized_out_randomized AS SELECT * FROM normalized_out ORDER BY RANDOM();
    
    
    
--    DISTRIBUTED BY (buffer_id);
