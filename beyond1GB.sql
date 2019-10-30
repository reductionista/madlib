
-- Runs on segments
DROP function IF EXISTS my_segment_udf(x real[], y integer);
CREATE OR REPLACE FUNCTION
my_segment_udf(
    x REAL[],
    y INTEGER
)
RETURNS SETOF INTEGER AS
$$
    import os
    plpy.info('Running on a segment, with y = {0}'.format(y))
    plpy.info(os.getpid())
    return []
$$ LANGUAGE plpythonu;

--select my_segment_udf(x,y) from small_unbatched;
