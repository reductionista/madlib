
-- Can we use this to return layers of weights that total > 1GB ?
DROP TYPE IF EXISTS weights CASCADE;
CREATE TYPE weights AS
(
    layer0 REAL[],
    layer1 REAL[],
    layer2 REAL[],
    layer3 REAL[],
    layer4 REAL[],
    layer5 REAL[],
    layer6 REAL[],
    layer7 REAL[],
    layer8 REAL[],
    layer9 REAL[]
);

-- Runs on segments
DROP function IF EXISTS my_segment_udf(x real[], y integer);
CREATE OR REPLACE FUNCTION
my_segment_udf(
    x REAL[],
    y INTEGER
)
RETURNS SETOF INTEGER AS
$$
    plpy.info('Running on a segment, with y = {0}'.format(y))
    res = plpy.execute('SELECT 1 AS one')
#    res = plpy.execute('SELECT my_udf AS one FROM meow')
    plpy.info(res[0])
    if 'num_calls' in SD:
        if SD['num_calls'] == 5:
            return [ res[0]['one'] ]
        else:
            SD['num_calls'] += 1
            return []
    else:
        SD['num_calls'] = 1
        return []
$$ LANGUAGE plpythonu;

-- Runs on master
DROP function IF EXISTS my_master_udf(x real[], y integer);
CREATE OR REPLACE FUNCTION
my_master_udf(
    x REAL[],
    y INTEGER
)
RETURNS SETOF REAL[] AS
$$
    plpy.info('Running on master')
    res1 = plpy.execute('SELECT * FROM meow')
    res2 = [ xx[0][0] + y for xx in x ]
    plpy.info(type(res1))
    return list(res1)
$$ LANGUAGE plpythonu;


-- POC that a UDF can train on all segments simulatneously,
--   and then each returns the final weights to the master.
--   master receives back a table where each row is the
--   final weights for one of the segments.  (It then just
--   has to average them together to get the new weights
--   to start next iteration with.)
select my_segment_udf(x,y) from beyond_1g_small;
