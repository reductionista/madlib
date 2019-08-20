
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
-- Runs on segments, gets PID of each segment
DROP function IF EXISTS get_segment_udf(s integer, x real[], y integer);
CREATE OR REPLACE FUNCTION
get_pids_udf(
    x REAL[],
    y INTEGER
)
RETURNS SETOF INTEGER AS
$$
    import os
    return [os.getpid()]
$$ LANGUAGE plpythonu;

-- Runs on segments, gets seg id of each segment
DROP function IF EXISTS get_segment_udf(s integer, x real[], y integer);
CREATE OR REPLACE FUNCTION
get_segment_udf(
    s INTEGER,
    x REAL[],
    y INTEGER
)
RETURNS SETOF INTEGER AS
$$
#    plpy.info('Running on a segment, with y = {0}'.format(y))
    res = plpy.execute('SELECT 1 AS one')

    if 'greeting' in GD:
        plpy.info('segment_udf found value of {} for greeting'.format(GD['greeting']))
        GD['greeting'] += 1
    else:
        plpy.info('segment_udf: greeting not found in GD')
        GD['greeting'] = 1


    if 'num_calls' in SD:
        SD['num_calls'] += 1
    else:
        SD['num_calls'] = 1

    return [s]
$$ LANGUAGE plpythonu;

-- Runs on master
DROP function IF EXISTS my_master_udf(x real[], y integer);
CREATE OR REPLACE FUNCTION
my_master_udf(
    x REAL[],
    y INTEGER
)
RETURNS REAL[] AS
$$
    import plpy
    import os
    plpy.info('Master PID is {}'.format(os.getpid()))
    res = plpy.execute('SELECT get_segment_udf(gp_segment_id, x, y) AS my_results FROM beyond_1g_small')
    seg_ids = [ r['my_results'] for r in res ]
    plpy.info("Segment ID's: {}", seg_ids)
    plpy.info("pid's of segments:")
    res = plpy.execute('SELECT get_pids_udf(x, y) AS my_results FROM beyond_1g_small')
    results = [ r['my_results'] for r in res ]
    return results
$$ LANGUAGE plpythonu;

-- POC that a UDF can train on all segments simulatneously,
--   and then each returns the final weights to the master.
--   master receives back a table where each row is the
--   final weights for one of the segments.  (It then just
--   has to average them together to get the new weights
--   to start next iteration with.)

select my_master_udf(ARRAY[1.0, 2.0], 3);
-- select my_segment_udf(x,y) from beyond_1g_small;
