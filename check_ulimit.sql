CREATE OR REPLACE FUNCTION yo() RETURNS VOID AS $$
import os
os.system('bash -c "ulimit -c" > /tmp/ulimit_output')
$$ LANGUAGE plpythonu;
