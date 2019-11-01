set timing=off;
set timing=on;

DROP TABLE IF EXISTS my_temp_table;

EXPLAIN     CREATE TEMP TABLE my_temp_table AS
                SELECT x,
                ARRAY[(y) IS NOT DISTINCT FROM 'airfield', (y) IS NOT DISTINCT FROM 'airplane_cabin', (y) IS NOT DISTINCT FROM 'airport_terminal', (y) IS NOT DISTINCT FROM 'alcove', (y) IS NOT DISTINCT FROM 'alley', (y) IS NOT DISTINCT FROM 'amphitheater', (y) IS NOT DISTINCT FROM 'amusement_arcade', (y) IS NOT DISTINCT FROM 'amusement_park', (y) IS NOT DISTINCT FROM 'apartment_building-outdoor', (y) IS NOT DISTINCT FROM 'aquarium']::INTEGER[]::SMALLINT[] AS y,
                row_number() over() AS row_id
            FROM cifar_demo_train_stringclasses ORDER BY RANDOM();

            CREATE TEMP TABLE my_temp_table AS
            SELECT x,
                ARRAY[(y) IS NOT DISTINCT FROM 'airfield', (y) IS NOT DISTINCT FROM 'airplane_cabin', (y) IS NOT DISTINCT FROM 'airport_terminal', (y) IS NOT DISTINCT FROM 'alcove', (y) IS NOT DISTINCT FROM 'alley', (y) IS NOT DISTINCT FROM 'amphitheater', (y) IS NOT DISTINCT FROM 'amusement_arcade', (y) IS NOT DISTINCT FROM 'amusement_park', (y) IS NOT DISTINCT FROM 'apartment_building-outdoor', (y) IS NOT DISTINCT FROM 'aquarium']::INTEGER[]::SMALLINT[] AS y,
                row_number() over() AS row_id
            FROM cifar_demo_train_stringclasses ORDER BY RANDOM();

