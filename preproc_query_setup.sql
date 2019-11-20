            CREATE TABLE madlib_perm_series
                AS
                SELECT generate_series(0, 999999) __dist_key__
                DISTRIBUTED BY (__dist_key__);


            CREATE TABLE madlib_perm_dist_key AS
                SELECT gp_segment_id AS id, min(__dist_key__) AS __dist_key__
                FROM madlib_perm_series
                    GROUP BY gp_segment_id;

