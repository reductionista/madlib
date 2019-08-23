#!/usr/bin/python

import psycopg2 as p2
import time

places_conn = p2.connect('postgresql://gpadmin@127.0.0.1:15432/places_shaped')
bytea_conn = p2.connect('postgresql://gpadmin@127.0.0.1:15432/bytea')

places_cur = places_conn.cursor()
bytea_cur = bytea_conn.cursor()

bytea_cur.execute('DROP TABLE IF EXISTS places10_bytea_{}'.format(suffix))
places_cur.execute('CREATE TABLE places10_bytea_{}'.format(suffix))

def convert_buffer(buf, batch_size):
    start_time = time.time()
    for i in range(0,500,batch_size):
        if i % 100 == 0:
            print "{}/500 after {}s".format(i, time.time() - start_time)
        res = p2.execute('SELECT * FROM places10_train WHERE (id % 98) = buf LIMIT {}'.format(batch_size))
        x = [ row['x'] for row in res ]
        y = [ row['y'] for row in res ]
        image_batch = np.array(x).reshape((batch_size,256,256,3))
        packed_images = np.concatenate((packed_images, image_batch))
        labels += y

buf = sys.argv[1]
if len(sys.argv) > 2:
    batch_size = sys.agv[2]
else:
    batch_size = 25 # default


