#!/usr/bin/env python

import psycopg2 as p2
import time
import sys
import numpy as np

places_conn = p2.connect('postgresql://gpadmin@127.0.0.1:5432/places_shaped')
bytea_conn = p2.connect('postgresql://gpadmin@127.0.0.1:5432/bytea')

places_cur = places_conn.cursor()
bytea_cur = bytea_conn.cursor()

def convert_buffer(buf, suffix, batch_size, buffer_size):
    # Initialize an empty buffer to hold image data
    packed_images = np.empty(shape=(0,256,256,3),dtype='float32')
    labels = []
    start_time = time.time()
    for i in range(0,buffer_size, batch_size):
        if i % 100 == 0:
            print "{}/{} after {}s".format(i, buffer_size, time.time() - start_time)
        sql = 'SELECT * FROM places10_train WHERE (id % 50) = {} LIMIT {}'.format(buf, batch_size)
        places_cur.execute(sql)
        res = places_cur.fetchall()
        x = [ float(row[1]/255.0) for row in res ]
        y = [ int(row[2]) for row in res ]
        before_np_array = time.time()
        image_batch = np.array(x).reshape((batch_size,256,256,3))
        before_concat = time.time()
        packed_images = np.concatenate((packed_images, image_batch))
        after_concat = time.time()
        print ("np_array = {}, concat = {}".format(before_concat - before_np_array, after_concat - before_concat))
        labels += y

    bytea_images = packed_images.tostring()
    bytea_cur.execute('INSERT INTO places10_bytea_{} (buffer_id, independent_var, dependent_var) VALUES(%s, %s, %s)'.format(suffix), (buf, p2.Binary(bytea_images), labels))
    bytea_conn.commit()

buf = sys.argv[1]
suffix = sys.argv[2]
if len(sys.argv) > 3:
    batch_size = sys.agv[3]
else:
    batch_size = 25 # default

buffer_size = int(suffix)

try:
    bytea_cur.execute('CREATE TABLE places10_bytea_{} (buffer_id INTEGER, independent_var BYTEA, '
                                                       'dependent_var SMALLINT[])'.format(suffix))
except:
    pass # Don't complain if table already exists, just append to it

bytea_conn.commit()

convert_buffer(buf, suffix, batch_size, buffer_size)
