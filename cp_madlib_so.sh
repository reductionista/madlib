gpssh -f ~/hosts.conf -e 'mkdir -p /home/gpadmin/madlib/build/src/ports/greenplum/5/lib'
gpscp -f ~/hosts.conf /home/gpadmin/madlib/build/src/ports/greenplum/5/lib/libmadlib.so =:/home/gpadmin/madlib/build/src/ports/greenplum/5/lib/
