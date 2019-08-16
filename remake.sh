make; make package; make gppkg

gppkg -r madlib-1.16_dev

gppkg -i deploy/gppkg/5/madlib-1.16_dev-gp5-rhel6-x86_64.gppkg
echo 'y' | $GPHOME/madlib/bin/madpack -p greenplum -c/okislal reinstall

