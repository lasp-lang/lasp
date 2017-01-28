#!/bin/sh

echo "http://`STACK_NAME=dcos ./bin/elb-host.sh`" | xargs open
