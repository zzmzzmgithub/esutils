#!/usr/bin/env bash

PIDFILE=etc/tmp/elasticsearch.pid

# Kill if it exists
if [ -e $PIDFILE ]; then
    PID=`cat $PIDFILE`
    echo "Killing ElasticSearch with $PID from $PIDFILE"
    kill -9 $PID
fi
