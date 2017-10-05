#!/usr/bin/env bash

etc/kill-es.sh

ES_VERSION=5.6.2
ES_CLUSTER=wirktop-esutils-test
TMPDIR=etc/tmp
ES_DIR=etc/tmp/elasticsearch
PIDFILE=$TMPDIR/elasticsearch.pid
PACKAGE=$TMPDIR/elasticsearch-${ES_VERSION}.tar.gz

if [ ! -e $TMPDIR ]; then
    mkdir $TMPDIR
fi

rm -rf $ES_DIR
mkdir $ES_DIR
mkdir $ES_DIR/logs

# Download ES
if [ ! -e $PACKAGE ]; then
    DONWLOAD_URL="https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-${ES_VERSION}.tar.gz"
    echo "Downloading ElasticSearch from $DONWLOAD_URL"
    curl "$DONWLOAD_URL" > $PACKAGE
fi

# Unpack
tar xf $PACKAGE -C $ES_DIR --strip-components=1
echo "cluster.name: ${ES_CLUSTER}" >> $ES_DIR/config/elasticsearch.yml
echo "transport.tcp.port: 9300" >> $ES_DIR/config/elasticsearch.yml

# Start
$ES_DIR/bin/elasticsearch > $ES_DIR/logs/out.log &
echo $! > $PIDFILE
PID=`cat $PIDFILE`
echo "Started ElasticSearch with PID $PID"
