#!/usr/bin/env bash

# Wait for ES to come up
url="http://localhost:9200"
echo "Waiting for ElasticSearch $url"
STATUSCODE=$(curl --silent --output /dev/null --write-out "%{http_code}" $url)
while [ $STATUSCODE -ne "200" ];
do
    sleep 1
    STATUSCODE=$(curl --silent --output /dev/null --write-out "%{http_code}" $url)
done
