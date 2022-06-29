#!/bin/bash

[[ "pg_kafka" == $(basename $(pwd)) ]] || { 2>&1 echo "Project dir should be named pg_kafka (used in dockerfile)"; exit 1; }

cd ..
set -eo pipefail
TAG=tmp$RANDOM$RANDOM
[[ -d dist ]] || mkdir dist
set -x

docker build . --file pg_kafka/Dockerfile --tag $TAG
docker run --rm -it --user $(id -u):$(id -g) -v $(pwd)/dist:/opt/pg_kafka/dist -w /opt/pg_kafka $TAG  -c 'cd stage && tar cvzf ../dist/pg_kafka-pg14-debian-x64.tgz *'
docker image rm $TAG

