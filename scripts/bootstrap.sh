#! /usr/bin/env bash

CURDIR=$(cd $(dirname $0); pwd)

exec "$CURDIR/bin/ad.oe.sre_metrics" --config.file=./conf/prometheus.yml --storage.tsdb.path=/tmp --storage.tsdb.retention.time=10y