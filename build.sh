#!/usr/bin/env bash
RUN_NAME="ad.oe.sre_metrics"

mkdir -p output/bin output/conf
cp scripts/* output/
cp ./documentation/examples/prometheus.yml output/conf/
chmod +x output/bootstrap.sh

if [[ "$IS_SYSTEM_TEST_ENV" != "1" ]]; then
    go build -o output/bin/${RUN_NAME} ./cmd/prometheus
else
    go test -c -covermode=set -o output/bin/${RUN_NAME} -coverpkg=./...
fi