#!/usr/bin/env bash
set -euxo pipefail

curl "http://localhost:9000/query/validateProperties"
echo
curl "http://localhost:9000/query/validateCardinality"
