#!/usr/bin/env bash
set -euo pipefail

curl -Ss "http://localhost:9000/query/validateProperties"
echo
curl -Ss "http://localhost:9000/query/validateCardinality"
