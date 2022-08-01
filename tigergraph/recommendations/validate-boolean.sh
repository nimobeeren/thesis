#!/usr/bin/env bash
set -euo pipefail

curl -Ss "http://localhost:9000/query/validatePropertiesBoolean"
echo
curl -Ss "http://localhost:9000/query/validateCardinalityBoolean"
