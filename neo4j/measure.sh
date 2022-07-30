#!/usr/bin/env bash
set -euo pipefail

# Prints the total execution time of all queries in the log file (in ms)
grep -Po '\d+(?= ms)' /logs/query.log | awk '{s+=$1} END {print s}'
