#!/usr/bin/env bash
set -euo pipefail

# Prints the total execution time of all queries in the log file (in ms)
grep -Po '(?<=Finished in )\d+\.?\d*(?= ms)' tigergraph/log/gpe/log.INFO | awk '{s+=$1} END {print s}'
