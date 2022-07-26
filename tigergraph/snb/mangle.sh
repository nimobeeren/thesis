#!/usr/bin/env bash
set -euo pipefail

# Sets a mandatory property from a specific vertex to the null value
curl -Ss -d '{
  "vertices": {
    "Post": {
      "3": {
        "length": {
          "value": -1
        }
      }
    }
  }
}' "http://localhost:9000/graph/ldbc_snb?update_vertex_only=true"
