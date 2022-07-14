#!/usr/bin/env bash
set -euxo pipefail

# Removes a mandatory property from a specific vertex
curl -d '{
  "vertices": {
    "Movie": {
      "0": {
        "title": {
          "value": ""
        }
      }
    }
  }
}' "http://localhost:9000/graph/Recommendations?update_vertex_only=true"
