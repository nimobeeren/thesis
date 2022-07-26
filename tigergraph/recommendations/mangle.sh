#!/usr/bin/env bash
set -euo pipefail

# Removes a mandatory property from a specific vertex
curl -Ss -d '{
  "vertices": {
    "Movie": {
      "0": {
        "title": {
          "value": "Toy Story"
        }
      }
    }
  }
}' "http://localhost:9000/graph/Recommendations?update_vertex_only=true"
