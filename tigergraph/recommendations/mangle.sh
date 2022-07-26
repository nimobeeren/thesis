#!/usr/bin/env bash
set -euo pipefail

# Sets a mandatory property from a specific vertex to the null value
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
