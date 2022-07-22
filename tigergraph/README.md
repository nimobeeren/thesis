## Prerequisites

- Docker 20 or newer

## Installation

Run a Docker container with TigerGraph:

```bash
docker run \
  -d \
  -p 14022:22 -p 9000:9000 -p 14240:14240 \
  --name tigergraph \
  --ulimit nofile=1000000:1000000 \
  -v ~/tigergraph/data:/home/tigergraph/mydata \
  -t docker.tigergraph.com/tigergraph:latest
```
