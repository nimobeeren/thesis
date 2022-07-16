## Prerequisites

- Docker 20 or newer
- Node.js v14 or newer

## Usage

1. Install Node.js dependencies with `npm install`.

2. Run a Docker container with Neo4j:

```bash
docker run \
    --name neo4j \
    -p 7474:7474 -p 7687:7687 \
    -v $HOME/neo4j/data:/data \
    -v $HOME/neo4j/logs:/logs \
    -v $HOME/neo4j/import:/var/lib/neo4j/import \
    -v $HOME/neo4j/plugins:/plugins \
    -e NEO4J_AUTH=neo4j/thesis \
    -e NEO4JLABS_PLUGINS=\[\"apoc\"\] \
    -e NEO4J_ACCEPT_LICENSE_AGREEMENT=yes \
    neo4j:4.4.8-enterprise
```

3. Run tests with `npm test`.
