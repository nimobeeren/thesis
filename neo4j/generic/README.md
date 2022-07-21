# Neo4j Generic Schema Validation

## Prerequisites

- Node.js v14 or newer

## Installation

1. Install dependencies with `npm install`

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
    -it \
    neo4j:4.4.8-enterprise \
    bash
```

### Re-using the container

If you want to use the container you created with `docker run` after ending the session, you can start it again using:

```bash
docker start -i neo4j
```

## Usage

Run tests with `npm test` (locally). The app connects to the running Neo4j instance using the crednentials in the `.env` file.
