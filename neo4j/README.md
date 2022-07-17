## Prerequisites

- Docker 20 or newer
- Node.js v14 or newer

## Installation

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

### Data loading

1. Download the [recommendations data in Neo4j dump format](https://github.com/neo4j-graph-examples/recommendations/blob/main/data/recommendations-43.dump) and place it in a directory `~/neo4j/import`.

2. Import the data (in the container). Note that the `--force` flag drops all existing data!

```bash
bin/neo4j-admin load --from=import/recommendations-43.dump --force
```

### Schema validation

TODO: add constraints, run queries, run tests?
