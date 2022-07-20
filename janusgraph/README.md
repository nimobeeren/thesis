# janusgraph-schema

## Prerequisites

- Docker 20 or newer

## Installation

1. Run a Docker container with JanusGraph:

```bash
docker run \
  --name janusgraph \
  -v ~/Development/thesis:/opt/janusgraph/thesis \
  -v ~/janusgraph/data:/opt/janusgraph/mydata \
  -it \
  janusgraph/janusgraph:0.6.2 \
  bash
```

2. Copy the server configuration to the right location:

```bash
cp conf/janusgraph-berkeleyje-server.properties /etc/opt/janusgraph/janusgraph.properties
```

### Re-using the container

If you want to use the container you created with `docker run` after ending the session, you can start it again using:

```bash
docker start -i janusgraph
```

## Usage

### Data loading

1. Download the [recommendations data](https://drive.google.com/drive/folders/17byMzP_Ux7DloJsYuNdk07-mjC9PbMbF?usp=sharing) and place all CSV files in a directory `~/janusgraph/data/recomendations`.

1. Package the app (locally):

```bash
mvn package
```

2. Run the app (in the container) with the `load` command and passnig the path to the directory containing the CSV files. Note that the `--drop` flag drops all existing data!

```bash
java -jar thesis/janusgraph/target/janusgraph-schema-0.1.jar load (snb|recommendations) mydata/directory --drop
```

### Schema validation

1. Package the app (locally):

```bash
mvn package
```

2. Run the app (in the container) with the `validate` command:

```bash
java -jar thesis/janusgraph/target/janusgraph-schema-0.1.jar validate
```

### Running queries yourself

1. Run JanusGraph server (in the container):

```bash
bin/janusgraph-server.sh conf/janusgraph-server.yaml
```

2. Open the Gremlin console, either by [installing locally](https://docs.janusgraph.org/getting-started/installation/#local-installation), or using a separate Docker container:

```bash
docker run \
    --name janusgraph-client \
    --link janusgraph \
    -e GREMLIN_REMOTE_HOSTS=janusgraph \
    -it \
    janusgraph/janusgraph:0.6.2 \
    bin/gremlin.sh
```

3. Connect to the server and start a session:

```groovy
// Use default configuration to connect to a Gremlin server on localhost
// and start a session (to persist variables)
:remote connect tinkerpop.server conf/remote.yaml session
// Open the remote console so all following commands are sent to the remote
:remote console
```

4. Run queries like `g.V().hasLabel("Movie").count()` or do [whatever you want](https://tinkerpop.apache.org/docs/3.6.0/tutorials/the-gremlin-console/).
