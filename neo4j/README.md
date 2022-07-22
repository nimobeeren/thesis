## Prerequisites

- Docker 20 or newer

## Installation

Run a Docker container with Neo4j:

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

### Recommendations

1. Download the [recommendations data in Neo4j dump format](https://github.com/neo4j-graph-examples/recommendations/blob/main/data/recommendations-43.dump) and place it in a directory `~/neo4j/import`.

2. Import the data (in the container). Note that the `--force` flag drops all existing data!

```bash
bin/neo4j-admin load --from=import/recommendations-43.dump --force
```

### SNB

1. Download the Social Network Benchmark dataset file `social_network-csv_basic-sf$SCALE_FACTOR.tar.zst` (where `$SCALE_FACTOR` is `0.1`, `0.3`, `1` etc.) from [this repository](https://github.com/ldbc/data-sets-surf-repository) and extract the CSV files using the instructions.

2. Run the prepare script, passing it the directory where the extracted CSV files are stored. This will modify the CSV files in place!

```bash
snb/prepare.sh path/to/directory
```

3. Copy the files to the Neo4j import directory that was specified when creating the container.

4. In the Docker container, run the `neo4j-admin` script to import the data. This will drop any existing data in the database!

```bash
bin/neo4j-admin import \
  --force \
  --id-type=INTEGER \
  --delimiter="|" \
  --nodes=Organisation=import/static/organisation_0_0.csv \
  --nodes=Place=import/static/place_0_0.csv \
  --nodes=Tag=import/static/tag_0_0.csv \
  --nodes=TagClass=import/static/tagclass_0_0.csv \
  --nodes=Message:Comment=import/dynamic/comment_0_0.csv \
  --nodes=Forum=import/dynamic/forum_0_0.csv \
  --nodes=Person=import/dynamic/person_0_0.csv \
  --nodes=Message:Post=import/dynamic/post_0_0.csv \
  --relationships=IS_LOCATED_IN=import/static/organisation_isLocatedIn_place_0_0.csv \
  --relationships=IS_PART_OF=import/static/place_isPartOf_place_0_0.csv \
  --relationships=HAS_TYPE=import/static/tag_hasType_tagclass_0_0.csv \
  --relationships=IS_SUBCLASS_OF=import/static/tagclass_isSubclassOf_tagclass_0_0.csv \
  --relationships=HAS_CREATOR=import/dynamic/comment_hasCreator_person_0_0.csv \
  --relationships=HAS_TAG=import/dynamic/comment_hasTag_tag_0_0.csv \
  --relationships=IS_LOCATED_IN=import/dynamic/comment_isLocatedIn_place_0_0.csv \
  --relationships=REPLY_OF=import/dynamic/comment_replyOf_comment_0_0.csv \
  --relationships=REPLY_OF=import/dynamic/comment_replyOf_post_0_0.csv \
  --relationships=CONTAINER_OF=import/dynamic/forum_containerOf_post_0_0.csv \
  --relationships=HAS_MEMBER=import/dynamic/forum_hasMember_person_0_0.csv \
  --relationships=HAS_MODERATOR=import/dynamic/forum_hasModerator_person_0_0.csv \
  --relationships=HAS_TAG=import/dynamic/forum_hasTag_tag_0_0.csv \
  --relationships=HAS_INTEREST=import/dynamic/person_hasInterest_tag_0_0.csv \
  --relationships=IS_LOCATED_IN=import/dynamic/person_isLocatedIn_place_0_0.csv \
  --relationships=KNOWS=import/dynamic/person_knows_person_0_0.csv \
  --relationships=LIKES=import/dynamic/person_likes_comment_0_0.csv \
  --relationships=LIKES=import/dynamic/person_likes_post_0_0.csv \
  --relationships=STUDY_AT=import/dynamic/person_studyAt_organisation_0_0.csv \
  --relationships=WORK_AT=import/dynamic/person_workAt_organisation_0_0.csv \
  --relationships=HAS_CREATOR=import/dynamic/post_hasCreator_person_0_0.csv \
  --relationships=HAS_TAG=import/dynamic/post_hasTag_tag_0_0.csv \
  --relationships=IS_LOCATED_IN=import/dynamic/post_isLocatedIn_place_0_0.csv
```

### Schema validation

TODO: add constraints, run queries, run tests?
