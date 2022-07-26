## Prerequisites

- Docker 20 or newer

## Installation

Run a Docker container with TigerGraph, making sure to mount the data directory as well as a directory `thesis` which has the contents of this repository:

```bash
docker run \
  -d \
  -p 14022:22 -p 9000:9000 -p 14240:14240 \
  --name tigergraph \
  --ulimit nofile=1000000:1000000 \
  -v ~/tigergraph/data:/home/tigergraph/mydata \
  -v ~/Development/thesis:/home/tigergraph/thesis \
  -t docker.tigergraph.com/tigergraph:3.6.0
```

## Data loading

1. Connect to the TigerGraph instance with `ssh -p 14022 tigergraph@localhost` and enter password `tigergraph`.

2. Start the TigerGraph services with `gadmin start all`.

### Recommendations

1. Download the [recommendations data](https://drive.google.com/drive/folders/17byMzP_Ux7DloJsYuNdk07-mjC9PbMbF?usp=sharing) and place all CSV files in a directory `~/tigergraph/data/recomendations`.

1. Drop the existing database and create the schema:
```bash
gsql thesis/tigergraph/recommendations/schema.gsql
```

2. Prepare jobs for loading the data:
```bash
gsql thesis/tigergraph/recommendations/preload.gsql
```

3. Execute the loading jobs:
```bash
gsql thesis/tigergraph/recommendations/load.gsql
```

4. Install the validation queries:
```bash
gsql thesis/tigergraph/recommendations/prevalidate.gsql
```

5. (Optional) Introduce errors in the data by running the `mangle.sh` script:
```bash
bash thesis/tigergraph/recommendations/mangle.sh | jq
```

6. Run the validation queries:
```bash
bash thesis/tigergraph/recommendations/validate.sh | jq
```

### SNB

1. Run the GSQL prepare script using
```bash
gsql thesis/tigergraph/snb/prepare.gsql
```

2. Adjust the data path variable in the `load.sh` script and run it with `thesis/tigergraph/snb/load.sh`.
