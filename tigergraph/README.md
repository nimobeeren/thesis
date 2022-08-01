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
  -m 6g \
  -t docker.tigergraph.com/tigergraph:3.6.0
```

## Data loading

1. Connect to the TigerGraph instance with
```bash
ssh -p 14022 tigergraph@localhost
```
and enter password `tigergraph`.

2. Start the TigerGraph services with `gadmin start all`.

### Recommendations

1. Download the [recommendations data](https://drive.google.com/drive/folders/17byMzP_Ux7DloJsYuNdk07-mjC9PbMbF?usp=sharing) and place all CSV files in a directory `~/tigergraph/data/recomendations`.

2. Drop the existing database and create the schema:
```bash
gsql thesis/tigergraph/recommendations/schema.gsql
```

3. Prepare jobs for loading the data:
```bash
gsql thesis/tigergraph/recommendations/preload.gsql
```

4. Execute the loading jobs:
```bash
gsql thesis/tigergraph/recommendations/load.gsql
```

5. Install the validation queries:
```bash
gsql thesis/tigergraph/recommendations/prevalidate.gsql
```

6. (Optional) Introduce errors in the data by running the `mangle.sh` script:
```bash
bash thesis/tigergraph/recommendations/mangle.sh | jq
```

7. Run the validation queries:
```bash
bash thesis/tigergraph/recommendations/validate.sh | jq
```

### SNB

1. Download the Social Network Benchmark dataset file `social_network-csv_basic-sf$SCALE_FACTOR.tar.zst` (where `$SCALE_FACTOR` is `0.1`, `0.3`, `1` etc.) from [this repository](https://github.com/ldbc/data-sets-surf-repository) and extract the CSV files using the instructions.

2. Drop the existing database and create the schema:
```bash
gsql thesis/tigergraph/snb/schema.gsql
```

3. Prepare jobs for loading the data:
```bash
gsql thesis/tigergraph/snb/preload.gsql
```

4. Adjust the `LDBC_SNB_DATA_DIR` variable in the `load.sh` script and execute it:
```bash
bash thesis/tigergraph/snb/load.sh
```

5. Install the validation queries:
```bash
gsql thesis/tigergraph/snb/prevalidate.gsql
```

6. (Optional) Introduce errors in the data by running the `mangle.sh` script:
```bash
bash thesis/tigergraph/snb/mangle.sh | jq
```

7. Run the validation queries:
```bash
bash thesis/tigergraph/snb/validate.sh | jq
```

## Time measuring

TigerGraph stores a log of all queries in `tigergraph/log/gpe/log.INFO`. We provide a script `measure.sh` which sums up the execution times of all queries in that log and returns the total (in milliseconds). Just be sure to empty the log before starting the workload with `echo > tigergraph/log/gpe/log.INFO`.
