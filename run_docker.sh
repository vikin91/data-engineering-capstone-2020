#!/bin/bash

mkdir -p data/sas_data # for storing parquet files from the I94 data set
mkdir -p cache # for caching downloaded files - to speed-up the processing in the Notebook
mkdir -p jars # for caching downloaded jar files - to speed-up the processing in the Notebook
mkdir -p model # place for the ETL results in form of parquet files

docker run \
  -p 8888:8888 \
  --env SPARK_OPTS="--driver-java-options=-Xms4096M --driver-java-options=-Xmx8192M --driver-java-options=-Dlog4j.logLevel=info" \
  --mount type=bind,source="$(pwd)",target=/home/jovyan/work/ \
  --mount type=bind,source="$(pwd)/cache",target=/home/jovyan/.ivy2/cache/ \
  --mount type=bind,source="$(pwd)/jars",target=/home/jovyan/.ivy2/jars/ \
  jupyter/all-spark-notebook:3b1f4f5e6cc1
