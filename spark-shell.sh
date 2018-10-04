#!/usr/bin/env bash

CURRENT_DIR=`pwd`

# Read the version from version.sbt
SPARK_LUCENERDD_VERSION=`cat version.sbt | awk '{print $5}' | xargs`

echo "==============================================="
echo "Loading LuceneRDD with version ${SPARK_LUCENERDD_VERSION}"
echo "==============================================="

# Assumes that spark is installed under home directory
echo "SPARK SHELL: $1"

# Run spark shell locally
spark-shell   \
        --conf "spark.executor.memory=1g" \
        --conf "spark.executor.cores=4" \
        --conf "spark.driver.memory=1g" \
        --conf "spark.rdd.compress=true" \
        --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
        --conf "spark.kryoserializer.buffer=24mb" \
        --conf "spark.kryo.registrator=org.zouzias.spark.lucenerdd.LuceneRDDKryoRegistrator" \
        --conf "spark.driver.extraJavaOptions=-Dlucenerdd.index.store.mode=disk" \
        --conf "spark.executor.extraJavaOptions=-Dlucenerdd.index.store.mode=disk" \
        --master local[*] \
        --packages org.zouzias:spark-lucenerdd_2.11:${SPARK_LUCENERDD_VERSION},org.apache.hadoop:hadoop-aws:2.7.4 \
        -i SetupS3Auth.scala
