#!/usr/bin/env bash

CURRENT_DIR=`pwd`

LUCENERDD_VERSION=`cat version.sbt | awk '{print $5}' | xargs`

# spark-lucenerdd assembly JAR
MAIN_JAR=${CURRENT_DIR}/target/scala-2.11/spark-lucenerdd-aws-assembly-${LUCENERDD_VERSION}.jar

# Run spark shell locally
spark-submit   \
	 --driver-memory 2g \
	 --executor-memory 4g \
	 --conf spark.executor.instances=4 \
	 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
	 --conf spark.kryo.registrator=org.zouzias.spark.lucenerdd.LuceneRDDKryoRegistrator \
	 --conf spark.executor.extraJavaOptions="-Dlucenerdd.index.store.mode=disk" \
	 --conf spark.driver.extraJavaOptions="-Dlucenerdd.index.store.mode=disk" \
	 --master yarn \
	 --deploy-mode client \
	 --class $1 \
	 "${MAIN_JAR}"
