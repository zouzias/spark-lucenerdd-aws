#!/bin/bash

CURRENT_DIR=`pwd`

# Read the version from version.sbt
SPARK_LUCENERDD_VERSION=`cat version.sbt | awk '{print $5}' | xargs`

echo "==============================================="
echo "Loading LuceneRDD with version ${SPARK_LUCENERDD_VERSION}"
echo "==============================================="
# spark-lucenerdd assembly JAR
MAIN_JAR=${CURRENT_DIR}/target/scala-2.11/spark-lucenerdd-aws-assembly-${SPARK_LUCENERDD_VERSION}.jar

# Run spark shell locally
# Run spark shell locally
spark-submit   \
	 --driver-memory 4g \
	 --executor-memory 4g \
	 --conf spark.executor.instances=10 \
	 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
	 --conf spark.kryo.registrator=org.zouzias.spark.lucenerdd.spatial.shape.ShapeLuceneRDDKryoRegistrator \
	 --conf spark.executor.extraJavaOptions="-Dlucenerdd.index.store.mode=disk" \
	 --conf spark.driver.extraJavaOptions="-Dlucenerdd.index.store.mode=disk" \
	 --master yarn \
	 --deploy-mode client \
	 --class org.zouzias.spark.lucenerdd.aws.indexing.WikipediaIndexingExample \
	 "${MAIN_JAR}"
