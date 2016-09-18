#!/usr/bin/env bash

CURRENT_DIR=`pwd`

# Assumes that spark is installed under home directory
HOME_DIR=`echo ~`
#export SPARK_LOCAL_IP=localhost
SPARK_HOME=${HOME_DIR}/spark-1.6.2-bin-hadoop2.6

# spark-lucenerdd assembly JAR
MAIN_JAR=${CURRENT_DIR}/target/scala-2.10/spark-lucenerdd-aws-assembly-0.0.24.jar

# Run spark shell locally
${SPARK_HOME}/bin/spark-submit   \
                 --driver-memory 2g \
		         --executor-memory 1g \
                 --conf spark.executor.memory=1g \
                 --conf spark.driver.memory=2g \
                 --conf spark.executor.cores=1 \
                 --conf spark.executor.instances=2 \
                 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
				 --conf spark.kryo.registrator=org.zouzias.spark.lucenerdd.LuceneRDDKryoRegistrator \
                 --conf spark.executor.extraJavaOptions="-Dlucenerdd.index.store.mode=memory" \
                 --conf spark.driver.extraJavaOptions="-Dlucenerdd.index.store.mode=memory" \
                 --master local[2] \
				 --class $1 \
				 "${MAIN_JAR}"
