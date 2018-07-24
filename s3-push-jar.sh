#!/bin/bash

# Pushes fat Jar to AWS S3 under bucket named spark-lucenerdd

# Assembly Jar
sbt assembly

# Push to S3 (assuming aws-cli is properly setup)
SPARK_LUCENERDD_VERSION=`cat version.sbt | awk '{print $5}' | xargs`
echo "Pushing Spark LuceneRDD version ${SPARK_LUCENERDD_VERSION} to S3"
aws s3 cp target/scala-2.11/spark-lucenerdd-aws-assembly-${SPARK_LUCENERDD_VERSION}.jar s3://spark-lucenerdd/aws/
