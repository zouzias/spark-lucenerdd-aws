#!/usr/bin/env bash


INSTANCE_TYPE="r3.xlarge"
INSTANCE_COUNT=5
EXECUTOR_INSTANCES=5
EXECUTOR_MEMORY="9g"
AWS_REGION="eu-central-1"
LUCENERDD_VERSION="0.0.24"


aws emr create-cluster --applications Name=Hadoop Name=Spark \
	--ec2-attributes '{"KeyName":"aws-amazon","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-26648d5d","EmrManagedSlaveSecurityGroup":"sg-d4e85abd","EmrManagedMasterSecurityGroup":"sg-d5e85abc"}' \
	--service-role EMR_DefaultRole \
	--enable-debugging \
	--release-label emr-4.7.2 \
	--log-uri 's3n://aws-logs-438094089491-eu-central-1/elasticmapreduce/' \
	--steps "[{\"Args\":[\"spark-submit\",\"--deploy-mode\",\"cluster\",\"--conf\",\"spark.rdd.compress=true\",\"--conf\",\"spark.executor.memory=${EXECUTOR_MEMORY}\",\"--conf\",\"spark.executor.instances=${EXECUTOR_INSTANCES}\",\"--conf\",\"spark.serializer=org.apache.spark.serializer.KryoSerializer\",\"--conf\",\"spark.kryo.registrator=org.zouzias.spark.lucenerdd.LuceneRDDKryoRegistrator\",\"--class\",\"org.zouzias.spark.lucenerdd.aws.linkage.VisaGeonamesLinkageExample\",\"s3://spark-lucenerdd/aws/spark-lucenerdd-aws-assembly-${LUCENERDD_VERSION}.jar\"],\"Type\":\"CUSTOM_JAR\",\"ActionOnFailure\":\"TERMINATE_CLUSTER\",\"Jar\":\"command-runner.jar\",\"Properties\":\"\",\"Name\":\"Spark application (spark-lucenerdd-aws)\"}]" \
    --name "LuceneRDD (linkage v${LUCENERDD_VERSION} - ${EXECUTOR_INSTANCES} executors - ${INSTANCE_COUNT} instances)" \
	--instance-groups "[{\"InstanceCount\":1,\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"m3.xlarge\",\"Name\": \"Master instance group - 1\"},{\"InstanceCount\": ${INSTANCE_COUNT} , \"InstanceGroupType\": \"CORE\", \"InstanceType\": \"${INSTANCE_TYPE}\" , \"Name\" : \"Core instance group - 2\"}]" \
    --auto-terminate \
	--region ${AWS_REGION}
