#!/usr/bin/env bash

AWS_REGION="eu-west-1"

aws emr create-cluster --applications Name=Hadoop Name=Spark \
    --ec2-attributes '{"KeyName":"aws-ireland-ec2","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-12af7876","EmrManagedSlaveSecurityGroup":"sg-3bfd315c","EmrManagedMasterSecurityGroup":"sg-3cfd315b"}' \
    --release-label emr-5.16.0 \
    --log-uri 's3n://aws-logs-438094089491-eu-west-1/elasticmapreduce/' \
    --steps '[{"Args":["spark-submit","--deploy-mode","cluster","--driver-memory","8g","--executor-memory","8g","--conf","spark.executor.instances=4","--conf","spark.dynamicAllocation.enabled=false","--conf","spark.serializer=org.apache.spark.serializer.KryoSerializer","--conf","spark.kryo.registrator=org.zouzias.spark.lucenerdd.LuceneRDDKryoRegistrator","--conf","spark.executor.extraJavaOptions=-Dlucenerdd.index.store.mode=disk","--conf","spark.driver.extraJavaOptions=-Dlucenerdd.index.store.mode=disk","--class","org.zouzias.spark.lucenerdd.aws.linkage.blocked.LinkageBlockGeonamesExample","s3://spark-lucenerdd/2.11/spark-lucenerdd-aws-assembly-0.3.3-SNAPSHOT.jar"],"Type":"CUSTOM_JAR","ActionOnFailure":"TERMINATE_CLUSTER","Jar":"command-runner.jar","Properties":"","Name":"LuceneRDD Deduplication Spark Job"}]' --instance-groups '[{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"MASTER","InstanceType":"m4.large","Name":"Master - 1"},{"InstanceCount":5,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"CORE","InstanceType":"r3.xlarge","Name":"Core - 2"}]' \
    --auto-terminate \
    --auto-scaling-role EMR_AutoScaling_DefaultRole \
    --service-role EMR_DefaultRole \
    --name 'My cluster' \
    --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
    --region ${AWS_REGION}
