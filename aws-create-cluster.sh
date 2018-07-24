#!/usr/bin/env bash

# Creates an AWS EMR Hadoop Cluster
# To configure manually change the values below

aws emr create-cluster --auto-scaling-role EMR_AutoScaling_DefaultRole --applications Name=Hadoop Name=Spark Name=Zeppelin --ebs-root-volume-size 10 --ec2-attributes '{"KeyName":"aws-ireland-ec2","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-12af7876","EmrManagedSlaveSecurityGroup":"sg-3bfd315c","EmrManagedMasterSecurityGroup":"sg-3cfd315b"}' --service-role EMR_DefaultRole --release-label emr-5.16.0 --name 'LuceneRDD Cluster' --instance-groups '[{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"MASTER","InstanceType":"m4.large","Name":"Master - 1"},{"InstanceCount":2,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"CORE","InstanceType":"r3.xlarge","Name":"Core - 2"}]' --scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region eu-west-1
