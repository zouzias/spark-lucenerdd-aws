# spark-lucenerdd-aws

Usage examples of [spark-lucenerdd](https://github.com/zouzias/spark-lucenerdd) for AWS.

## Usage

Install Java, [SBT](http://www.scala-sbt.org) and clone the project

```bash
git clone https://github.com/zouzias/spark-lucenerdd-aws.git
cd spark-lucenerdd-aws
sbt compile assembly
```

The above JAR is pushed to `s3://spark-lucenerdd/aws/spark-lucenerdd-aws-assembly-0.0.18.jar` and used to generate statistics about the `spark-lucenerdd` project.
