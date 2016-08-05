# spark-lucenerdd-examples

Usage examples of [spark-lucenerdd](https://github.com/zouzias/spark-lucenerdd) for AWS.

## Usage

Install Java, [SBT](http://www.scala-sbt.org) and clone the project

```bash
git clone https://github.com/zouzias/spark-lucenerdd-aws.git
cd spark-lucenerdd-aws
sbt compile assembly
```

Download and extract apache spark under your home directory, update the `spark-submit.sh` script accordingly and run

```
./spark-submit.sh
```

to run the project.
