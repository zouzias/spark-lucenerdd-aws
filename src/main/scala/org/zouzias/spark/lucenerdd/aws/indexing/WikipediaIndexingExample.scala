package org.zouzias.spark.lucenerdd.aws.indexing

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.SparkConf
import org.zouzias.spark.lucenerdd.{LuceneRDD, _}
import org.zouzias.spark.lucenerdd.aws.utils._
import org.zouzias.spark.lucenerdd.logging.Logging


/**
 * Wikipedia indexing performance test
 *
 * This test index only the titles of all wikipedia articles
 */
object WikipediaIndexingExample extends Logging {

  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName("WikipediaIndexingExample")
    implicit val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val today = Utils.dayString()
    val executorMemory = conf.get("spark.executor.memory")
    val executorCores = conf.get("spark.executor.cores")
    val executorInstances = conf.get("spark.executor.instances")

    log.info(s"Executor instances: ${executorInstances}")
    log.info(s"Executor cores: ${executorCores}")
    log.info(s"Executor memory: ${executorMemory}")

    logInfo("Loading Wikipedia titles")
    val sparkInfo = SparkInfo(executorInstances, executorMemory, executorCores)

    import spark.implicits._
    val wiki = spark.read.parquet("s3://spark-lucenerdd/wikipedia/enwiki-latest-all-titles.parquet")
      .map(row => row.getString(0)).map(_.replaceAll("_", " ")).map(_.replaceAll("[^a-zA-Z0-9\\s]", ""))
      .rdd

    logInfo("Wikipedia titles loaded successfully")

    val start = System.currentTimeMillis()
    val luceneRDD = LuceneRDD(wiki)
    luceneRDD.cache()
    luceneRDD.count() // To force caching
    val end = System.currentTimeMillis()

    import spark.implicits._
    val timingsDF = spark.sparkContext.parallelize(Array(IndexingTiming(end - start, today, Utils.Version))).toDF()
    timingsDF.write.mode(SaveMode.Append).parquet(s"s3://spark-lucenerdd/timings/wikipedia-indexing-${sparkInfo.toString()}.parquet")

    // terminate spark context
    spark.stop()
  }
}
