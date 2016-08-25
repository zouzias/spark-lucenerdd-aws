package org.zouzias.spark.lucenerdd.aws.dfvslucene

import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.zouzias.spark.lucenerdd.aws.utils.WikipediaUtils
import org.zouzias.spark.lucenerdd.facets.FacetedLuceneRDD
import org.zouzias.spark.lucenerdd._
import org.apache.spark.sql.functions._


/**
 * Dataframe vs LuceneRDD
 */
object DataFrameVsLuceneRDDExample extends Logging {

  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName(DataFrameVsLuceneRDDExample.getClass.getName)

    //
    implicit val sc = new SparkContext(conf)
    implicit val sqlContext = new SQLContext(sc)

    val today = WikipediaUtils.dayString()
    val executorMemory = conf.get("spark.executor.memory")
    val executorCores = conf.get("spark.executor.cores")
    val executorInstances = conf.get("spark.executor.instances")
    val fieldName = "Country"


    log.info(s"Executor instances: ${executorInstances}")
    log.info(s"Executor cores: ${executorCores}")
    log.info(s"Executor memory: ${executorMemory}")

    logInfo("Loading Cities")
    val citiesDF = sqlContext.read.parquet("/Users/taazoan3/recordLinkageData/maxmind/world-cities-maxmind.parquet")
    citiesDF.cache()
    citiesDF.count()
    val luceneRDD = FacetedLuceneRDD(citiesDF)
    luceneRDD.cache()
    luceneRDD.count()
    logInfo("Cities loaded successfully")

    val k = 10

    val dfStart = System.currentTimeMillis()
    val dfResults = citiesDF.groupBy("Country").count().sort(desc("count")).take(k)
    val dfEnd = System.currentTimeMillis()

    val lucStart =System.currentTimeMillis()
    val luceneResults = luceneRDD.facetQuery("*:*", "Country", k)
    val lucEnd =System.currentTimeMillis()


    println("=" * 10)
    println(s"DF time: ${dfEnd - dfStart} / 1000 seconds")
    println("=" * 10)
    println(s"Lucene time: ${lucEnd - lucStart} / 1000 seconds")
    println("=" * 10)


    // linkedDF.write.mode(SaveMode.Overwrite).parquet(s"s3://spark-lucenerdd/timings/v0.0.20/dataframe-vs-lucenerdd-${today}.parquet")

    // terminate spark context
    sc.stop()

  }
}
