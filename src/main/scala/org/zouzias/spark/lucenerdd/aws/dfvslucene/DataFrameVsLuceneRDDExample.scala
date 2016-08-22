package org.zouzias.spark.lucenerdd.aws.dfvslucene

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.zouzias.spark.lucenerdd.aws.utils.{LinkedRecord, WikipediaUtils}
import org.zouzias.spark.lucenerdd.{LuceneRDD, _}
/**
 * H1B visas vs geonames cities linkage example
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
    val fieldName = "lca_case_employer_city"


    log.info(s"Executor instances: ${executorInstances}")
    log.info(s"Executor cores: ${executorCores}")
    log.info(s"Executor memory: ${executorMemory}")

    logInfo("Loading H1B Visa")
    val visaDF = sqlContext.read.parquet("s3://h1b-visa-enigma.io/enigma-io-h1bvisa.parquet")
    val luceneRDD = LuceneRDD(visaDF.select(fieldName))
    luceneRDD.cache()
    logInfo("H1B Visas loaded successfully")



    linkedDF.write.mode(SaveMode.Overwrite).parquet(s"s3://spark-lucenerdd/timings/v0.0.20/dataframe-vs-lucenerdd-${today}.parquet")

    // terminate spark context
    sc.stop()

  }
}
