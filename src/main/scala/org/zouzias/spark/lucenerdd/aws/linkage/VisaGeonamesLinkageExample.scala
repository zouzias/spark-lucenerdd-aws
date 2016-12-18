package org.zouzias.spark.lucenerdd.aws.linkage

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.SparkConf
import org.zouzias.spark.lucenerdd.aws.utils.{LinkedRecord, SparkInfo, Utils}
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd.logging.Logging

/**
 * H1B visas vs Geoname cities linkage example
 */
object VisaGeonamesLinkageExample extends Logging {

  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName(VisaGeonamesLinkageExample.getClass.getName)

    implicit val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val today = Utils.dayString()
    val executorMemory = conf.get("spark.executor.memory")
    val executorCores = conf.get("spark.executor.cores")
    val executorInstances = conf.get("spark.executor.instances")
    val fieldName = "lca_case_employer_city"


    log.info(s"Executor instances: ${executorInstances}")
    log.info(s"Executor cores: ${executorCores}")
    log.info(s"Executor memory: ${executorMemory}")
    val sparkInfo = SparkInfo(executorInstances, executorMemory, executorCores)


    val start = System.currentTimeMillis()

    logInfo("Loading H1B Visa")
    val visa = spark.read.parquet("s3://h1b-visa-enigma.io/enigma-io-h1bvisa.parquet")
    val luceneRDD = LuceneRDD(visa.select(fieldName))
    luceneRDD.cache()
    logInfo("H1B Visas loaded successfully")

    logInfo("Loading Geonames Cities")
    val citiesDF = spark.read.parquet("s3://recordlinkage/geonames-usa-cities.parquet")
    val cities = citiesDF.flatMap(row => Option(row.getString(1)))
    cities.cache()
    logInfo("Geonames cities loaded successfully")

    val andLinker = (cityName: String) => {
      val nameTokenized = cityName.split(" ").map(_.replaceAll("[^a-zA-Z0-9]", "")).filter(_.length > 3).mkString(" AND ")

      if (nameTokenized.nonEmpty)s"${fieldName}:(${nameTokenized})" else "*:*"
    }

    val linked = luceneRDD.link(cities.rdd, andLinker, 5)
    linked.cache

    import spark.implicits._
    val linkedDF = spark.createDataFrame(linked.map{ case (left, right) => LinkedRecord(left, right.headOption.map(_.doc.textField(fieldName).toArray, today))})

    linkedDF.write.mode(SaveMode.Overwrite)
      .parquet(s"s3://spark-lucenerdd/timings/v${Utils.Version}/visa-vs-geonames-linkage-result-${sparkInfo.toString}.parquet")

    val end = System.currentTimeMillis()

    spark.createDataFrame(Seq(ElapsedTime(start, end, end - start, today, Utils.Version))).write.mode(SaveMode.Overwrite)
      .parquet(s"s3://spark-lucenerdd/timings/visa-vs-geonames-linkage-timing-${sparkInfo.toString}.parquet")

    // terminate spark context
    spark.stop()
  }
}
