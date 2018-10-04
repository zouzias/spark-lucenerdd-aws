package org.zouzias.spark.lucenerdd.aws.linkage.blocked

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd.aws.linkage.ElapsedTime
import org.zouzias.spark.lucenerdd.aws.utils.{LinkedRecord, SparkInfo, Utils}
import org.zouzias.spark.lucenerdd.logging.Logging

/**
 * Geonames deduplication example
 */
object LinkageBlockIllinoisDonorsExample extends Logging {

  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName("LinkageBlockIllinoisDonorsExample")

    implicit val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val today = Utils.dayString()
    val executorMemory = conf.get("spark.executor.memory")
    val executorCores = conf.get("spark.executor.cores")
    val executorInstances = conf.get("spark.executor.instances")
    val fieldName = "name"

    log.info(s"Executor instances: $executorInstances")
    log.info(s"Executor cores: $executorCores")
    log.info(s"Executor memory: $executorMemory")
    val sparkInfo = SparkInfo(executorInstances, executorMemory, executorCores)


    val start = System.currentTimeMillis()

    logInfo("Loading Geonames Cities")
    val citiesDF = spark.read.parquet("s3://recordlinkage/geonames-usa-cities.parquet")

    val andLinker = (row: Row) => {
      val cityName = row.getString(row.fieldIndex(fieldName))
      val nameTokenized = cityName.split(" ").map(_.replaceAll("[^a-zA-Z0-9]", "")).filter(_.length > 3).mkString(" OR ")

      if (nameTokenized.nonEmpty) s"$fieldName:($nameTokenized)" else "*:*"
    }

    val linked = LuceneRDD.blockDedup(citiesDF, andLinker, Array("featureclass"))


    val linkedDF = linked.map{ case (l, r) =>
      val docs = r.flatMap(_.doc.textField(fieldName)).toArray
      LinkedRecord(l.getString(l.fieldIndex(fieldName)),
        Some(docs),
        today)
    }.toDF()

    linkedDF.write.mode(SaveMode.Append)
      .parquet(s"s3://spark-lucenerdd/timings/v${Utils.Version}/dedup-blocked-geonames-result-${sparkInfo}.parquet")

    val end = System.currentTimeMillis()

    spark.createDataFrame(Seq(ElapsedTime(start, end, end - start, today, Utils.Version)))
      .write
      .mode(SaveMode.Append)
      .parquet(s"s3://spark-lucenerdd/timings/visa-vs-geonames-linkage-timing-${sparkInfo}.parquet")

    // terminate spark context
    spark.stop()
  }
}
