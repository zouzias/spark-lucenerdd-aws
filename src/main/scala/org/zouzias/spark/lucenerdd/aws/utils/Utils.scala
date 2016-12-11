package org.zouzias.spark.lucenerdd.aws.utils

// CAUTION: Do not remove this (sbt-build-info)
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.zouzias.spark.lucenerdd.aws.BuildInfo

object Utils {

  val FuzzyEditDistance = 1
  val topK = 10

  def loadWikipediaTitles(implicit sparkSession: SparkSession): RDD[String] = {
    import sparkSession.sqlContext.implicits._
    sparkSession.read.parquet("s3://spark-lucenerdd/wikipedia/enwiki-latest-all-titles.parquet")
      .map(row => row.getString(0)).map(_.replaceAll("_", " ")).map(_.replaceAll("[^a-zA-Z0-9\\s]", ""))
      .rdd
  }

  def sampleTopKWikipediaTitles(k: Int)(implicit sparkSession: SparkSession): List[String] = {
    loadWikipediaTitles.sample(false, 0.01).take(k).toList
  }

  def dayString(): String = {
    val date = new DateTime()
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    formatter.print(date)
  }

  val Version = BuildInfo.version
}
