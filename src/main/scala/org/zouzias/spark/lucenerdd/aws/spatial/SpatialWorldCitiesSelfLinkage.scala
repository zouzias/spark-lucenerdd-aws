package org.zouzias.spark.lucenerdd.aws.spatial

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.zouzias.spark.lucenerdd.aws.linkage.ElapsedTime
import org.zouzias.spark.lucenerdd.aws.utils.{LinkedSpatialRecord, Utils}
import org.zouzias.spark.lucenerdd.logging.Logging
import org.zouzias.spark.lucenerdd.spatial.shape._
import org.zouzias.spark.lucenerdd._
import org.zouzias.spark.lucenerdd.spatial.shape.rdds.ShapeLuceneRDD

/**
 * Spatial world cities self-linkage
 */
object SpatialWorldCitiesSelfLinkage extends Logging {

    def main(args: Array[String]) {

      // initialise spark context
      val conf = new SparkConf().setAppName(SpatialWorldCitiesSelfLinkage.getClass.getName)

      implicit val spark = SparkSession.builder().config(conf).getOrCreate()
      import spark.implicits._

      val today = Utils.dayString()
      val executorMemory = conf.get("spark.executor.memory")
      val executorCores = conf.get("spark.executor.cores")
      val executorInstances = conf.get("spark.executor.instances")
      val fieldName = "City"


      log.info(s"Executor instances: ${executorInstances}")
      log.info(s"Executor cores: ${executorCores}")
      log.info(s"Executor memory: ${executorMemory}")

      val start = System.currentTimeMillis()

      val citiesDF = spark.read.parquet("s3://recordlinkage/world-cities-maxmind.parquet").repartition(60)
      citiesDF.cache
      val total = citiesDF.count
      logInfo(s"Cities: ${total}")

      val cities = citiesDF.select("Latitude", "Longitude", "City", "Country")
        .map(row => ((row.getString(1).toDouble, row.getString(0).toDouble), (row.getString(2), row.getString(3))))


      val shapes = ShapeLuceneRDD(cities)

      shapes.cache
      shapes.count
      logInfo("Max mind cities loaded successfully")

      val coord = (x: ((Double, Double), (String, String))) => {
        x._1
      }

      // Link and fetch top-3
      val linkage = shapes.linkByRadius(cities.rdd, coord, 3)


      import spark.implicits._
      val asCaseClass =linkage.map{ case (left, right) => LinkedSpatialRecord(left._2, right.headOption.flatMap(_.doc.textField(fieldName)))}
      val linkedDF = spark.createDataFrame(asCaseClass)

      linkedDF.write.mode(SaveMode.Overwrite)
        .parquet(s"s3://spark-lucenerdd/timings/v${Utils.Version}/max-mind-cities-linkage-result-${today}-${executorMemory}-${executorInstances}-${executorCores}.parquet")

      val end = System.currentTimeMillis()

      spark.createDataFrame(Seq(ElapsedTime(start, end, end - start, today, Utils.Version))).write.mode(SaveMode.Overwrite)
        .parquet(s"s3://spark-lucenerdd/timings/max-mind-cities-linkage-timing-${executorMemory}-${executorInstances}-${executorCores}.parquet")

      // terminate spark context
      spark.stop()
    }

}
