package org.zouzias.spark.lucenerdd.aws.linkage.blocked

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.index.Term
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.search.{BooleanQuery, Query, TermQuery}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd.aws.utils.{SparkInfo, Utils}
import org.zouzias.spark.lucenerdd.logging.Logging

import scala.collection.mutable

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

    val illinoisFullDF = spark.read.parquet("s3://recordlinkage/illinois-donors.parquet")
    logInfo(s"Loaded ${illinoisFullDF.count} records")

    def analyze(text: String): Array[String] = {
      val analyzer = new StandardAnalyzer()
      val result = mutable.ArrayBuilder.make[String]()
      val tokenStream = analyzer.tokenStream("text", text)
      val attr = tokenStream.addAttribute(classOf[CharTermAttribute])
      tokenStream.reset()
      while (tokenStream.incrementToken() ) {
        result.+=(attr.toString)
      }
      result.result()
    }


    val illinoisDF = illinoisFullDF.select("RctNum", "LastOnlyName", "FirstName", "City")


    // Custom linker
    val linker: Row => Query = row => {
      val name = row.getString(row.fieldIndex("FirstName"))
      val lastName = row.getString(row.fieldIndex("LastOnlyName"))

      val booleanQuery = new BooleanQuery.Builder()
      if (name != null) {
        analyze(name)
          .filter(_.length >= 2).foreach { nameToken =>
          booleanQuery.add(new TermQuery(new Term("FirstName", nameToken.toLowerCase)), Occur.SHOULD)
        }
      }

      if ( lastName != null) {
        analyze(lastName)
          .filter(_.length >= 2).foreach { lastNameToken =>
          booleanQuery.add(new TermQuery(new Term("LastOnlyName", lastNameToken.toLowerCase)), Occur.SHOULD)
        }
      }

      booleanQuery.setMinimumNumberShouldMatch(1)
      booleanQuery.build()
    }

    // Block on the City field
    val blockingFields = Array("City")

    // Block entity linkage
    val linkedResults = LuceneRDD.blockDedup(illinoisDF, linker, blockingFields)

    val linkageResults: DataFrame = spark.createDataFrame(linkedResults
      .filter(_._2.nonEmpty)
      .map{ case (left, topDocs) =>
        (topDocs.head.getString(topDocs.head.fieldIndex("RctNum")),
          left.getString(left.fieldIndex("RctNum"))
        )
      })
      .toDF("left_id", "right_id")
      .filter($"left_id".equalTo($"right_id"))

    val correctHits: Double = linkageResults.count()
    logInfo(s"Correct hits are $correctHits")
    val total: Double = illinoisDF.count
    val accuracy = correctHits / total
    val end = System.currentTimeMillis()

    logInfo("=" * 40)
    logInfo(s"|| Elapsed time: ${(end - start) / 1000.0} seconds ||")
    logInfo("=" * 40)

    logInfo("*" * 40)
    logInfo(s"* Accuracy of deduplication is $accuracy *")
    logInfo("*" * 40)

    // terminate spark context
    spark.stop()
  }
}
