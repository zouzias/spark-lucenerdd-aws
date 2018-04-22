package org.zouzias.spark.lucenerdd.aws.search.wikipedia

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.SparkConf
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd._
import org.zouzias.spark.lucenerdd.aws.utils.SearchType.SearchType
import org.zouzias.spark.lucenerdd.aws.utils._
import org.zouzias.spark.lucenerdd.logging.Logging


/**
 * Wikipedia search example
 */
object WikipediaSearchExample extends Logging {

  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName("WikipediaSearchExample")
    implicit val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val executorMemory = conf.get("spark.executor.memory")
    val executorCores = conf.get("spark.executor.cores")
    val executorInstances = conf.get("spark.executor.instances")

    log.info(s"Executor instances: ${executorInstances}")
    log.info(s"Executor cores: ${executorCores}")
    log.info(s"Executor memory: ${executorMemory}")

    logInfo("Loading Wikipedia titles")
    val wiki = Utils.loadWikipediaTitles
    val luceneRDD = LuceneRDD(wiki)
    luceneRDD.cache()
    luceneRDD.count() // To force caching
    logInfo("Wikipedia titles loaded successfully")

    val wikiSample = Utils.sampleTopKWikipediaTitles(1000)

    val sparkInfo = SparkInfo(executorInstances, executorMemory, executorCores)

    timeQueries(luceneRDD, wikiSample, SearchType.TermQuery, sparkInfo)
    timeQueries(luceneRDD, wikiSample, SearchType.PrefixQuery, sparkInfo)
    timeQueries(luceneRDD, wikiSample, SearchType.FuzzyQuery, sparkInfo)
    timeQueries(luceneRDD, wikiSample, SearchType.PhraseQuery, sparkInfo)

    // terminate spark context
    spark.stop()
  }

  def timeQueries(luceneRDD: LuceneRDD[String],
                  wikiSample: List[String],
                  searchType: SearchType,
                  sparkInfo: SparkInfo)
                 (implicit sparkSession: SparkSession): Unit = {

    val today = Utils.dayString()

    val timings = wikiSample.map{ case title =>

      val start = System.currentTimeMillis()
      searchType match{
        case SearchType.TermQuery => luceneRDD.termQuery("_1", title, Utils.topK)
        case SearchType.PrefixQuery => luceneRDD.prefixQuery("_1", title, Utils.topK)
        case SearchType.FuzzyQuery => luceneRDD.fuzzyQuery("_1", title, Utils.FuzzyEditDistance, Utils.topK)
        case SearchType.PhraseQuery => luceneRDD.phraseQuery("_1", title, Utils.topK)
      }
      val end = System.currentTimeMillis()

      Math.max(0L, end - start)
    }

    import sparkSession.implicits._
    val timingsDF = timings.map(Timing(searchType.toString, _, today, Utils.Version)).toDF()
    timingsDF.write.mode(SaveMode.Append).parquet(s"s3://spark-lucenerdd/timings/wikipedia-search-${sparkInfo.toString()}.parquet")
  }
}
