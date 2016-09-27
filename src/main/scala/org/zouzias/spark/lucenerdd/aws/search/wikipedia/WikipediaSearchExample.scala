package org.zouzias.spark.lucenerdd.aws.search.wikipedia

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.SparkConf
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd._
import org.zouzias.spark.lucenerdd.aws.utils._
import org.zouzias.spark.lucenerdd.logging.Logging


/**
 * Wikipedia search example
 */
object WikipediaSearchExample extends Logging {

  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName("WikipediaSearchExample")
    implicit val spark = SparkSession.builder().config(conf).getOrCreate()

    val executorMemory = conf.get("spark.executor.memory")
    val executorCores = conf.get("spark.executor.cores")
    val executorInstances = conf.get("spark.executor.instances")


    log.info(s"Executor instances: ${executorInstances}")
    log.info(s"Executor cores: ${executorCores}")
    log.info(s"Executor memory: ${executorMemory}")

    logInfo("Loading Wikipedia titles")
    val wiki = WikipediaUtils.loadWikipediaTitles
    val luceneRDD = LuceneRDD(wiki)
    luceneRDD.cache()
    luceneRDD.count() // To force caching
    logInfo("Wikipedia titles loaded successfully")

    val wikiSample = WikipediaUtils.sampleTopKWikipediaTitles(1000)

    timeQueries(luceneRDD, wikiSample, SearchInfo(SearchType.TermQuery, executorInstances, executorMemory, executorCores))
    timeQueries(luceneRDD, wikiSample, SearchInfo(SearchType.PrefixQuery, executorInstances, executorMemory, executorCores))
    timeQueries(luceneRDD, wikiSample, SearchInfo(SearchType.FuzzyQuery, executorInstances, executorMemory, executorCores))
    timeQueries(luceneRDD, wikiSample, SearchInfo(SearchType.PhraseQuery, executorInstances, executorMemory, executorCores))

    // terminate spark context
    spark.stop()
  }

  def timeQueries(luceneRDD: LuceneRDD[String], wikiSample: List[String], searchInfo: SearchInfo)
                 (implicit sparkSession: SparkSession): Unit = {
    val timings = wikiSample.map{ case title =>

      val start = System.currentTimeMillis()
      searchInfo.searchType match{
        case SearchType.TermQuery => luceneRDD.termQuery("_1", title, WikipediaUtils.topK)
        case SearchType.PrefixQuery => luceneRDD.prefixQuery("_1", title, WikipediaUtils.topK)
        case SearchType.FuzzyQuery => luceneRDD.fuzzyQuery("_1", title, WikipediaUtils.FuzzyEditDistance, WikipediaUtils.topK)
        case SearchType.PhraseQuery => luceneRDD.phraseQuery("_1", title, WikipediaUtils.topK)
      }
      val end = System.currentTimeMillis()

      Math.max(0L, end - start)
    }


    import sparkSession.implicits._
    val timingsDF = timings.map(Timing(searchInfo.searchType.toString, _)).toDF()

    timingsDF.write.mode(SaveMode.Append).parquet(s"s3://spark-lucenerdd/timings/v${Utils.Version}/timing-search-${WikipediaUtils.dayString}-${searchInfo.toString()}.parquet")
  }
}
