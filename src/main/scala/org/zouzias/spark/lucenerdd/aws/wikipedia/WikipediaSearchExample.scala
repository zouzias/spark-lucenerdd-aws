package org.zouzias.spark.lucenerdd.aws.wikipedia

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd._
import org.zouzias.spark.lucenerdd.aws.utils.{SearchInfo, SearchType, Timing, WikipediaUtils}


/**
 * Wikipedia search example
 */
object WikipediaSearchExample extends Logging {

  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName("WikipediaSearchExample")

    //
    implicit val sc = new SparkContext(conf)
    implicit val sqlContext = new SQLContext(sc)

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
    sc.stop()

  }

  def timeQueries(luceneRDD: LuceneRDD[String], wikiSample: List[String], searchInfo: SearchInfo)(implicit sqlContext: SQLContext): Unit = {
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


    import sqlContext.implicits._
    val timingsDF = timings.map(Timing(searchInfo.searchType.toString, _)).toDF()

    timingsDF.write.mode(SaveMode.Append).parquet(s"s3://spark-lucenerdd/timings/v0.0.18/timing-search-${WikipediaUtils.dayString}-${searchInfo.toString()}.parquet")

  }

}
