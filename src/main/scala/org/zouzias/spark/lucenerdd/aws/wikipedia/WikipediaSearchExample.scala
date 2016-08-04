package org.zouzias.spark.lucenerdd.aws.wikipedia

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd._
import org.zouzias.spark.lucenerdd.aws.utils.SearchType.SearchType
import org.zouzias.spark.lucenerdd.aws.utils.{SearchType, Timing, WikipediaUtils}


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


    logInfo("Loading Wikipedia titles")
    val wiki = WikipediaUtils.loadWikipediaTitles
    val luceneRDD = LuceneRDD(wiki)
    luceneRDD.cache()
    logInfo("Wikipedia titles loaded successfully")

    val wikiSample = WikipediaUtils.sampleTopKWikipediaTitles(1000)

    timeQueries(luceneRDD, wikiSample, SearchType.TermQuery)
    timeQueries(luceneRDD, wikiSample, SearchType.PrefixQuery)
    timeQueries(luceneRDD, wikiSample, SearchType.FuzzyQuery)
    timeQueries(luceneRDD, wikiSample, SearchType.PhraseQuery)

    // terminate spark context
    sc.stop()

  }

  def timeQueries(luceneRDD: LuceneRDD[String], wikiSample: List[String], searchType: SearchType)(implicit sqlContext: SQLContext): Unit = {
    val timings = wikiSample.map{ case title =>

      val start = System.currentTimeMillis()
      searchType match{
        case SearchType.TermQuery => luceneRDD.termQuery("_1", title, WikipediaUtils.topK)
        case SearchType.PrefixQuery => luceneRDD.prefixQuery("_1", title, WikipediaUtils.topK)
        case SearchType.FuzzyQuery => luceneRDD.fuzzyQuery("_1", title, WikipediaUtils.topK)
        case SearchType.PhraseQuery => luceneRDD.phraseQuery("_1", title, WikipediaUtils.topK)
      }
      val end = System.currentTimeMillis()

      Math.max(0L, end - start)
    }

    val searchTypeClean = searchType.toString.toLowerCase

    import sqlContext.implicits._
    val timingsDF = timings.map(Timing(searchType.toString, _)).toDF()

    timingsDF.write.mode(SaveMode.Append).parquet(s"s3://spark-lucenerdd/timings/v0.0.18/${searchTypeClean}-${WikipediaUtils.dayString}")

  }

}
