package org.zouzias.spark.lucenerdd.aws.utils

case class Timing(searchType: String, elapsedTime: Long, day: String, version: String)
case class IndexingTiming(elapsedTime: Long, day: String, version: String)
