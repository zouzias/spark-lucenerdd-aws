package org.zouzias.spark.lucenerdd.aws.utils


object SearchType extends Enumeration{
  type SearchType = Value
  val TermQuery = Value(0)
  val PrefixQuery = Value(1)
  val FuzzyQuery = Value(2)
  val PhraseQuery = Value(3)
}
