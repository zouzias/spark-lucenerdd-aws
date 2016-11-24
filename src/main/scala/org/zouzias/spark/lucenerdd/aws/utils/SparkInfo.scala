package org.zouzias.spark.lucenerdd.aws.utils


case class SparkInfo(executorInstances: String, executorMemory: String, executorCores: String){
  override def toString(): String = {
    s"${executorInstances}-${executorMemory}-${executorCores}"
  }
}
