package org.zouzias.spark.lucenerdd.aws.utils

case class LinkedRecord(left: String, right: Option[Array[String]])

case class LinkedSpatialRecord(left: (String, String), right: Option[String])
