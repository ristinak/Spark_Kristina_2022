package com.github.ristinak

import org.apache.spark.sql.SparkSession

object Day19TestingHadoop extends App {
  println(s"Testing Hadoop with Scala : ${util.Properties.versionNumberString}")
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${spark.version}")
}
