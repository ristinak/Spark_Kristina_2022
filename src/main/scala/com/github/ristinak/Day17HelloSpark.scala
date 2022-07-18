package com.github.ristinak

import org.apache.spark.sql.SparkSession

object Day17HelloSpark extends App {
  println(s"Testing Scala version: ${util.Properties.versionString}")

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  //also session is a common name for the above spark object
  println(s"Session started on Spark version ${spark.version}")

  val myRange = spark.range(1000).toDF("number") //create a single column dataframe (table)
  val divisibleBy5 = myRange.where("number % 5 = 0") //so similaraities with SQL and regular Scala
  divisibleBy5.show(10) //show first 10 rows

  //TODO create range of numbers 0 to 100
  //TODO filter into numbers divisible by 10
  //TODO show the results

  spark.stop() //or .close() if you want to stop the Spark engine before the program stops running
}

