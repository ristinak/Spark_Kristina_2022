package com.github.ristinak

import org.apache.spark.sql.SparkSession

import scala.io.StdIn.readLine

object Day17HelloSpark extends App {
  println(s"Testing Scala version: ${util.Properties.versionString}")

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  //also session is a common name for the above spark object
  println(s"Session started on Spark version ${spark.version}")
  val partitions = 5
  spark.conf.set("spark.sql.shuffle.partitions", partitions.toString)

  val t0 = System.nanoTime()
  val myRange = spark.range(10000).toDF("number") //create a single column dataframe (table)
  val divisibleBy5 = myRange.where("number % 5 = 0") //so similaraities with SQL and regular Scala
//  divisibleBy5.show(10) //show first 10 rows

  //TODO create range of numbers 0 to 100
  val myRange100 = spark.range(100).toDF("number")
  //TODO filter into numbers divisible by 10
  val divisibleBy10 = myRange100.where("number % 10 = 0")
  //TODO show the results
//  divisibleBy10.show()

  val number = divisibleBy10.count()
  println(s"There are $number numbers divisible by 10 in range from 0 to 99.")

  val t1 = System.nanoTime()
  println(s"It took ${(t1-t0)/1000000} milliseconds to execute all that with $partitions partitions.")

//  readLine("Press any key to stop the app: ")

  spark.stop() //or .close() if you want to stop the Spark engine before the program stops running
}

