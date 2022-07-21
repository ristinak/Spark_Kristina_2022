package com.github.ristinak

import org.apache.spark.sql.SparkSession

object Day18SparkSQL extends App {
  println(s"Reading CSVs with Scala version: ${util.Properties.versionNumberString}")

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  //  spark.sparkContext.setLogLevel("WARN") //if you did not have log4j.xml set up in src/main/resources
  //problem with above approach is that it would still spew all the initial configuration debeg info
  println(s"Session started on Spark version ${spark.version}")

  // in Scala
  val flightData2015 = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/resources/flight-data/csv/2015-summary.csv") //relative path to our project

  println(s"We have ${flightData2015.count()} rows of data")

  //if we want to use SQL syntax on our DataFrames we create a SQL view
  flightData2015.createOrReplaceTempView("flight_data_2015")

  //now we can use SQL!
  // in Scala
  val sqlWay = spark.sql("""
      SELECT DEST_COUNTRY_NAME, count(1)
      FROM flight_data_2015
      GROUP BY DEST_COUNTRY_NAME
      """)

  //this is the other approach
  val dataFrameWay = flightData2015.groupBy("DEST_COUNTRY_NAME").count()

//  sqlWay.show(10)
//
//  dataFrameWay.show(10)

  //TODO set up logging  log4j2.xml config file
  //TODO set level to warning for both file and console

  //TODO open up flight Data from 2014
  //TODO create SQL view
  //TODO ORDER BY flight counts
  //TODO show top 10 flights

  //you can also show the dataFrameWay as well but we have not looked at that in detail

  val flightData2014 = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/resources/flight-data/csv/2014-summary.csv") //relative path to our project

  println(s"We have ${flightData2014.count()} rows of data")

  //if we want to use SQL syntax on our DataFrames we create a SQL view
  flightData2014.createOrReplaceTempView("flight_data_2014")

  //now we can use SQL!
  // in Scala
  val sqlWay14 = spark.sql("""
      SELECT DEST_COUNTRY_NAME, sum(count) as flight
      FROM flight_data_2014
      GROUP BY DEST_COUNTRY_NAME
      ORDER BY flight DESC
      """)

  //this is the other approach
  val dataFrameWay14 = flightData2014.groupBy("DEST_COUNTRY_NAME").count()

  sqlWay14.show(10)

//  dataFrameWay14.show(10)


}

