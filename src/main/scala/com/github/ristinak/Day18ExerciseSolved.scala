package com.github.ristinak

import org.apache.spark.sql.functions.{desc, max}
//import org.apache.spark.sql.functions. //lot of functions you probably do not want to import all of them
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Encoder, Encoders}

// in Scala
case class Flight(DEST_COUNTRY_NAME: String,
                  ORIGIN_COUNTRY_NAME: String,
                  count: BigInt)

object Day18SExerciseSolved extends App {
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

  println(s"We have ${flightData2015.count()} rows of data in flightData2015")

  //if we want to use SQL syntax on our DataFrames we create a SQL view
  flightData2015.createOrReplaceTempView("flight_data_2015")

  //now we can use SQL!
  // in Scala
  val sqlWay = spark.sql("""
      SELECT DEST_COUNTRY_NAME, count(1)
      FROM flight_data_2015
      GROUP BY DEST_COUNTRY_NAME
      """)

  //this is the other approach using methods built into DataFrame
  val dataFrameWay = flightData2015.groupBy("DEST_COUNTRY_NAME").count()

//  sqlWay.show(10)
//
//  dataFrameWay.show(10)

  sqlWay.explain()
  dataFrameWay.explain()

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
    .csv("src/resources/flight-data/csv/2014-summary.csv")
  //TODO create SQL view

  flightData2014.createOrReplaceTempView("flight_data_2014")
  //TODO ORDER BY flight counts
  val sqlFly = spark.sql("""
      SELECT DEST_COUNTRY_NAME, SUM(count) as flight
      FROM flight_data_2014
      GROUP BY DEST_COUNTRY_NAME
      ORDER BY flight DESC
      """)
  //TODO show top 10 flights
//  sqlFly.show(10)

  //again two approaches to do the same thing
//  spark.sql("SELECT max(count) from flight_data_2015").show() //show everything in this case just 1 row
//  flightData2015.select(max("count")).show() //intellij feels that functions.max would be less confusing


  //very similar to the exercise except we use limit 5 here
  val maxSql = spark.sql("""
      SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
      FROM flight_data_2015
      GROUP BY DEST_COUNTRY_NAME
      ORDER BY sum(count) DESC
      LIMIT 5
      """)
//  maxSql.show()

  //Now, let’s move to the DataFrame syntax that is semantically similar but slightly different in
  //implementation and ordering
  flightData2015
    .groupBy("DEST_COUNTRY_NAME")
    .sum("count")
    .withColumnRenamed("sum(count)", "destination_total")
    .sort(desc("destination_total"))
    .limit(5)
    .show()

  // I'll save the output into a Scala Array (of rows):
  val arrTopDestinations =  flightData2015
    .groupBy("DEST_COUNTRY_NAME")
    .sum("count")
    .withColumnRenamed("sum(count)", "destination_total")
    .sort(desc("destination_total"))
    .limit(5)
    .collect()

  println(s"Top 5 destinations are:\n${arrTopDestinations.mkString("\n")}")


  //results and execution should be the same we can always check with explain() instead of show()

  //we do not have to print the results we can save to many different formats
  //here is an example we will use later on
  //FIXME ERROR org.apache.spark.sql.execution.datasources.FileFormatWriter - Aborting job 11a3f18a-9d05-4a9e-8a0d-91134d4c7b9c.
  //java.lang.UnsatisfiedLinkError: 'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(java.lang.String, int)'
  //	at org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(Native Method) ~[hadoop-client-api-3.3.2.jar:?]
  //  flightData2015
  //    .groupBy("DEST_COUNTRY_NAME")
  //    .sum("count")
  //    .withColumnRenamed("sum(count)", "destination_total")
  //    .sort(desc("destination_total"))
  //    .toDF()
  //    .write
  //    .format("csv")
  //    .mode("overwrite")
  ////    .option("sep","\t")
  //    .save("src/resources/flight-data/csv/top_destinations_2015.csv")


  //  val flightsDF = spark.read
  //    .parquet("src/resources/flight-data/parquet/2010-summary.parquet/")

  //one of those dark corners of Scala called implicits, which is a bit magical

  //  implicit val enc: Encoder[Flight] = Encoders.product[Flight]
  //  //we needed the above line so the below type conversion works
  //  val flights = flightsDF.as[Flight]
  //  val flightsArray = flights.collect() //now we have local storage of our Flights
  //  //now we can use regular Scala methods
  //  println(s"We have information on ${flightsArray.length} flights")
  //  val sortedFlights = flightsArray.sortBy(_.count)
  //  println(sortedFlights.take(5).mkString("\n"))
  //
}

