package com.github.ristinak

import com.github.ristinak.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.functions.{col, collect_list, collect_set, corr, covar_pop, covar_samp, expr, kurtosis, lit, mean, skewness, stddev_pop, stddev_samp, var_pop, var_samp}

object Day28StatisticalFunExercise extends App {
  println("Ch7: Statistics Functions")

  //TODO
  //load March 8th of 2011 CSV
  //lets show avg, variance, std, skew, kurtosis, correlation and population covariance

  val spark = getSpark("Sparky", verbose=false)
  val filePath = "src/resources/retail-data/by-day/2011-03-08.csv"
  val df = readDataWithView(spark, filePath, printSchema=false)

  df.select(
    mean("Quantity"),
    var_pop("Quantity"),
    stddev_pop("Quantity"),
    skewness("Quantity"),
    kurtosis("Quantity"),
    corr("Quantity", "StockCode"),
    covar_pop("Quantity", "StockCode"))
    .show()

  //TODO transform unique Countries for that day into a regular Scala Array of strings
  //you could use SQL distinct of course - do not hav eto use collect_set but you can :)

  val distCountries = df.agg(collect_set("Country"))
  val countryArray = distCountries.collectAsList().toArray().map(_.toString)
  val countries = countryArray(0).split("\\(|\\)").tail.dropRight(1)
  println("My solution result:")
  println(countries.mkString(","))

  //TODO same as above, classroom solution:

  val dfMarch = readDataWithView(spark, filePath)

  dfMarch.select(mean( "Quantity"),
    var_pop( "Quantity"),
    var_samp("Quantity"),
    stddev_pop("Quantity"),
    stddev_samp("Quantity"),
    skewness("Quantity"),
    kurtosis("Quantity")).show()

  val countries1 = dfMarch.agg(collect_set("Country")).collect()
  println(countries1.mkString) //prints but we actually want the strings
  //  val countryStrings = countries.map(_.getString(0))
  println("Printing row by row")
  for (row <- countries1) {
    println(row)
  }
  //turns out we only have a single row which we would need to split using regex

  val distinctCountries1 = spark.sql(
    """
      |SELECT DISTINCT(country) FROM dfTable
      |""".stripMargin)

  distinctCountries1.show()
  val countryStringsRows = distinctCountries1.collect()
  val countryStrings = countryStringsRows.map(_.getString(0))
  //should be a regular array of strings
  println(countryStrings.mkString(","))

  //so this is how we could get out a sequence saved in a single cell of a dataframe
  //check chapter 6 again on complex types
  val countrySeq:Seq[String] = countries1.head.getSeq(0) //first column for our first row
  println(countrySeq.mkString("[",",","]"))



}


