package com.github.ristinak

import com.github.ristinak.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.functions.{col, collect_list, collect_set, corr, covar_pop, covar_samp, expr, kurtosis, lit, mean, skewness, stddev_pop, stddev_samp, var_pop, var_samp}

object Day28StatisticalFunExercise extends App {
  println("Ch7: Statistics Functions")

  //TODO
  //load March 8th of 2011 CSV
  //lets show avg, variance, std, skew, kurtosis, correlation and population covariance

  val spark = getSpark("Sparky", verbose=false)
  val df = readDataWithView(spark, "src/resources/retail-data/by-day/2011-03-08.csv", printSchema=false)

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

  println(countries.mkString(","))









}


