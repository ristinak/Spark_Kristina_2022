package com.github.ristinak

import com.github.ristinak.SparkUtil.{getSpark, readDataWithView}

object Day30ExerciseJoin extends App {
  println("Ch8: Joins")
  val spark = getSpark("Sparky")

  //TODO inner join src/resources/retail-data/all/online-retail-dataset.csv
  //TODO with src/resources/retail-data/customers.csv
  //on Customer ID in first matching Id in second
  //in other words I want to see the purchases of these customers with their full names
  //try to show it both spark API and spark SQL

  val filePathSales = "src/resources/retail-data/all/*.csv"
  val dfSales = readDataWithView(spark, filePathSales, viewName = "dfTableSales", printSchema = false)

  val filePathCustomers = "src/resources/retail-data/customers.csv"
  val dfCustomers = readDataWithView(spark, filePathCustomers, viewName = "dfTableCustomers", printSchema = false)

  val joinExpression = dfSales.col("CustomerID") === dfCustomers.col("Id")

  dfSales.join(dfCustomers, joinExpression).show()

  //same in spark sql
  spark.sql(
    """
      |SELECT * FROM dfTableSales JOIN dfTableCustomers
      |ON dfTableSales.CustomerID = dfTableCustomers.Id
      |""".stripMargin)
    .show()



}

