package com.github.ristinak

import com.github.ristinak.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.functions._

object Day25DateExerciseSolved extends App {
  val spark = getSpark("StringFun")

  val filePath = "src/resources/retail-data/by-day/2010-12-01.csv"

  val df = readDataWithView(spark, filePath)

  df.withColumn("Current_date", current_date())
    .withColumn("Current_timestamp", current_timestamp())
    .withColumn("Days_since_invoiceDate", datediff(col("Current_date"), col("InvoiceDate")))
    .withColumn("Months_since_invoiceDate", months_between(col("Current_date"), col("InvoiceDate")))
    .show(10, false)

  spark.sql(
    """
      |SELECT *,
      |current_date,
      |current_timestamp,
      |datediff(day, InvoiceDate, current_date) AS dateDiff,
      |datediff(month, InvoiceDate, current_date) AS monthDiff
      |FROM dfTable
      |""".stripMargin)
    .show(5, false)


}
