package com.github.ristinak

import com.github.ristinak.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.functions.{expr, monotonically_increasing_id}

object Day37CumulativeSum extends App {
  val spark = getSpark("SparkY")

  val defaultSrc = "src/resources/csv/fruits.csv"
  //so our src will either be default file  or the first argument supplied by user
  val src = if (args.length >= 1) args(0) else defaultSrc

  println(s"My Src file will be $src")

  val df = readDataWithView(spark, src)
    .withColumn("id",monotonically_increasing_id)
    .withColumn("total", expr("quantity * price"))

  //I have to create the view again since original view does not have total column
  df.createOrReplaceTempView("dfTable")
  df.show()

  val sumDF = spark.sql(
    """
      |SELECT *, SUM(total) OVER
      |(ROWS BETWEEN
      |UNBOUNDED PRECEDING AND
      |CURRENT ROW) as CSUM,
      |SUM(total) OVER
      |(PARTITION BY fruit
      |ROWS BETWEEN
      |UNBOUNDED PRECEDING AND
      |CURRENT ROW) as SUMFRUIT
      |FROM dfTable
      |ORDER BY id ASC
      |""".stripMargin)
  //so our sum is over default ordering an no partitions
  sumDF.show(false)

  //TODO do the same using spark functions, also check ROUND function
  //you can use WindowSPe see Day 29

  val sumRounder = sumDF
    .withColumn("SUMFRUIT", expr("ROUND(`SUMFRUIT`, 2)"))
  //you could do same with round(col functions as well
  //so we had to use backticks for column names, same syntax should work above
  //I overwrite the old SUMFRUIT

  sumRounder.show()
}
