package com.github.ristinak

import com.github.ristinak.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, col, dense_rank, desc, max, min, rank, to_date, to_timestamp}

object Day29WindowFunctions extends App {
  println("Ch7: Window Functions")
  val spark = getSpark("Sparky")

//  val filePath = "src/resources/retail-data/by-day/2010-12-01.csv" //here it is a single file but wildcard should still work
  val filePath = "src/resources/retail-data/all/*.csv"
  val df = readDataWithView(spark, filePath)
  df.show(3)



  //Window Functions
  //You can also use window functions to carry out some unique aggregations by either computing
  //some aggregation on a specific “window” of data, which you define by using a reference to the
  //current data. This window specification determines which rows will be passed in to this function.
  //Now this is a bit abstract and probably similar to a standard group-by, so let’s differentiate them
  //a bit more.
  //A group-by takes data, and every row can go only into one grouping. A window function
  //calculates a return value for every input row of a table based on a group of rows, called a frame.
  //Each row can fall into one or more frames. A common use case is to take a look at a rolling
  //average of some value for which each row represents one day. If you were to do this, each row
  //would end up in seven different frames. We cover defining frames a little later, but for your
  //reference, Spark supports three kinds of window functions: ranking functions, analytic functions,
  //and aggregate functions.

  //https://stackoverflow.com/questions/62943941/to-date-fails-to-parse-date-in-spark-3-0
  //multiple ways of solving it
  //change to LEGACY parser
  //convert to timestamp first
  //use different format
  //https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html

  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
//  val dfWithDate = df.withColumn("date", to_date(to_timestamp(col("InvoiceDate")),
  val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"),
//    "M/D/y H:mm")) //newere format
    "MM/d/yyyy H:mm")) //legacy formatting
  dfWithDate.createOrReplaceTempView("dfWithDate")

  println("Number of unique dates")
  spark.sql(
    """
      |SELECT COUNT(DISTINCT(date)) FROM dfWithDate
      |""".stripMargin)
    .show(10)


  val windowSpec = Window
    .partitionBy("CustomerId", "date")
    .orderBy(col("Quantity").desc)
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  //Now we want to use an aggregation function to learn more about each specific customer. An
  //example might be establishing the maximum purchase quantity over all time. To answer this, we
  //use the same aggregation functions that we saw earlier by passing a column name or expression.
  //In addition, we indicate the window specification that defines to which frames of data this
  //function will apply

  val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)

  //You will notice that this returns a column (or expressions). We can now use this in a DataFrame
  //select statement. Before doing so, though, we will create the purchase quantity rank. To do that
  //we use the dense_rank function to determine which date had the maximum purchase quantity
  //for every customer. We use dense_rank as opposed to rank to avoid gaps in the ranking
  //sequence when there are tied values (or in our case, duplicate rows):

  val purchaseDenseRank = dense_rank().over(windowSpec)
  val purchaseRank = rank().over(windowSpec)

  //This also returns a column that we can use in select statements. Now we can perform a select to
  //view the calculated window values:
  //finally we are ready to get some rankings!

  dfWithDate.where("CustomerId IS NOT NULL")
//    .orderBy("CustomerId")
    .orderBy(desc("date"), desc("CustomerId")) //so 2nd tiebreak would be CustomerId
    .select(
      col("CustomerId"),
      col("date"),
      col("Quantity"),
      purchaseRank.alias("quantityRank"),
      purchaseDenseRank.alias("quantityDenseRank"),
      maxPurchaseQuantity.alias("maxPurchaseQuantity"))
    .show(20, false)

  spark.sql(
    """
      |SELECT CustomerId, date, Quantity,
      |rank(Quantity) OVER (PARTITION BY CustomerId, date
      |ORDER BY Quantity DESC NULLS LAST
      |ROWS BETWEEN
      |UNBOUNDED PRECEDING AND
      |CURRENT ROW) as rank,
      |dense_rank(Quantity) OVER (PARTITION BY CustomerId, date
      |ORDER BY Quantity DESC NULLS LAST
      |ROWS BETWEEN
      |UNBOUNDED PRECEDING AND
      |CURRENT ROW) as dRank,
      |max(Quantity) OVER (PARTITION BY CustomerId, date
      |ORDER BY Quantity DESC NULLS LAST
      |ROWS BETWEEN
      |UNBOUNDED PRECEDING AND
      |CURRENT ROW) as maxPurchase,
      |min(Quantity) OVER (PARTITION BY CustomerId, date
      |ORDER BY Quantity DESC NULLS LAST
      |ROWS BETWEEN
      |UNBOUNDED PRECEDING AND
      |CURRENT ROW) as minPurchase
      |FROM dfWithDate WHERE CustomerId IS NOT NULL
      |ORDER BY CustomerId DESC, date ASC, Quantity DESC
      |""".stripMargin)
    .show(20, false)

  //TODO create WindowSpec which partitions by StockCode and date, ordered by Price
  //with rows unbounded preceding and current row

  val windowSpecStock = Window
    .partitionBy("StockCode", "date")
    .orderBy(col("UnitPrice").desc)
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  //create max min dense rank and rank for the price over the newly created WindowSpec

  val maxUnitPrice = max(col("UnitPrice")).over(windowSpecStock)
  val minUnitPrice = min(col("UnitPrice")).over(windowSpecStock)

  val priceDenseRank = dense_rank().over(windowSpecStock)
  val priceRank = rank().over(windowSpecStock)

  //show top 40 results ordered in descending order by StockCode and price
  //show max, min, dense rank and rank for every row as well using our newly created columns(min, max, dense rank and rank)

  dfWithDate.where("StockCode IS NOT NULL")
    .orderBy(desc("StockCode"), desc("UnitPrice"))
    .select(
      col("date"),
      col("StockCode"),
      col("UnitPrice"),
      priceRank.alias("unitPriceRank"),
      priceDenseRank.alias("unitPriceDenseRank"),
      maxUnitPrice.alias("maxUnitPrice"),
      minUnitPrice.alias("minUnitPrice"))
    .show(40, false)




}
