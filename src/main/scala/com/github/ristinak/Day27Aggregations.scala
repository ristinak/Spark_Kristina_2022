package com.github.ristinak

import com.github.ristinak.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.functions.{approx_count_distinct, count, countDistinct}

object Day27Aggregations extends App {
  println("Ch7: Aggregations")
  val spark = getSpark("Sparky")

  //Chapter 7. Aggregations
  //Aggregating is the act of collecting something together and is a cornerstone of big data analytics.
  //In an aggregation, you will specify a key or grouping and an aggregation function that specifies
  //how you should transform one or more columns. This function must produce one result for each
  //group, given multiple input values. Spark’s aggregation capabilities are sophisticated and mature,
  //with a variety of different use cases and possibilities. In general, you use aggregations to
  //summarize numerical data usually by means of some grouping. This might be a summation, a
  //product, or simple counting. Also, with Spark you can aggregate any kind of value into an array,
  //list, or map, as we will see in “Aggregating to Complex Types”.
  //In addition to working with any type of values, Spark also allows us to create the following
  //groupings types:
  //The simplest grouping is to just summarize a complete DataFrame by performing an
  //aggregation in a select statement.
  //A “group by” allows you to specify one or more keys as well as one or more
  //aggregation functions to transform the value columns.
  //A “window” gives you the ability to specify one or more keys as well as one or more
  //aggregation functions to transform the value columns. However, the rows input to the
  //function are somehow related to the current row.
  //A “grouping set,” which you can use to aggregate at multiple different levels. Grouping
  //sets are available as a primitive in SQL and via rollups and cubes in DataFrames.
  //A “rollup” makes it possible for you to specify one or more keys as well as one or more
  //aggregation functions to transform the value columns, which will be summarized
  //hierarchically.
  //A “cube” allows you to specify one or more keys as well as one or more aggregation
  //functions to transform the value columns, which will be summarized across all
  //combinations of columns.

  //Important:
  //Each grouping returns a RelationalGroupedDataset on which we specify our aggregations
  //which we will want to transform into a DataFrame at some point

//  val filePath = "src/resources/retail-data/all/online-retail-dataset.csv"
  val filePath = "src/resources/retail-data/all/*.csv" //here it is a single file but wildcard should still work
  val df = readDataWithView(spark, filePath)

//  df.printSchema() //done by readDataWithView
  df.show(5, false)

  //As mentioned, basic aggregations apply to an entire DataFrame. The simplest example is the
  //count method

  println(df.count(), "rows")

  //If you’ve been reading this book chapter by chapter, you know that count is actually an action as
  //opposed to a transformation, and so it returns immediately. You can use count to get an idea of
  //the total size of your dataset but another common pattern is to use it to cache an entire
  //DataFrame in memory, just like we did in this example.
  //Now, this method is a bit of an outlier because it exists as a method (in this case) as opposed to a
  //function and is eagerly evaluated instead of a lazy transformation. In the next section, we will see
  //count used as a lazy function, as well.

  //most things in spark are lazy - only done when really really needed, otherwise we are just writing a steps to transformation
  //which Spark engine will be able to optimize

  //again .show() of course is eager(immediate) as is .collect()

  //count
  //The first function worth going over is count, except in this example it will perform as a
  //transformation instead of an action. In this case, we can do one of two things: specify a specific
  //column to count, or all the columns by using count(*) or count(1) to represent that we want to
  //count every row as the literal one, as shown in this example

  df.select(count("StockCode")).show()

  //countDistinct
  //Sometimes, the total number is not relevant; rather, it’s the number of unique groups that you
  //want. To get this number, you can use the countDistinct function. This is a bit more relevant
  //for individual columns:

  df.select(countDistinct("StockCode")).show()

  //approx_count_distinct
  //Often, we find ourselves working with large datasets and the exact distinct count is irrelevant.
  //There are times when an approximation to a certain degree of accuracy will work just fine, and
  //for that, you can use the approx_count_distinct function

  df.select(approx_count_distinct("StockCode", 0.1)).show()
  //default is RSD of 0.05 below
  //https://en.wikipedia.org/wiki/Coefficient_of_variation - RSD
  df.select(approx_count_distinct("StockCode")).show()
  df.select(approx_count_distinct("StockCode", 0.01)).show()

  //You will notice that approx_count_distinct took another parameter with which you can
  //specify the maximum estimation error allowed. In this case, we specified a rather large error and
  //thus receive an answer that is quite far off but does complete more quickly than countDistinct.
  //You will see much greater performance gains with larger datasets

  //TODO simple task find count, distinct count and also aproximate distinct count (with default RSD)
  // for InvoiceNo, CustomerID AND UnitPrice columns
  //of course count should be the same for all of these because that is the number of rows

}
