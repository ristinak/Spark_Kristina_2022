package com.github.ristinak

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{bround, col, corr, countDistinct, expr, lit, max, mean, monotonically_increasing_id, not, ntile, pow, round}

object Day23WorkingWithDataTypes extends App {
  println("Ch6: Working with Different Types\nof Data - Part 2")
  val spark = SparkUtil.getSpark("BasicSpark")

  val filePath = "src/resources/retail-data/by-day/2010-12-01.csv"

  // in Scala
  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true") //we let Spark determine schema
    .load(filePath)

  df.printSchema()
  df.createOrReplaceTempView("dfTable")

  //We mentioned that you can specify Boolean expressions with multiple parts when you use and
  //or or. In Spark, you should always chain together AND filters as a sequential filter

  //The reason for this is that even if Boolean statements are expressed serially (one after the other),
  //Spark will flatten all of these filters into one statement and perform the filter at the same time,
  //creating the and statement for us. Although you can specify your statements explicitly by using
  //and if you like, they’re often easier to understand and to read if you specify them serially

  //OR or
  //statements need to be specified in the same statement:

  // in Scala
  val priceFilter = col("UnitPrice") > 600  //so Filter is a Column type
  val descriptionFilter = col("Description").contains("POSTAGE") //again Column type

  //here we use prexisting order
  df.where(col("StockCode").isin("DOT")).where(priceFilter.or(descriptionFilter))
    .show()

  //so messier with SQL
  spark.sql("SELECT * FROM dfTable " +
    "WHERE StockCode IN ('DOT') AND (UnitPrice > 600 OR Description LIKE '%POSTAGE%')")
    .show()

  //  /
  // Boolean expressions are not just reserved to filters. To filter a DataFrame, you can also just
  //specify a Boolean column:
  //in Scala
  val DOTCodeFilter = col("StockCode") === "DOT"

  //so we add a new Boolean column which shows whether StockCode is named DOT
  df.withColumn("stockCodeDOT", DOTCodeFilter).show(10)
  df.withColumn("stockCodeDOT", DOTCodeFilter)
    .where("stockCodeDOT") //so filters by existance of truth in this column
    .show()

  //so we create a boolean column using 3 filters cond1 && (cond2 || cond3)
  df.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descriptionFilter)))
    .where("isExpensive") //immediately filter by that column
    .select("unitPrice", "isExpensive") //we only show specific columns
    .show(5)

  //If you’re coming from a SQL background, all of these statements should seem quite familiar.
  //Indeed, all of them can be expressed as a where clause. In fact, it’s often easier to just express
  //filters as SQL statements than using the programmatic DataFrame interface and Spark SQL
  //allows us to do this without paying any performance penalty. For example, the following two
  //statements are equivalent:

  df.withColumn("isExpensive", not(col("UnitPrice").leq(250))) //not really needed here
    .filter("isExpensive")
    .select("Description", "UnitPrice").show(5)

  df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))
    .filter("isExpensive")
    .select("Description", "UnitPrice").show(5)

  //personally I would prefer this shorter version :)
  df.withColumn("isExpensive", col("UnitPrice") > 250)
    .filter("isExpensive")
    .select("Description", "UnitPrice").show(5)

  //WARNING
  //One “gotcha” that can come up is if you’re working with null data when creating Boolean expressions.
  //If there is a null in your data, you’ll need to treat things a bit differently. Here’s how you can ensure
  //that you perform a null-safe equivalence test:

  //so if Description might have null / does not exist then we would need to use this eqNullSafe method
  //no alias for this method?
  df.where(col("Description").eqNullSafe("hello")).show()

  //Working with Numbers
  //When working with big data, the second most common task you will do after filtering things is
  //counting things. For the most part, we simply need to express our computation, and that should
  //be valid assuming that we’re working with numerical data types.
  //To fabricate a contrived example, let’s imagine that we found out that we mis-recorded the
  //quantity in our retail dataset and the true quantity is equal to square of (the current quantity * the unit
  //price) + 5. This will introduce our first numerical function as well as the pow function that raises
  //a column to the expressed power

  val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
  df.select(col("CustomerId"),
    col("Quantity"),
    col("UnitPrice"),
    fabricatedQuantity.alias("realQuantity"))
    .show(3)

  //Notice that we were able to multiply our columns together because they were both numerical.
  //Naturally we can add and subtract as necessary, as well. In fact, we can do all of this as a SQL
  //expression, as well

  df.selectExpr(
    "CustomerId", //remember in selectExpr we can select columns and do some work on them at the same tiime
    "Quantity as oldQuant",
    "UnitPrice",
    "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity")
    .show(2)

  //we can use any of the SQL functions available at
  //https://spark.apache.org/docs/2.3.0/api/sql/index.html

  df.selectExpr(
    "CustomerId", //remember in selectExpr we can select columns and do some work on them at the same tiime
    "Quantity as oldQuant",
    "UnitPrice",
    "ROUND((POWER((Quantity * UnitPrice), 2.0) + 5), 2) as realQuantity") //so round to 2 digits after comma
    .show(4)

  df.select(round(col("UnitPrice"), 1).alias("rounded"), col("UnitPrice")).show(5)

  //By default, the round function rounds up if you’re exactly in between two numbers. You can
  //round down by using the bround

  // in Scala

  df.select(round(lit("2.6")), bround(lit("2.6"))).show(2)

  //only difference is at the middle
  df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)

  df.select(round(lit("2.4")), bround(lit("2.4"))).show(2)

  //https://en.wikipedia.org/wiki/Pearson_correlation_coefficient
  //Another numerical task is to compute the correlation of two columns. For example, we can see
  //the Pearson correlation coefficient for two columns to see if cheaper things are typically bought
  //in greater quantities. We can do this through a function as well as through the DataFrame
  //statistic methods

  //so we are answering a question
  //whether when units get more expensive do we buy more or less or is it pretty random
  //so max inverse correlation would be -1, no correlation would be 0 and positive correlation would be 1(max)

  val corrCoefficient = df.stat.corr("Quantity", "UnitPrice") //you can save this single double value as well
  println(s"Pearson correlations coefficient for Quantity and UnitPrice is $corrCoefficient")
  df.select(corr("Quantity", "UnitPrice")).show()

  //so -0.04 that means that Quantity and UnitPrice are not correlated, just a tiny negative correlation

  //Another common task is to compute summary statistics for a column or set of columns. We can
  //use the describe method to achieve exactly this. This will take all numeric columns and
  //calculate the count, mean, standard deviation, min, and max. You should use this primarily for
  //viewing in the console because the schema might change in the future:

  // in Scala
  df.describe().show()


  //  If you need these exact numbers, you can also perform this as an aggregation yourself by
  //    importing the functions and applying them to the columns that you need:
  //import org.apache.spark.sql.functions.{count, mean, stddev_pop, min, max}

  df.select(mean("Quantity"), mean("UnitPrice"), max("UnitPrice")).show()

  //There are a number of statistical functions available in the StatFunctions Package (accessible
  //using stat as we see in the code block below). These are DataFrame methods that you can use
  //to calculate a variety of different things. For instance, you can calculate either exact or
  //approximate quantiles of your data using the approxQuantile method

  val colName = "UnitPrice"
  val quantileProbs = Array(0.1, 0.4, 0.5, 0.6, 0.9, 0.99) //checking different quantiles
  val relError = 0.05
  val quantilePrices = df.stat.approxQuantile("UnitPrice", quantileProbs, relError) // 2.51

  for ((prob, price) <- quantileProbs zip quantilePrices) {
    println(s"Quantile ${prob} - price ${price}")
  }

  //so the median price is aproximately 2.51

  //TODO mini task check what happens if you change relError to 0.01
  //so default is quartile (4 cut points)
  def getQuantiles(df: DataFrame, colName:String, quantileProbs:Array[Double]=Array(0.25,0.5,0.75,0.99), relError:Double=0.05):Array[Double] = {
    df.stat.approxQuantile(colName, quantileProbs, relError)
  }

  def printQuantiles(df: DataFrame, colName:String, quantileProbs:Array[Double]=Array(0.25,0.5,0.75,0.99), relError:Double=0.05): Unit = {
    val quantiles = getQuantiles(df, colName, quantileProbs, relError)
    println(s"For column $colName")
    for ((prob, cutPoint) <- quantileProbs zip quantiles) {
      println(s"Quantile ${prob} so aprox ${Math.round(prob*100)}% of data is covered - cutPoint ${cutPoint}")
    }
  }

  val deciles = (1 to 10).map(n => n.toDouble/10).toArray
  printQuantiles(df, "UnitPrice", deciles)

  //ventiles 20-quantiles
  val ventiles = (1 to 20).map(n => n.toDouble/20).toArray
  printQuantiles(df, "UnitPrice", ventiles)

  //You also can use this to see a cross-tabulation or frequent item pairs (be careful, this output will
  //be large and is omitted for this reason):
  //https://en.wikipedia.org/wiki/Contingency_table
  // df.stat.crosstab("StockCode", "Quantity").show()



  //we might want to have some distinct counts for ALL columns
  //https://stackoverflow.com/questions/40888946/spark-dataframe-count-distinct-values-of-every-column

  df.groupBy("Country").count().show()
  df.agg(countDistinct("Country")).show()

  for (col <- df.columns) {
    //    val count = df.agg(countDistinct(col))
    //this would get you breakdown by distinct value
    //    val count = df.groupBy(col).count().collect()
    val count = df.groupBy(col).count().count() //so first Count is aggregation second count is row count in our results
    println(s"Column $col has $count distinct values")
  }

  //so here crostab would be huge here we could do something like
  //quantile breakdown by country :)
  //so first we would need to get quantile rank for each row
  //  val windowSpec  = Window.partitionBy("UnitPrice").orderBy("UnitPrice")
  //here we do not need to partition we want rank over ALL of the rows
  val windowSpec  = Window.partitionBy().orderBy("UnitPrice") //TODO check if window applies to whole DF
  //in chapter 7

  df.select(col("Description"), col("UnitPrice"),
    ntile(4).over(windowSpec).alias("quantile_rank") //we are trying to get rank 1 to 4 for each row
  ).show()
  //TODO double check syntax for quartile ranking

  //As a last note, we can also add a unique ID to each row by using the function
  //monotonically_increasing_id. This function generates a unique value for each row, starting
  //with 0:
  df.select(monotonically_increasing_id()).show(5)


}


