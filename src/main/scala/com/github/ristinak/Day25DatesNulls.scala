package com.github.ristinak

import com.github.ristinak.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.functions.{coalesce, col, expr, lit, to_date, to_timestamp}

object Day25DatesNulls extends App {
  println("Ch6: Dealing with Nulls in Data")
  val spark = getSpark("StringFun")

  val filePath = "src/resources/retail-data/by-day/2010-12-01.csv"

  val df = readDataWithView(spark, filePath)

  //Spark will not throw an error if it cannot parse the date; rather, it will just return null. This can
  //be a bit tricky in larger pipelines because you might be expecting your data in one format and
  //getting it in another. To illustrate, let’s take a look at the date format that has switched from year-
  //month-day to year-day-month. Spark will fail to parse this date and silently return null instead:

  df.select(to_date(lit("2016-20-12")),to_date(lit("2017-12-11"))).show(2)

  //We find this to be an especially tricky situation for bugs because some dates might match the
  //correct format, whereas others do not. In the previous example, notice how the second date
  //appears as Decembers 11th instead of the correct day, November 12th. Spark doesn’t throw an
  //error because it cannot know whether the days are mixed up or that specific row is incorrect.
  //Let’s fix this pipeline, step by step, and come up with a robust way to avoid these issues entirely.
  //The first step is to remember that we need to specify our date format according to the Java
  //SimpleDateFormat standard.
  //We will use two functions to fix this: to_date and to_timestamp. The former optionally
  //expects a format, whereas the latter requires one:

  //https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html

  val dateFormat = "yyyy-dd-MM"
  val euroFormat = "dd-MM-yy"
  val cleanDateDF = spark.range(1).select(
    to_date(lit("2017-12-11"), dateFormat).alias("date"),
    to_date(lit("2017-20-12"), dateFormat).alias("date2"),
    to_date(lit("02-08-22"), euroFormat).alias("date3"),
    to_date(lit("22_02_08"), "yy_dd_MM").alias("date4"), //does not work in Spark 3.0+ FIXME
  )
  cleanDateDF.createOrReplaceTempView("dateTable2")

  cleanDateDF.show(3,false)

  spark.sql(
    """
      |SELECT to_date(date, 'yyyy-dd-MM'), to_date(date2, 'yyyy-dd-MM'), to_date(date)
      |FROM dateTable2
      |""".stripMargin)
    .show(2)

  //Now let’s use an example of to_timestamp, which always requires a format to be specified:

  cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()

  //After we have our date or timestamp in the correct format and type, comparing between them is
  //actually quite easy. We just need to be sure to either use a date/timestamp type or specify our
  //string according to the right format of yyyy-MM-dd if we’re comparing a date
  cleanDateDF.filter(col("date3") > lit("2021-12-12")).show() //here it filters nothing

  //Working with Nulls in Data

  //As a best practice, you should always use nulls to represent missing or empty data in your
  //DataFrames. Spark can optimize working with null values more than it can if you use empty
  //strings or other values. The primary way of interacting with null values, at DataFrame scale, is to
  //use the .na subpackage on a DataFrame.

  //WARNING
  //Nulls are a challenging part of all programming, and Spark is no exception. In our opinion, being
  //explicit is always better than being implicit when handling null values. For instance, in this part of the
  //book, we saw how we can define columns as having null types. However, this comes with a catch.
  //When we declare a column as not having a null time, that is not actually enforced. To reiterate, when
  //you define a schema in which all columns are declared to not have null values, Spark will not enforce
  //that and will happily let null values into that column. The nullable signal is simply to help Spark SQL
  //optimize for handling that column. If you have null values in columns that should not have null values,
  //you can get an incorrect result or see strange exceptions that can be difficult to debug.

  //There are two things you can do with null values: you can explicitly drop nulls or you can fill
  //them with a value (globally or on a per-column basis). Let’s experiment with each of these now

  //Coalesce
  //Spark includes a function to allow you to select the first non-null value from a set of columns by
  //using the coalesce function. In this case, there are no null values, so it simply returns the first
  //column:

  df.describe().show() //double checking for description

  df.
    withColumn("mynulls", expr("null")).
    select(coalesce(col("mynulls"), col("Description"), col("CustomerId")))
    .show()
  //so it shows the first column without nulls

  //ifnull, nullIf, nvl, and nvl2
  //There are several other SQL functions that you can use to achieve similar things. ifnull allows
  //you to select the second value if the first is null, and defaults to the first. Alternatively, you could
  //use nullif, which returns null if the two values are equal or else returns the second if they are
  //not. nvl returns the second value if the first is null, but defaults to the first. Finally, nvl2 returns
  //the second value if the first is not null; otherwise, it will return the last specified value
  //(else_value in the following example):

  spark.sql(
    """
      |SELECT
      |ifnull(null, 'return_value'),
      |nullif('value', 'value'),
      | nvl(null, 'return_value'),
      |nvl2('not_null', 'return_value', "else_value"),
      |nvl2(null, 'return_value', "else_value")
      |FROM dfTable LIMIT 1
      |""".stripMargin).
    show(2)


  //drop
  //The simplest function is drop, which removes rows that contain nulls. The default is to drop any
  //row in which any value is null:
  println(s"Originally df is size: ${df.count()}")

  println(df.na.drop().count())
  println(df.na.drop("any").count()) //same as above drops rows where any column is null

  //all will drop rows only if ALL columns are null
  println(df.na.drop("all").count())

  //We can also apply this to certain sets of columns by passing in an array of columns:
  println("After dropping empty StockCode AND invoiceNo")
  println(df.na.drop("all", Seq("StockCode", "InvoiceNo")).count())
  println("After dropping when null Description")
  println(df.na.drop("all", Seq("Description")).count())

  //fill
  //Using the fill function, you can fill one or more columns with a set of values. This can be done
  //by specifying a map—that is a particular value and a set of columns.
  //For example, to fill all null values in columns of type String, you might specify the following:
  //df.na.fill("All Null values become this string")

  //We could do the same for columns of type Integer by using df.na.fill(5:Integer), or for
  //Doubles df.na.fill(5:Double). To specify columns, we just pass in an array of column names
  //like we did in the previous example:

  // in Scala
  //so we put 777 in those cells of specific columns which are null at the moment
  df.na.fill(777, Seq("StockCode", "InvoiceNo", "CustomerID"))
    .where(expr("CustomerID = 777"))
    .show(10, false)

  //We can also do this with with a Scala Map, where the key is the column name and the value is the
  //value we would like to use to fill null values

  // in Scala
  val fillColValues = Map("CustomerID" -> 5, "Description" -> "No Description")
  df.na.fill(fillColValues).
    where(expr("Description = 'No Description'"))
    .show(5, false)

  //replace
  //In addition to replacing null values like we did with drop and fill, there are more flexible
  //options that you can use with more than just null values. Probably the most common use case is
  //to replace all values in a certain column according to their current value. The only requirement is
  //that this value be the same type as the original value:

  //so with replace we can replace also not null values
  //of course we could have used our withColumn syntax to create new columns with replaced values as well

  // in Scala
  df.na.replace("Description", Map("VINTAGE SNAP CARDS" -> "BRAND NEW CARDS"))
    .where("Description = 'BRAND NEW CARDS'")
    .show(5, false)

  //so we could have a huge Map of different values to replace, but again not as flexible as regex based approach we saw earlier


}
