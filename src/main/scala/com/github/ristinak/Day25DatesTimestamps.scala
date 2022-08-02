package com.github.ristinak


import com.github.ristinak.SparkUtil.{getSpark, readCSVWithView}
import org.apache.spark.sql.functions.{col, current_date, current_timestamp, date_add, date_sub, datediff, lit, months_between, to_date, to_timestamp}

object Day25DatesTimestamps extends App {
  println("CH6: Dates and Timestamps")
  val spark = getSpark("StringFun")

  val filePath = "src/resources/retail-data/by-day/2010-12-01.csv"

  val df = readCSVWithView(spark, filePath)

  //Working with Dates and Timestamps
  //Dates and times are a constant challenge in programming languages and databases. It’s always
  //necessary to keep track of timezones and ensure that formats are correct and valid. Spark does its
  //best to keep things simple by focusing explicitly on two kinds of time-related information. There
  //are dates, which focus exclusively on calendar dates, and timestamps, which include both date
  //and time information. Spark, as we saw with our current dataset, will make a best effort to
  //correctly identify column types, including dates and timestamps when we enable inferSchema.
  //We can see that this worked quite well with our current dataset because it was able to identify
  //and read our date format without us having to provide some specification for it.
  //As we hinted earlier, working with dates and timestamps closely relates to working with strings
  //because we often store our timestamps or dates as strings and convert them into date types at
  //runtime. This is less common when working with databases and structured data but much more
  //common when we are working with text and CSV files. We will experiment with that shortly

  //Although Spark will do read dates or times on a best-effort basis. However, sometimes there will
  //be no getting around working with strangely formatted dates and times. The key to
  //understanding the transformations that you are going to need to apply is to ensure that you know
  //exactly what type and format you have at each given step of the way. Another common “gotcha”
  //is that Spark’s TimestampType class supports only second-level precision, which means that if
  //you’re going to be working with milliseconds or microseconds, you’ll need to work around this
  //problem by potentially operating on them as longs. Any more precision when coercing to a
  //TimestampType will be removed.
  //Spark can be a bit particular about what format you have at any given point in time. It’s
  //important to be explicit when parsing or converting to ensure that there are no issues in doing so.
  //At the end of the day, Spark is working with Java dates and timestamps and therefore conforms
  //to those standards. Let’s begin with the basics and get the current date and the current
  //timestamps:

  val dateDF = spark.range(10)
    .withColumn("today", current_date())
    .withColumn("now", current_timestamp()) //so all rows get the same timestamp
  dateDF.createOrReplaceTempView("dateTable")

  dateDF.show(10, false)
  dateDF.printSchema()

  //Now that we have a simple DataFrame to work with, let’s add and subtract five days from today.
  //These functions take a column and then the number of days to either add or subtract as the
  //arguments:

  dateDF.select(col("today"),
    date_sub(col("today"), 5),
    date_add(col("today"), 7),
    date_add(col("today"), 365))
    .show(3)

  //SQL
  spark.sql(
    """
      |SELECT today, date_sub(today, 5), date_add(today, 5) FROM dateTable
      |""".stripMargin)
    .show(3)

  //Another common task is to take a look at the difference between two dates. We can do this with
  //the datediff function that will return the number of days in between two dates. Most often we
  //just care about the days, and because the number of days varies from month to month, there also
  //exists a function, months_between, that gives you the number of months between two dates:

  dateDF.withColumn("week_ago", date_sub(col("today"), 7))
    .select(datediff(col("week_ago"), col("today"))).show(1)

  dateDF.select(
    to_date(lit("2022-06-13")).alias("start"),
    to_date(lit("2022-08-30")).alias("end"))
    .withColumn("monthlyDifference", months_between(col("start"), col("end")))
    .withColumn("dayDifference", datediff(col("end"), col("start")))
    .show(2)

  //Notice that we introduced a new function: the to_date function. The to_date function allows
  //you to convert a string to a date, optionally with a specified format. We specify our format in the
  //Java SimpleDateFormat which will be important to reference if you use this function:
  //https://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html

  val dateTimeDF = spark.range(5)
    .withColumn("date", to_date(lit("2022-08-02")))
    .withColumn("timesStamp", to_timestamp(col("date"))) //we can do this since we JUST made date column one operation before
    .withColumn("badDate", to_date(lit("2022-08nota adadfa-02")))
  //    .select(to_date(col("date")))
  dateTimeDF.show(5)
  dateTimeDF.printSchema()

  //TODO open March 1st , 2011
  //Add new column with current date
  //Add new column with current timestamp
  //add new column which contains days passed since InvoiceDate (here it is March 1, 2011 but it could vary)
  //add new column with months passed since InvoiceDate



}

