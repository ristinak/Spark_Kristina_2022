package com.github.ristinak

import org.apache.spark.sql.SparkSession

// in Scala
import org.apache.spark.sql.functions.{window, column, desc, col}

object Day19StreamingExample extends App {
  println(s"Exploring Streaming with Scala version: ${util.Properties.versionNumberString}")
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${spark.version}")

  //partition count for local use
  //Because you’re likely running this in local mode, it’s a good practice to set the number of shuffle
  //partitions to something that’s going to be a better fit for local mode. This configuration specifies
  //the number of partitions that should be created after a shuffle. By default, the value is 200, but
  //because there aren’t many executors on this machine, it’s worth reducing this to 5.
  spark.conf.set("spark.sql.shuffle.partitions", "5")

  //https://www.precisely.com/blog/big-data/big-data-101-batch-stream-processing
  //With Structured Streaming, you can take the same operations that you perform in
  //batch mode using Spark’s structured APIs and run them in a streaming fashion. This can reduce
  //latency and allow for incremental processing. The best thing about Structured Streaming is that it
  //allows you to rapidly and quickly extract value out of streaming systems with virtually no code
  //changes.

  // in Scala
  //csv reading documentation: https://spark.apache.org/docs/latest/sql-data-sources-csv.html
  val staticDataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true") //we are letting Spark figure out the type of data in our csvs
    //    .csv("src/resources/retail-data/by-day/2010-12-01.csv")
    .csv("src/resources/retail-data/by-day/*.csv") //in order for this to work Hadoop (via Wintils on Windows) should be installed
  //get Hadoop with Winutils for Windows from https://github.com/kontext-tech/winutils
  //src/resources/retail-data/by-day
  //    .load("src/resources/retail-data/by-day/*.csv") //notice the wildcard we are loading everything!
  staticDataFrame.createOrReplaceTempView("retail_data")
  val staticSchema = staticDataFrame.schema
  println(staticSchema.toArray.mkString("\n"))

  println(s"We got ${staticDataFrame.count()} rows of data!") //remember count action goes across all partitions

  //  //batch query
  //  staticDataFrame
  //    .selectExpr(
  //      "CustomerId",
  //      "(UnitPrice * Quantity) as total_cost",
  //      "InvoiceDate")
  //    .groupBy(
  //      col("CustomerId"), window(col("InvoiceDate"), "1 day"))
  //    .sum("total_cost")
  //    .show(5)
  //
  //  //Now that we’ve seen how that works, let’s take a look at the streaming code! You’ll notice that
  //  //very little actually changes about the code. The biggest change is that we used readStream
  //  //instead of read, additionally you’ll notice the maxFilesPerTrigger option, which simply specifies
  //  //the number of files we should read in at once. This is to make our demonstration more
  //  //“streaming,” and in a production scenario this would probably be omitted.
  //
  //  val streamingDataFrame = spark.readStream
  //    .schema(staticSchema) //we provide the schema that we got from our static read
  //    .option("maxFilesPerTrigger", 1)
  //    .format("csv")
  //    .option("header", "true")
  ////    .load("src/resources/retail-data/by-day/*.csv")
  //    .load("src/resources/retail-data/by-day/2010-12-01.csv")
  //
  //  //lazy operation This is still a lazy operation, so we will need to call a streaming action to start the execution of
  //  //this data flow.
  //  // in Scala
  //  val purchaseByCustomerPerHour = streamingDataFrame
  //    .selectExpr(
  //      "CustomerId",
  //      "(UnitPrice * Quantity) as total_cost",
  //      "InvoiceDate")
  //    .groupBy(
  //      col("CustomerId"), window(col("InvoiceDate"), "1 day"))
  ////    .groupBy(
  ////      col("CustomerId"), window(col("CustomerId"), "1 day"))
  //    .sum("total_cost")
  //
  //  //Streaming actions are a bit different from our conventional static action because we’re going to
  //  //be populating data somewhere instead of just calling something like count (which doesn’t make
  //  //any sense on a stream anyways). The action we will use will output to an in-memory table that
  //  //we will update after each trigger. In this case, each trigger is based on an individual file (the read
  //  //option that we set). Spark will mutate the data in the in-memory table such that we will always
  //  //have the highest value as specified in our previous aggregation
  //
  //  purchaseByCustomerPerHour.writeStream
  //    .format("memory") // memory = store in-memory table
  //    .queryName("customer_purchases") // the name of the in-memory table
  //    .outputMode("complete") // complete = all the counts should be in the table
  //    .start()
  //
  //  // in Scala
  //  spark.sql("""
  //      SELECT *
  //      FROM customer_purchases
  //      ORDER BY `sum(total_cost)` DESC
  //      """)
  //    .show(5)
}

