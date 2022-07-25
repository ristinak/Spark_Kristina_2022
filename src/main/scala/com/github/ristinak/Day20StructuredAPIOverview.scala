package com.github.ristinak

import org.apache.spark.sql.SparkSession

object Day20StructuredAPIOverview extends App {
  println("CH4: Structured API Overview - look into DataFrames and Datasets")

  println(s"Scala version: ${util.Properties.versionNumberString}")

  val spark = SparkSession.builder().appName("tourDeSpark").master("local").getOrCreate()
  spark.conf.set("spark.sql.shuffle.partitions", "5") //recommended for local, default is 200?
  println(s"Session started on Spark version ${spark.version}")

  //Overview of Structured Spark Types
  //Spark is effectively a programming language of its own. Internally, Spark uses an engine called
  //Catalyst that maintains its own type information through the planning and processing of work. In
  //doing so, this opens up a wide variety of execution optimizations that make significant
  //differences. Spark types map directly to the different language APIs that Spark maintains and
  //there exists a lookup table for each of these in Scala, Java, Python, SQL, and R. Even if we use
  //Spark’s Structured APIs from Python or R, the majority of our manipulations will operate strictly
  //on Spark types, not Python types. For example, the following code does not perform addition in
  //Scala or Python; it actually performs addition purely in Spark

  // in Scala
  val df = spark.range(40).toDF("number")
  df.select(df.col("number") + 100) //so here we have a spark command
    .show(15) //default for show is 20

  //To Spark (in Scala), DataFrames are
  //simply Datasets of Type Row. The “Row” type is Spark’s internal representation of its optimized
  //in-memory format for computation. This format makes for highly specialized and efficient
  //computation because rather than using JVM types, which can cause high garbage-collection and
  //object instantiation costs, Spark can operate on its own internal format without incurring any of
  //those costs

  //Columns
  //Columns represent a simple type like an integer or string, a complex type like an array or map, or
  //a null value. Spark tracks all of this type information for you and offers a variety of ways, with
  //which you can transform columns. Columns are discussed extensively in Chapter 5, but for the
  //most part you can think about Spark Column types as columns in a table

  //Rows
  //A row is nothing more than a record of data. Each record in a DataFrame must be of type Row, as
  //we can see when we collect the following DataFrames. We can create these rows manually from
  //SQL, from Resilient Distributed Datasets (RDDs), from data sources, or manually from scratch.

  // in Scala
  val tinyRange = spark.range(2).toDF().collect()
  //so Collect moved the data into our own program memory because it is now an Array which is
  //which is one of our basic data Structures in Scala/Java

  val arrRow = spark.range(10).toDF(colNames = "myNumber").collect()
  //so we have an array of Rows (with a single column each here)
  //so now we can use regular Scala stuff
  println("First 3 elements")
  arrRow.take(3).foreach(println) //first 3 elements
  println("3rd to 7th element starting form 0")
  arrRow.slice(2,7).foreach(println) //3rd(index starts with 0) to 7th element
  println("First")
  println(arrRow.head)
  println("Last")
  println(arrRow.last) //should be Row holding 9 here

  //Spark Types
  //We mentioned earlier that Spark has a large number of internal type representations. We include
  //a handy reference table on the next several pages so that you can most easily reference what
  //type, in your specific language, lines up with the type in Spark.
  //Before getting to those tables, let’s talk about how we instantiate, or declare, a column to be of a
  //certain type.
  //To work with the correct Scala types, use the following:
  import org.apache.spark.sql.types._ //this imports ALL spark datatypes
  val b = ByteType

  //TODO create a DataFrame with a single column called JulyNumbers from 1 to 31
  //TODO Show all 31 numbers
  //TODO Create another dataframe with numbers from 100 to 3100
  //TODO show last 5 numbers

  val dataFrame = spark.range(1,31+1).toDF("JulyNumber")

  //TODO Show all 31 numbers
  dataFrame.show(31)
  //TODO Create another dataframe with numbers from 100 to 3100
  val anotherDataFrame = spark.range(100,3100+1).toDF().collect()
  //TODO show last 5 numbers
  anotherDataFrame.reverse.take(5).foreach(println)
  val df100to3100 = dataFrame.select(dataFrame.col("JulyNumber") * 100) //we could make the formula more complicated
  df100to3100.collect().reverse.take(5).foreach(println)
}

