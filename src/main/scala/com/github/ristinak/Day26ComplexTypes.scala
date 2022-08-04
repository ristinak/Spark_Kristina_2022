package com.github.ristinak

import com.github.ristinak.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.functions.{array_contains, col, explode, map, size, split, struct}

object Day26ComplexTypes extends App {
  println("Ch6: Complex Data Types")
  val spark = getSpark("StringFun")

  val filePath = "src/resources/retail-data/by-day/2010-12-01.csv"

  val df = readDataWithView(spark, filePath)

  //Working with Complex Types
  //Complex types can help you organize and structure your data in ways that make more sense for
  //the problem that you are hoping to solve. There are three kinds of complex types: structs, arrays,
  //and maps.

  //Structs
  //You can think of structs as DataFrames within DataFrames. A worked example will illustrate
  //this more clearly. We can create a struct by wrapping a set of columns in parenthesis in a query

  val complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
  complexDF.createOrReplaceTempView("complexDF")

  //We now have a DataFrame with a column complex. We can query it just as we might another
  //DataFrame, the only difference is that we use a dot syntax to do so, or the column method
  //getField

  complexDF.show(5, false)

  //next 3 approaches are equivalent way of getting Description column out of complex column (which holds 2 columns)
  complexDF.select("complex.Description").show(3)
  complexDF.select(col("complex").getField("Description")).show(3)
  spark.sql(
    """
      |SELECT complex.Description FROM complexDF
      |""".stripMargin)
    .show(3)

  //so for all columns in some complex struct
  complexDF.select("complex.*").show(3)

  //Arrays
  //To define arrays, let’s work through a use case. With our current data, our objective is to take
  //every single word in our Description column and convert that into a row in our DataFrame.
  //The first task is to turn our Description column into a complex type, an array.
  //split
  //We do this by using the split function and specify the delimiter

  //so basic data engineering task

  //Splits str around matches of the given pattern.
  //Params:
  //str – a string expression to split
  //pattern – a string representing a regular expression. The regex string should be a Java regular expression.
  df.select(split(col("Description"), " ")).show(3, false)

  //SQL same thing
  spark.sql(
    """
      |SELECT split(Description, ' ') as mySplit FROM dfTable
      |""".stripMargin)
    .show(3, false)

  //This is quite powerful because Spark allows us to manipulate this complex type as another
  //column. We can also query the values of the array using Python-like syntax

  // in Scala
  df.select(split(col("Description"), " ").alias("array_col"))
    .selectExpr("array_col[0] as first", "array_col[1] as second").show(3)

  //Array Length
  //We can determine the array’s length by querying for its size:

  df.select(split(col("Description"), " ").alias("array_col"))
    .withColumn("arr_len", size(col("array_col"))) //so we add a new column using our only column from previous selction
    .show(5, false)

//array_contains
  //We can also see whether this array contains a value:
  df.select(split(col("Description"), " ").alias("array_col"))
    .withColumn("arr_len", size(col("array_col"))) //so we add a new column using our only column from previous selction
    .withColumn("white_exists", array_contains(col("array_col"), "WHITE"))
    .show(5, false)


  //explode
  //The explode function takes a column that consists of arrays and creates one row (with the rest of
  //the values duplicated) per value in the array
  df.withColumn("splitted", split(col("Description"), " "))
    .withColumn("exploded", explode(col("splitted")))
    .select("Description", "InvoiceNo", "splitted", "exploded").show(25, false)


  //Maps
  //Maps are created by using the map function and key-value pairs of columns. You then can select
  //them just like you might select from an array:

  df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
    .show(5, false)

  //sql
  spark.sql(
    """
      |-- in SQL
      |SELECT map(Description, InvoiceNo) as complex_map FROM dfTable
      |WHERE Description IS NOT NULL
      |""".stripMargin)
    .show(5, false)
  //notice how key can not be null

  //You can query them by using the proper key. A missing key returns null:
  // in Scala
  df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
    .selectExpr("complex_map['WHITE METAL LANTERN']").show(5, false)

  //You can also explode map types, which will turn them into columns:

  // in Scala
  df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
    .selectExpr("explode(complex_map)").show(12, false)

  //TODO open 4th of august CSV from 2011
  //create a new dataframe with all the original columns
  //plus array of of split description
  //plus length of said array (size)
  //filter by size of at least 3
  //withSelect add 3 more columns for the first 3 words in this dataframe
  //show top 10 results sorted by first word

  //so 5 new columns (filtered rows) sorted and then top 10 results

}
