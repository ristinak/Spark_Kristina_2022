package com.github.ristinak

import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.functions.{col, column} //of course just col is enough no need for column which does the same thing

object Day20BasicStructuredOperations extends App {
  println("Chapter 5. Basic Structured Operations")
  val spark = SparkUtil.getSpark("BasicSpark")

  val flightPath = "src/resources/flight-data/json/2015-summary.json"

  val df = spark.read.format("json")
    .load(flightPath)

  df.show(5)

  //We discussed that a DataFame will have columns, and we use a schema to define them. Let’s
  //take a look at the schema on our current DataFrame:
  println(df.schema)
  df.printSchema() //prints the above schema a bit nicer

  //Schemas
  //A schema defines the column names and types of a DataFrame. We can either let a data source
  //define the schema (called schema-on-read) or we can define it explicitly ourselves

  //Warning on Schemas
  //Deciding whether you need to define a schema prior to reading in your data depends on your use case.
  //For ad hoc analysis, schema-on-read usually works just fine (although at times it can be a bit slow with
  //plain-text file formats like CSV or JSON). However, this can also lead to precision issues like a long
  //type incorrectly set as an integer when reading in a file. When using Spark for production Extract,
  //Transform, and Load (ETL), it is often a good idea to define your schemas manually, especially when
  //working with untyped data sources like CSV and JSON because schema inference can vary depending
  //on the type of data that you read in.

  //If the types in the data (at runtime) do not match
  //the schema, Spark will throw an error. The example that follows shows how to create and
  //enforce a specific schema on a DataFrame.


  val myManualSchema = StructType(Array(
    StructField("DEST_COUNTRY_NAME", StringType, true),
    StructField("ORIGIN_COUNTRY_NAME", StringType, true),
    StructField("count", LongType, false,
      Metadata.fromJson("{\"hello\":\"world\"}"))
  ))
  //we could use the above schema instead of letting Spark define schema on the run

  //Columns and Expressions
  //Columns in Spark are similar to columns in a spreadsheet, R dataframe, or pandas DataFrame.
  //You can select, manipulate, and remove columns from DataFrames and these operations are
  //represented as expressions.
  //To Spark, columns are logical constructions that simply represent a value computed on a per-
  //record basis by means of an expression. This means that to have a real value for a column, we
  //need to have a row; and to have a row, we need to have a DataFrame. You cannot manipulate an
  //individual column outside the context of a DataFrame; you must use Spark transformations
  //within a DataFrame to modify the contents of a column

  //Columns
  //There are a lot of different ways to construct and refer to columns but the two simplest ways are
  //by using the col or column functions. To use either of these functions, you pass in a column
  //name:

  //Expressions
  //We mentioned earlier that columns are expressions, but what is an expression? An expression is
  //a set of transformations on one or more values in a record in a DataFrame. Think of it like a
  //function that takes as input one or more column names, resolves them, and then potentially
  //applies more expressions to create a single value for each record in the dataset. Importantly, this
  //“single value” can actually be a complex type like a Map or Array.

  //to see columns
  println("Columns in our Dataframe are:")
  println(df.columns.mkString(","))


  val firstRow = df.first() //of df.head()
  println(firstRow)
  val last5Rows = df.tail(5)
  val lastRow = df.tail(1)(0) //that is legal we just took the first item from tail(1) which returns Array
  println(lastRow)
  //I could have used collect on DF and used Scala as well

  //before writing we need to go back to one partiion (or we could have gone to collect and then written Rows one by one )
  df.coalesce(1)
    .write
    .option("header", "true")
    .option("sep", ",")
    .mode("overwrite")
    .csv("src/resources/csv/flight_summary_2015.csv")

  // Testing Spark definitive guide page 77

  df.selectExpr(
    "*", // include all original columns
    "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")
    .show(10)

}

